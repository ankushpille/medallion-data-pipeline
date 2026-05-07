import base64
import io
import json
import logging
import os
import re
import zipfile
from typing import Any, Dict, List, Optional, Tuple
from urllib import request as urlrequest

from services.pipeline_intelligence_service import analyze_pipeline_live

logger = logging.getLogger(__name__)


EXPRESSION_FUNCTIONS = (
    "activity",
    "concat",
    "item",
    "variables",
    "equals",
    "tolower",
    "toupper",
    "pipeline",
    "coalesce",
    "if",
    "formatdatetime",
    "substring",
    "replace",
    "string",
    "json",
)


def _field(value: Any, confidence: float, source: str, evidence: str, status: str = "DISCOVERED") -> Dict[str, Any]:
    return {
        "value": value,
        "confidence": confidence,
        "source": source,
        "evidence": evidence,
        "status": status,
    }


def _not_present(source: str, evidence: str) -> Dict[str, Any]:
    return _field(None, 1.0, source, evidence, status="NOT_PRESENT")


def _unknown(source: str, evidence: str) -> Dict[str, Any]:
    return _field(None, 0.0, source, evidence, status="UNKNOWN")


def _json_load(raw: bytes) -> Optional[Any]:
    try:
        return json.loads(raw.decode("utf-8-sig"))
    except Exception:
        return None


def _walk(value: Any, fn):
    if isinstance(value, dict):
        fn(value)
        for nested in value.values():
            _walk(nested, fn)
    elif isinstance(value, list):
        for item in value:
            _walk(item, fn)


def _find_values(obj: Any, key_name: str) -> List[Any]:
    found: List[Any] = []

    def visit(node: Dict[str, Any]):
        if key_name in node and node[key_name] not in (None, "", [], {}):
            found.append(node[key_name])

    _walk(obj, visit)
    return found


def _expr_to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float, bool)):
        return str(value)
    if isinstance(value, dict):
        if isinstance(value.get("value"), str):
            return value["value"]
        if isinstance(value.get("expression"), str):
            return value["expression"]
        if isinstance(value.get("dynamicContent"), str):
            return value["dynamicContent"]
        if isinstance(value.get("content"), str):
            return value["content"]
        if isinstance(value.get("type"), str) and isinstance(value.get("value"), dict):
            return _expr_to_text(value.get("value"))
    try:
        return json.dumps(value, default=str)
    except Exception:
        return str(value)


def _looks_like_expression(value: Any) -> bool:
    text = _expr_to_text(value).strip()
    if not text:
        return False
    lowered = text.lower()
    return text.startswith("@") or any(f"{fn}(" in lowered for fn in EXPRESSION_FUNCTIONS) or "@{" in text


def _deep_find_expressions(value: Any, path: str = "") -> List[Dict[str, Any]]:
    found: List[Dict[str, Any]] = []
    if isinstance(value, dict):
        for key, nested in value.items():
            child_path = f"{path}.{key}" if path else key
            if _looks_like_expression(nested):
                found.append({"path": child_path, "expression": _expr_to_text(nested)})
            found.extend(_deep_find_expressions(nested, child_path))
    elif isinstance(value, list):
        for idx, item in enumerate(value):
            child_path = f"{path}[{idx}]"
            if _looks_like_expression(item):
                found.append({"path": child_path, "expression": _expr_to_text(item)})
            found.extend(_deep_find_expressions(item, child_path))
    return found


def _safe_name(value: Any, fallback: str) -> str:
    text = str(value or "").strip()
    return text or fallback


def _collect_activity_records(activities: Any, parent: Optional[str] = None, scope: str = "root", depth: int = 0, branch: Optional[str] = None, records: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    records = records or []
    if not isinstance(activities, list):
        return records
    for index, activity in enumerate(activities):
        if not isinstance(activity, dict):
            continue
        name = _safe_name(activity.get("name") or activity.get("type"), f"activity_{depth}_{index}")
        record = {
            "name": name,
            "type": activity.get("type") or "Unknown",
            "activity": activity,
            "parent": parent,
            "scope": scope,
            "depth": depth,
            "branch": branch,
            "depends_on": activity.get("dependsOn") if isinstance(activity.get("dependsOn"), list) else [],
        }
        records.append(record)
        type_props = activity.get("typeProperties") or {}
        if isinstance(type_props.get("activities"), list):
            _collect_activity_records(type_props.get("activities"), parent=name, scope=f"{scope}.{name}", depth=depth + 1, branch="ForEach", records=records)
        if isinstance(type_props.get("ifTrueActivities"), list):
            _collect_activity_records(type_props.get("ifTrueActivities"), parent=name, scope=f"{scope}.{name}.ifTrue", depth=depth + 1, branch="IfTrue", records=records)
        if isinstance(type_props.get("ifFalseActivities"), list):
            _collect_activity_records(type_props.get("ifFalseActivities"), parent=name, scope=f"{scope}.{name}.ifFalse", depth=depth + 1, branch="IfFalse", records=records)
        for case_index, case in enumerate(type_props.get("cases", []) or []):
            if isinstance(case, dict):
                _collect_activity_records(case.get("activities"), parent=name, scope=f"{scope}.{name}.case{case_index}", depth=depth + 1, branch=_safe_name(case.get("value"), f"Case{case_index}"), records=records)
        if isinstance(type_props.get("defaultActivities"), list):
            _collect_activity_records(type_props.get("defaultActivities"), parent=name, scope=f"{scope}.{name}.default", depth=depth + 1, branch="Default", records=records)
    return records


def _parse_expression(expression: str, activity_name: Optional[str], field_path: str) -> Dict[str, Any]:
    text = _expr_to_text(expression).strip()
    lowered = text.lower()
    functions = sorted(set(match.lower() for match in re.findall(r"@?([a-zA-Z_][a-zA-Z0-9_]*)\s*\(", text)))
    activity_refs = re.findall(r"activity\('([^']+)'\)", text, flags=re.IGNORECASE)
    variable_refs = re.findall(r"variables\('([^']+)'\)", text, flags=re.IGNORECASE)
    parameter_refs = re.findall(r"parameters\('([^']+)'\)", text, flags=re.IGNORECASE)
    pipeline_param_refs = re.findall(r"pipeline\(\)\.parameters\.([A-Za-z0-9_]+)", text, flags=re.IGNORECASE)
    item_fields = re.findall(r"item\(\)\.([A-Za-z0-9_]+)", text, flags=re.IGNORECASE)

    classification = "runtime_expression"
    if activity_refs:
        classification = "activity_output_reference"
    elif item_fields:
        classification = "foreach_item_reference"
    elif variable_refs:
        classification = "variable_reference"
    elif parameter_refs or pipeline_param_refs:
        classification = "parameter_reference"
    elif any(fn in {"concat", "replace", "substring", "formatdatetime"} for fn in functions):
        classification = "dynamic_value_builder"
    elif "equals(" in lowered or "if(" in lowered:
        classification = "branch_condition"

    ast = _parse_expression_ast(text)
    semantic_meaning = "Runtime expression"
    if item_fields:
        semantic_meaning = f"Current metadata-driven {'/'.join(item_fields)}"
    elif activity_refs:
        semantic_meaning = f"Depends on output of activity {activity_refs[0]}"
    elif variable_refs:
        semantic_meaning = f"Uses runtime variable {variable_refs[0]}"

    resolved = {
        "activity": activity_name,
        "field_path": field_path,
        "expression": text,
        "classification": classification,
        "source_activity": activity_refs[0] if activity_refs else None,
        "dependent_activity": activity_name if activity_refs else None,
        "referenced_activities": activity_refs,
        "referenced_variables": variable_refs,
        "referenced_parameters": sorted(set(parameter_refs + pipeline_param_refs)),
        "referenced_item_fields": item_fields,
        "functions": functions,
        "dynamic_table_names": [field for field in item_fields if "table" in field.lower() or "entity" in field.lower()],
        "dynamic_filenames": [field for field in item_fields if any(token in field.lower() for token in ("file", "path", "folder", "name"))],
        "runtime_variables": variable_refs,
        "orchestration_dependencies": activity_refs,
        "ast": ast,
        "resolved_from": f"{activity_name or 'pipeline'} {field_path}",
        "semantic_meaning": semantic_meaning,
        "evidence": _field(text, 1.0, field_path, f"Expression found in {field_path}"),
    }
    return resolved


def _build_execution_paths(nodes: List[str], adjacency: Dict[str, List[str]], roots: List[str]) -> List[List[str]]:
    paths: List[List[str]] = []

    def dfs(current: str, seen: List[str]):
        next_nodes = adjacency.get(current, [])
        if not next_nodes:
            paths.append(seen + [current])
            return
        extended = False
        for nxt in next_nodes:
            if nxt in seen:
                continue
            extended = True
            dfs(nxt, seen + [current])
        if not extended:
            paths.append(seen + [current])

    for root in roots or nodes[:1]:
        dfs(root, [])
    unique = []
    seen = set()
    for path in paths:
        marker = "->".join(path)
        if marker not in seen:
            seen.add(marker)
            unique.append(path)
    return unique


def _split_function_arguments(text: str) -> List[str]:
    args = []
    current = []
    depth = 0
    in_string = False
    i = 0
    while i < len(text):
        ch = text[i]
        if ch == "'" and (i == 0 or text[i - 1] != "\\"):
            in_string = not in_string
            current.append(ch)
        elif not in_string and ch == "(":
            depth += 1
            current.append(ch)
        elif not in_string and ch == ")":
            depth -= 1
            current.append(ch)
        elif not in_string and ch == "," and depth == 0:
            args.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
        i += 1
    if current:
        args.append("".join(current).strip())
    return [arg for arg in args if arg]


def _parse_expression_ast(expression: str) -> Any:
    text = expression.strip()
    if text.startswith("@"):
        text = text[1:].strip()

    func_match = re.match(r"([A-Za-z_][A-Za-z0-9_]*)\(([\s\S]*)\)$", text)
    if func_match:
        fn_name = func_match.group(1)
        inner = func_match.group(2)
        return {
            "function": fn_name,
            "arguments": [_parse_expression_ast(arg) for arg in _split_function_arguments(inner)],
        }
    if re.match(r"item\(\)\.[A-Za-z0-9_]+$", text, flags=re.IGNORECASE):
        return {"dynamic_reference": text}
    if re.match(r"activity\('([^']+)'\)\.output(\..+)?$", text, flags=re.IGNORECASE):
        return {"activity_output_reference": text}
    if re.match(r"variables\('([^']+)'\)$", text, flags=re.IGNORECASE):
        return {"variable_reference": text}
    if re.match(r"pipeline\(\)\.parameters\.[A-Za-z0-9_]+$", text, flags=re.IGNORECASE):
        return {"pipeline_parameter_reference": text}
    if text.startswith("'") and text.endswith("'"):
        return text[1:-1]
    return {"literal": text}


def _extract_activity_graph(records: List[Dict[str, Any]], resolved_expressions: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    resolved_expressions = resolved_expressions or []
    nodes = []
    edges = []
    node_names = set()
    adjacency: Dict[str, List[str]] = {}
    incoming: Dict[str, List[str]] = {}

    for order, record in enumerate(records):
        name = record["name"]
        node_names.add(name)
        nodes.append({
            "id": name,
            "label": name,
            "type": record["type"],
            "position_hint": order,
            "scope": record["scope"],
            "depth": record["depth"],
            "branch": record["branch"],
            "parent": record["parent"],
        })
        adjacency.setdefault(name, [])
        incoming.setdefault(name, [])

    for record in records:
        current = record["name"]
        for dep in record.get("depends_on") or []:
            dep_name = dep.get("activity") if isinstance(dep, dict) else dep
            if not dep_name:
                continue
            condition = dep.get("dependencyConditions") if isinstance(dep, dict) else []
            edges.append({
                "id": f"{dep_name}->{current}",
                "source": dep_name,
                "target": current,
                "condition": condition or ["Succeeded"],
                "edge_type": "dependsOn",
            })
            adjacency.setdefault(dep_name, []).append(current)
            incoming.setdefault(current, []).append(dep_name)
        if record.get("parent"):
            edges.append({
                "id": f"{record['parent']}=>{current}",
                "source": record["parent"],
                "target": current,
                "condition": [record.get("branch") or "Nested"],
                "edge_type": "container",
            })
            adjacency.setdefault(record["parent"], []).append(current)
            incoming.setdefault(current, []).append(record["parent"])

    for expr in resolved_expressions:
        source = expr.get("source_activity")
        target = expr.get("activity")
        if source and target and source != target:
            edge_id = f"{source}~>{target}:{expr.get('field_path')}"
            if not any(edge["id"] == edge_id for edge in edges):
                edges.append({
                    "id": edge_id,
                    "source": source,
                    "target": target,
                    "condition": [expr.get("classification") or "expression"],
                    "edge_type": "expression",
                })
                adjacency.setdefault(source, []).append(target)
                incoming.setdefault(target, []).append(source)

    roots = [name for name in node_names if not incoming.get(name)]
    execution_paths = _build_execution_paths(sorted(node_names), adjacency, sorted(roots))
    failure_paths = [path for path in execution_paths if any("fail" in step.lower() for step in path)]
    success_paths = [path for path in execution_paths if path not in failure_paths]
    return {
        "nodes": nodes,
        "edges": edges,
        "execution_paths": execution_paths,
        "failure_paths": failure_paths,
        "success_paths": success_paths,
    }


def _detect_storage_kind(text: str) -> str:
    lowered = text.lower()
    if any(token in lowered for token in ("http", "rest", "relativeurl", "baseurl", "webactivity")):
        return "REST API"
    if any(token in lowered for token in ("warehouse", "sql", "jdbc", "table")):
        return "Warehouse"
    if any(token in lowered for token in ("lakehouse", "onelake", "deltalake", "adls", "blob", "file")):
        return "Lakehouse"
    if "notebook" in lowered:
        return "Notebook"
    return "Unknown"


def _collect_ingestion_type_evidence(records: List[Dict[str, Any]], datasets: List[Dict[str, Any]], notebook_intelligence: List[Dict[str, Any]], copy_lineage: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    evidence = []
    seen = set()
    for record in records:
        activity_type = str(record.get("type") or "")
        lowered = activity_type.lower()
        if "web" in lowered:
            marker = "REST API"
            if marker not in seen:
                seen.add(marker)
                evidence.append({"type": "REST API", "evidence": f"{activity_type} detected", "source": f"activity:{record['name']}"})
        if "copy" in lowered:
            sink_text = json.dumps((record["activity"].get("typeProperties") or {}).get("sink") or {}, default=str)
            source_text = json.dumps((record["activity"].get("typeProperties") or {}).get("source") or {}, default=str)
            if "json" in sink_text.lower() or "json" in source_text.lower():
                marker = "JSON"
                if marker not in seen:
                    seen.add(marker)
                    evidence.append({"type": "JSON", "evidence": "Json source/sink detected", "source": f"activity:{record['name']}"})
            if "delimitedtext" in sink_text.lower() or "delimitedtext" in source_text.lower():
                marker = "DelimitedText"
                if marker not in seen:
                    seen.add(marker)
                    evidence.append({"type": "DelimitedText", "evidence": "DelimitedText source/sink detected", "source": f"activity:{record['name']}"})
            if "lakehouse" in sink_text.lower() or "lakehouse" in source_text.lower():
                marker = "Lakehouse"
                if marker not in seen:
                    seen.add(marker)
                    evidence.append({"type": "Lakehouse", "evidence": "Lakehouse dataset settings detected", "source": f"activity:{record['name']}"})
            if "warehouse" in sink_text.lower() or "warehouse" in source_text.lower():
                marker = "DataWarehouse"
                if marker not in seen:
                    seen.add(marker)
                    evidence.append({"type": "DataWarehouse", "evidence": "Warehouse dataset settings detected", "source": f"activity:{record['name']}"})
    for dataset in datasets:
        dtype = str(dataset.get("type") or "")
        if dtype and dtype not in seen:
            seen.add(dtype)
            evidence.append({"type": dtype, "evidence": "Dataset type detected", "source": f"dataset:{dataset.get('path') or dataset.get('name')}"})
    for notebook in notebook_intelligence:
        for layer in notebook.get("medallion_layers") or []:
            marker = layer
            if marker not in seen:
                seen.add(marker)
                evidence.append({"type": layer, "evidence": "Notebook runtime parameter references layer", "source": f"activity:{notebook.get('activity')}"})
    return evidence


def _extract_sql_analysis(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    analyses = []
    for record in records:
        activity = record["activity"]
        type_props = activity.get("typeProperties") or {}
        candidates = [
            _expr_to_text(type_props.get("sqlReaderQuery")),
            _expr_to_text((type_props.get("source") or {}).get("sqlReaderQuery")),
            _expr_to_text((type_props.get("source") or {}).get("query")),
        ]
        sql = next((item for item in candidates if item and "select" in item.lower()), "")
        if not sql:
            continue
        tables = re.findall(r"(?:from|join)\s+([A-Za-z0-9_\.\[\]]+)", sql, flags=re.IGNORECASE)
        filters = re.search(r"\bwhere\b\s+(.+?)(?:\border\b|\bgroup\b|$)", sql, flags=re.IGNORECASE | re.DOTALL)
        table = tables[0] if tables else "UNKNOWN"
        purpose = "Operational SQL"
        lowered = sql.lower()
        if any(token in table.lower() for token in ("configure", "config", "control", "metadata")):
            purpose = "Metadata-driven ingestion controller"
        elif any(token in table.lower() for token in ("audit", "status", "log")):
            purpose = "Audit/status tracking"
        elif "watermark" in lowered or "incremental" in lowered:
            purpose = "Incremental load control"
        analyses.append({
            "activity": record["name"],
            "sql": sql,
            "config_table": table,
            "tables": tables,
            "purpose": purpose,
            "filter_logic": filters.group(1).strip() if filters else "",
            "confidence": 0.99,
            "source": f"activity:{record['name']}",
            "evidence": "sqlReaderQuery/query present",
        })
    return analyses


def _extract_foreach_intelligence(records: List[Dict[str, Any]], resolved_expressions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    items_by_activity = {(expr.get("activity"), expr.get("field_path")): expr for expr in resolved_expressions}
    loops = []
    for record in records:
        if str(record.get("type") or "").lower() != "foreach":
            continue
        items_expr = _expr_to_text(((record["activity"].get("typeProperties") or {}).get("items")))
        parsed = items_by_activity.get((record["name"], "typeProperties.items")) or _parse_expression(items_expr, record["name"], "typeProperties.items")
        loop_type = "Metadata-driven ingestion loop" if parsed.get("source_activity") or parsed.get("referenced_item_fields") else "Iterative loop"
        entity = "Configured source systems" if any(field.lower() in {"tablename", "baseurl", "entity", "source"} for field in parsed.get("referenced_item_fields") or []) else "Loop items"
        loops.append({
            "activity": record["name"],
            "loop_type": loop_type,
            "iteration_source": parsed.get("source_activity") or "Direct expression",
            "iterated_entity": entity,
            "items_expression": items_expr,
        })
    return loops


def _extract_notebook_intelligence(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    notebooks = []
    for record in records:
        if "notebook" not in str(record.get("type") or "").lower():
            continue
        type_props = record["activity"].get("typeProperties") or {}
        params = {}
        for key, value in (type_props.get("parameters") or {}).items():
            params[str(key)] = _expr_to_text((value or {}).get("value") if isinstance(value, dict) else value)
        params_text = json.dumps(params, default=str).lower()
        layers = [layer for layer in ("raw", "bronze", "silver", "gold", "unified") if layer in params_text]
        intent = "Notebook transformation"
        if layers:
            intent = f"Medallion transformation toward {' -> '.join(layers)}"
        notebooks.append({
            "activity": record["name"],
            "notebook_id": _expr_to_text(type_props.get("notebookId") or type_props.get("notebook")),
            "runtime_parameters": params,
            "transformation_intent": intent,
            "medallion_layers": layers,
            "evidence": _field(params, 0.98 if params else 0.85, f"activity:{record['name']}", "Notebook parameters"),
        })
    return notebooks


def _extract_copy_lineage(records: List[Dict[str, Any]], resolved_expressions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    lineage = []
    for record in records:
        if str(record.get("type") or "").lower() != "copy":
            continue
        type_props = record["activity"].get("typeProperties") or {}
        source = type_props.get("source") or {}
        sink = type_props.get("sink") or {}
        source_text = json.dumps(source, default=str)
        sink_text = json.dumps(sink, default=str)
        format_text = f"{source_text} {sink_text}"
        source_kind = _detect_storage_kind(source_text)
        sink_kind = _detect_storage_kind(sink_text)
        source_path = _expr_to_text((source.get("datasetSettings") or {}).get("typeProperties")) or _expr_to_text(source.get("path")) or None
        sink_path = _expr_to_text((sink.get("datasetSettings") or {}).get("typeProperties")) or _expr_to_text(sink.get("path")) or None
        fmt = None
        fmt_evidence = ""
        if "json" in format_text.lower():
            fmt = "JSON"
            fmt_evidence = "Json source/sink detected"
        elif "delimitedtext" in format_text.lower():
            fmt = "DelimitedText"
            fmt_evidence = "DelimitedText source/sink detected"
        lineage.append({
            "activity": record["name"],
            "source": source_kind,
            "target": sink_kind,
            "source_detail": source_path,
            "target_detail": sink_path,
            "format": fmt,
            "format_conversion": source_kind != sink_kind or fmt is not None,
            "evidence": {
                "source": _field(source_kind if source_kind != "Unknown" else None, 0.95 if source_kind != "Unknown" else 0.0, f"activity:{record['name']}.typeProperties.source", "Copy source settings"),
                "target": _field(sink_kind if sink_kind != "Unknown" else None, 0.95 if sink_kind != "Unknown" else 0.0, f"activity:{record['name']}.typeProperties.sink", "Copy sink settings"),
                "format": _field(fmt, 0.95 if fmt else 0.0, f"activity:{record['name']}", fmt_evidence or "No explicit source/sink format evidence", status="DISCOVERED" if fmt else "NOT_PRESENT"),
            },
        })
    return lineage


def _extract_email_notifications(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    notifications = []
    for record in records:
        activity = record["activity"]
        text = json.dumps(activity, default=str)
        lowered = text.lower()
        if "email" not in lowered and "mail" not in lowered:
            continue
        recipients = sorted(set(re.findall(r"[\w\.-]+@[\w\.-]+\.\w+", text)))
        notifications.append({
            "activity": record["name"],
            "recipients": recipients,
            "trigger": "Failure alert" if "fail" in lowered else "Success/operational alert",
            "pattern": "Operational monitoring notification",
        })
    return notifications


def _detect_load_strategy(records: List[Dict[str, Any]], sql_analysis: List[Dict[str, Any]], resolved_expressions: List[Dict[str, Any]]) -> Dict[str, Any]:
    joined = json.dumps(records, default=str).lower()
    sql_text = " ".join(item.get("sql", "") for item in sql_analysis).lower()
    expr_text = " ".join(item.get("expression", "") for item in resolved_expressions).lower()
    source = f"{joined} {sql_text} {expr_text}"
    strategies = {
        "append": "append" in source or "insert" in source,
        "merge": "merge" in source or "upsert" in source,
        "truncate": "truncate" in source,
        "cdc": "cdc" in source or "changefeed" in source,
        "incremental": any(token in source for token in ("watermark", "incremental", "lastmodified", "modifieddate")),
        "full_load": "full" in source or "truncate" in source,
    }
    primary = "merge" if strategies["merge"] else "incremental" if strategies["incremental"] else "append" if strategies["append"] else "full_load" if strategies["full_load"] else None
    evidence = []
    for key, enabled in strategies.items():
        if enabled:
            evidence.append({"strategy": key, "evidence": "Explicit token present in SQL/expressions/activity JSON"})
    return {"primary_strategy": primary, "signals": strategies, "evidence": evidence}


def _extract_failure_handling(records: List[Dict[str, Any]], graph: Dict[str, Any]) -> Dict[str, Any]:
    retry_policies = []
    notifications = []
    compensating = []
    for record in records:
        policy = record["activity"].get("policy") or {}
        if policy:
            retry_policies.append({
                "activity": record["name"],
                "retry": policy.get("retry"),
                "retry_interval_in_seconds": policy.get("retryIntervalInSeconds"),
                "timeout": policy.get("timeout"),
            })
        lowered = json.dumps(record["activity"], default=str).lower()
        if "email" in lowered or "mail" in lowered:
            notifications.append(record["name"])
        if any(token in lowered for token in ("rollback", "compensat", "audit", "status")):
            compensating.append(record["name"])
    failed_edges = [edge for edge in graph.get("edges", []) if any("fail" in str(cond).lower() for cond in edge.get("condition", []))]
    return {
        "retry_patterns": retry_policies,
        "failure_paths": graph.get("failure_paths", []),
        "failed_edges": failed_edges,
        "notification_patterns": notifications,
        "compensating_logic": compensating,
    }


def _infer_dq_recommendations(semantic: Dict[str, Any]) -> List[Dict[str, Any]]:
    recommendations = []
    if semantic.get("metadata_driven_ingestion", {}).get("enabled"):
        recommendations.append({
            "rule": "Metadata controller row validation",
            "reason": "Pipeline uses metadata-driven orchestration",
            "evidence": "Lookup + ForEach + dynamic item() expressions",
        })
    if semantic.get("load_strategy", {}).get("signals", {}).get("incremental"):
        recommendations.append({
            "rule": "Watermark freshness validation",
            "reason": "Incremental load signals detected",
            "evidence": "Watermark/incremental tokens in SQL or expressions",
        })
    if semantic.get("failure_handling", {}).get("retry_patterns"):
        recommendations.append({
            "rule": "Retry outcome audit validation",
            "reason": "Pipeline defines retry behavior",
            "evidence": "Activity policy.retry present",
        })
    if semantic.get("copy_lineage"):
        recommendations.append({
            "rule": "Copy boundary reconciliation",
            "reason": "Copy lineage exists between explicit source and sink settings",
            "evidence": "Copy activity source/sink mappings detected",
        })
    if semantic.get("notebook_intelligence"):
        recommendations.append({
            "rule": "Notebook output validation",
            "reason": "Notebook transformation stage present",
            "evidence": "Notebook activity with runtime parameters detected",
        })
    if semantic.get("source_systems"):
        recommendations.append({
            "rule": "Endpoint response validation",
            "reason": "REST API source activity detected",
            "evidence": "WebActivity and dynamic/static endpoint settings detected",
        })
    return recommendations


def _semantic_pipeline_analysis(pipeline_json: Dict[str, Any], manifest: Dict[str, Any], supporting_files: Dict[str, Any], zip_inventory: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    resource, pipeline_name = _find_pipeline_resource(pipeline_json)
    resource = resource or {}
    props = resource.get("properties") or {}
    records = _collect_activity_records(props.get("activities") or [])
    activities = [record["activity"] for record in records]
    activity_types = sorted({str(record["type"] or "Unknown") for record in records})

    resolved_expressions = []
    for record in records:
        for expr in _deep_find_expressions(record["activity"]):
            resolved_expressions.append(_parse_expression(expr["expression"], record["name"], expr["path"]))
    graph = _extract_activity_graph(records, resolved_expressions)

    linked_services = sorted({
        str(value.get("referenceName") or value.get("name"))
        for value in _find_values(pipeline_json, "linkedServiceName")
        if isinstance(value, dict) and (value.get("referenceName") or value.get("name"))
    })
    if not linked_services:
        linked_services = sorted({
            str(item.get("name"))
            for item in supporting_files.get("linked_services", [])
            if isinstance(item, dict) and item.get("name")
        })

    datasets = [
        {
            "name": item.get("name") or item.get("properties", {}).get("type"),
            "type": item.get("properties", {}).get("type") or item.get("type"),
            "path": item.get("__path"),
        }
        for item in supporting_files.get("datasets", [])
        if isinstance(item, dict)
    ]
    notebook_refs = sorted({
        str(value)
        for key in ("notebookId", "notebook", "notebookPath", "notebookName")
        for value in _find_values(pipeline_json, key)
        if value not in (None, "", [], {})
    })
    schedule = _normalize_schedule({"manifest": manifest, "pipeline": pipeline_json, "supporting_files": supporting_files})
    sql_analysis = _extract_sql_analysis(records)
    foreach_intelligence = _extract_foreach_intelligence(records, resolved_expressions)
    notebook_intelligence = _extract_notebook_intelligence(records)
    copy_lineage = _extract_copy_lineage(records, resolved_expressions)
    email_notifications = _extract_email_notifications(records)
    load_strategy = _detect_load_strategy(records, sql_analysis, resolved_expressions)
    failure_handling = _extract_failure_handling(records, graph)
    ingestion_type_evidence = _collect_ingestion_type_evidence(records, datasets, notebook_intelligence, copy_lineage)

    lookup_names = [record["name"] for record in records if str(record["type"]).lower() == "lookup"]
    foreach_names = [item["activity"] for item in foreach_intelligence]
    web_records = [record for record in records if "web" in str(record["type"]).lower() or "http" in json.dumps(record["activity"], default=str).lower()]
    copy_names = [record["name"] for record in records if str(record["type"]).lower() == "copy"]
    notebook_names = [item["activity"] for item in notebook_intelligence]

    metadata_enabled = bool(lookup_names and foreach_names and any(expr.get("referenced_item_fields") or expr.get("source_activity") for expr in resolved_expressions))
    config_table = next((item.get("config_table") for item in sql_analysis if item.get("purpose") == "Metadata-driven ingestion controller"), "") or (sql_analysis[0].get("config_table") if sql_analysis else "")
    endpoint_expr = next((expr for expr in resolved_expressions if any(field.lower() in {"baseurl", "relativeurl", "url", "endpoint"} for field in expr.get("referenced_item_fields", []))), None)
    source_systems = []
    if web_records:
        source_systems.append({
            "type": "REST API",
            "endpoint_strategy": "Metadata-driven" if endpoint_expr else "Static",
            "endpoint_source": endpoint_expr.get("referenced_item_fields", [None])[0] if endpoint_expr else None,
            "source": f"activity:{web_records[0]['name']}",
            "evidence": "WebActivity detected",
        })
    elif linked_services:
        source_systems.append({
            "type": _detect_storage_kind(" ".join(linked_services)),
            "endpoint_strategy": "Linked service configuration",
            "endpoint_source": linked_services[0],
            "source": "linked_services",
            "evidence": "Linked service names detected",
        })

    execution_flow = []
    if lookup_names:
        execution_flow.append("Lookup metadata")
    if foreach_names:
        execution_flow.append("Iterate configured tables")
    if any("ifcondition" == str(record["type"]).lower() for record in records):
        execution_flow.append("Evaluate orchestration conditions")
    if web_records:
        execution_flow.append("Call REST endpoint")
    if copy_names:
        execution_flow.append("Store or copy source payload")
    if notebook_names:
        execution_flow.append("Run notebook transformation")
    if any("warehouse" in json.dumps(lineage, default=str).lower() for lineage in copy_lineage):
        execution_flow.append("Load warehouse")
    if any(item.get("purpose") == "Audit/status tracking" for item in sql_analysis):
        execution_flow.append("Audit ingestion")
    if email_notifications:
        execution_flow.append("Send notification")
    if not execution_flow:
        execution_flow = [node["label"] for node in graph.get("nodes", [])]

    architecture_layers = []
    notebook_text = json.dumps(notebook_intelligence, default=str).lower()
    for layer in ("raw", "bronze", "silver", "gold", "unified"):
        if layer in notebook_text or layer in json.dumps(copy_lineage, default=str).lower():
            architecture_layers.append(layer)
    pipeline_pattern = "Metadata-driven REST ingestion" if metadata_enabled and web_records else None

    discovered = {
        "activity_types": [
            _field(record["type"], 1.0, f"activity:{record['name']}", "Activity type")
            for record in records
        ],
        "ingestion_types": [
            _field(item["type"], 1.0, item["source"], item["evidence"])
            for item in ingestion_type_evidence
        ],
        "source_type": _field("REST API", 1.0, f"activity:{web_records[0]['name']}" if web_records else "pipeline", "WebActivity detected") if web_records else _not_present("pipeline", "No explicit WebActivity present"),
        "linked_services": [
            _field(item, 1.0, "pipeline_json", "linkedServiceName detected")
            for item in linked_services
        ],
        "datasets": [
            _field(item.get("type"), 1.0, item.get("path") or item.get("name") or "dataset", "Dataset type detected")
            for item in datasets if item.get("type")
        ],
        "trigger_type": schedule.get("trigger_type"),
        "frequency": schedule.get("frequency"),
        "lineage": copy_lineage,
        "sql_analysis": sql_analysis,
        "notebook_parameters": notebook_intelligence,
    }

    resolved = {
        "dynamic_expressions": [
            {
                "type": "dynamic_expression",
                "expression": expr["expression"],
                "resolved_from": expr["resolved_from"],
                "semantic_meaning": expr["semantic_meaning"],
                "ast": expr["ast"],
                "confidence": 1.0,
                "source": expr["field_path"],
                "evidence": f"Expression found in {expr['field_path']}",
            }
            for expr in resolved_expressions
        ],
        "endpoint_source_column": _field(endpoint_expr.get("referenced_item_fields", [None])[0], 0.99, endpoint_expr.get("field_path"), "Dynamic endpoint item() field") if endpoint_expr else _not_present("pipeline", "No dynamic endpoint expression found"),
        "endpoint": {
            "type": "dynamic" if endpoint_expr else None,
            "expression": endpoint_expr.get("expression") if endpoint_expr else None,
            "resolved_from": f"{endpoint_expr.get('source_activity')} output" if endpoint_expr and endpoint_expr.get("source_activity") else None,
            "lookup_source": f"{config_table}.{endpoint_expr.get('referenced_item_fields', [None])[0]}" if endpoint_expr and config_table and endpoint_expr.get("referenced_item_fields") else None,
            "confidence": 0.98 if endpoint_expr else 0.0,
            "source": endpoint_expr.get("field_path") if endpoint_expr else "pipeline",
            "evidence": "Dynamic endpoint expression" if endpoint_expr else "No dynamic endpoint expression found",
            "status": "RESOLVED" if endpoint_expr else "NOT_PRESENT",
        },
        "foreach_intelligence": foreach_intelligence,
        "activity_graph": graph,
    }

    inferred = {
        "architecture_pattern": _field("Metadata-driven REST ingestion", 0.94, "semantic_analysis", "WebActivity + Lookup + ForEach pattern") if pipeline_pattern else _not_present("semantic_analysis", "Pattern evidence incomplete"),
        "architecture": {
            "pattern": "Medallion" if architecture_layers else None,
            "layers": architecture_layers,
            "confidence": 0.94 if architecture_layers else 0.0,
            "source": "notebook parameters / copy lineage",
            "evidence": "Explicit layer tokens found" if architecture_layers else "No explicit medallion layer evidence found",
            "status": "INFERRED" if architecture_layers else "NOT_PRESENT",
        },
        "metadata_driven_ingestion": {
            "enabled": metadata_enabled,
            "config_table": config_table or None,
            "loop_pattern": "Lookup + ForEach" if lookup_names and foreach_names else None,
            "dynamic_endpoint_resolution": bool(endpoint_expr),
            "confidence": 0.95 if metadata_enabled else 0.0,
            "source": "lookup/sql/foreach/expressions",
            "evidence": "Lookup + ForEach + item()/activity() evidence" if metadata_enabled else "Pattern evidence incomplete",
            "status": "INFERRED" if metadata_enabled else "NOT_PRESENT",
        },
        "execution_flow": execution_flow,
        "load_strategy": load_strategy,
        "operational_monitoring": {
            "email_alerts": bool(email_notifications),
            "retry_enabled": bool(failure_handling.get("retry_patterns")),
            "failure_paths": graph.get("failure_paths", []),
        },
        "notification_strategy": {
            "emails": email_notifications,
            "enabled": bool(email_notifications),
        },
        "audit_framework": {
            "audit_tables": [item["config_table"] for item in sql_analysis if "audit" in item.get("purpose", "").lower()],
            "status_tracking": any("status" in json.dumps(item, default=str).lower() for item in sql_analysis + records),
        },
        "retry_strategy": failure_handling,
    }

    unknown = {
        "trigger_type": schedule.get("trigger_type") if schedule.get("trigger_type", {}).get("status") in {"NOT_PRESENT", "UNKNOWN"} else None,
        "frequency": schedule.get("frequency") if schedule.get("frequency", {}).get("status") in {"NOT_PRESENT", "UNKNOWN"} else None,
    }

    confidence_scores = {
        "source_type": discovered["source_type"]["confidence"],
        "endpoint": resolved["endpoint"]["confidence"],
        "architecture_pattern": inferred["architecture_pattern"]["confidence"] if isinstance(inferred["architecture_pattern"], dict) else 0.0,
        "metadata_driven_ingestion": inferred["metadata_driven_ingestion"]["confidence"],
    }

    evidence_map = {
        "source_type": discovered["source_type"]["evidence"],
        "endpoint": resolved["endpoint"]["evidence"],
        "metadata_table": config_table,
        "dynamic_expressions": [expr["field_path"] for expr in resolved_expressions],
    }

    semantic = {
        "discovered": discovered,
        "resolved": resolved,
        "inferred": inferred,
        "unknown": unknown,
        "confidence_scores": confidence_scores,
        "evidence_map": evidence_map,
        "source_systems": source_systems,
        "execution_flow": execution_flow,
        "lineage": [
            {
                "source": item["source"],
                "target": item["target"],
                "format": item["format"],
                "evidence": item.get("evidence"),
            }
            for item in copy_lineage
        ],
        "dynamic_expressions": resolved_expressions,
        "activity_graph": graph,
        "sql_analysis": sql_analysis,
        "foreach_intelligence": foreach_intelligence,
        "notebook_intelligence": notebook_intelligence,
        "copy_lineage": copy_lineage,
        "load_strategy": load_strategy,
        "failure_handling": failure_handling,
        "notification_strategy": inferred["notification_strategy"],
        "audit_framework": inferred["audit_framework"],
        "operational_monitoring": inferred["operational_monitoring"],
        "retry_strategy": failure_handling,
        "dq_recommendations": [],
    }
    semantic["dq_recommendations"] = _infer_dq_recommendations(semantic)

    return {
        "pipeline_name": pipeline_name or manifest.get("name") or "Fabric Pipeline",
        "pipeline_json": pipeline_json,
        "manifest_json": manifest,
        "activity_count": len(activities),
        "activities": activities,
        "activity_types": activity_types,
        "activity_graph": graph,
        "dependencies": graph["edges"],
        "parameters": props.get("parameters") or pipeline_json.get("parameters") or {},
        "variables": props.get("variables") or {},
        "datasets": datasets,
        "linked_services": linked_services,
        "notebook_references": notebook_refs,
        "triggers": supporting_files.get("triggers", []),
        "scheduling": schedule,
        "retry_policies": failure_handling.get("retry_patterns") or [],
        "orchestration_flow": execution_flow,
        "lookup_configs": [{"name": record["name"], "source": ((record["activity"].get("typeProperties") or {}).get("source"))} for record in records if str(record["type"]).lower() == "lookup"],
        "foreach_loops": foreach_intelligence,
        "notebook_execution": notebook_intelligence,
        "expressions": [item.get("expression") for item in resolved_expressions[:100]],
        "manifest_metadata": manifest.get("properties") or manifest,
        "zip_inventory": zip_inventory or [],
        "semantic_analysis": semantic,
        "resolvedExpressions": resolved_expressions,
        "sql_analysis": sql_analysis,
        "copy_lineage": copy_lineage,
        "source_systems": source_systems,
        "pipeline_pattern": pipeline_pattern,
        "load_strategy": load_strategy,
        "notification_strategy": semantic["notification_strategy"],
        "audit_framework": semantic["audit_framework"],
        "operational_monitoring": semantic["operational_monitoring"],
        "discovered": discovered,
        "resolved": resolved,
        "inferred": inferred,
        "unknown": unknown,
        "confidence_scores": confidence_scores,
        "evidence_map": evidence_map,
    }


def _flatten_activities(activities: Any, result: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    result = result or []
    if not isinstance(activities, list):
        return result
    for activity in activities:
        if not isinstance(activity, dict):
            continue
        result.append(activity)
        type_props = activity.get("typeProperties") or {}
        for nested_key in ("activities", "ifTrueActivities", "ifFalseActivities", "defaultActivities"):
            _flatten_activities(type_props.get(nested_key), result)
        for case in type_props.get("cases", []) or []:
            _flatten_activities((case or {}).get("activities"), result)
    return result


def _find_pipeline_resource(payload: Any) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if isinstance(payload, dict) and isinstance(payload.get("properties"), dict) and isinstance(payload["properties"].get("activities"), list):
        return payload, payload.get("name")

    if isinstance(payload, dict) and isinstance(payload.get("resources"), list):
        for resource in payload["resources"]:
            if not isinstance(resource, dict):
                continue
            props = resource.get("properties")
            if isinstance(props, dict) and isinstance(props.get("activities"), list):
                return resource, resource.get("name")

    return None, None


def _choose_pipeline_json(json_files: Dict[str, Any]) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    candidates: List[Tuple[int, str, Dict[str, Any]]] = []
    for path, payload in json_files.items():
        if not isinstance(payload, dict):
            continue
        resource, _ = _find_pipeline_resource(payload)
        if not resource:
            continue
        score = 0
        lower_path = path.lower()
        if "pipeline-content" in lower_path:
            score += 5
        if "pipeline" in lower_path:
            score += 2
        if "manifest" in lower_path or "metadata" in lower_path:
            score -= 3
        score += len((resource.get("properties") or {}).get("activities") or [])
        candidates.append((score, path, payload))

    if not candidates:
        return None, None

    candidates.sort(key=lambda item: item[0], reverse=True)
    _, path, payload = candidates[0]
    return path, payload


def _extract_manifest(zip_names: List[str], json_files: Dict[str, Any]) -> Dict[str, Any]:
    manifest = {}
    for path in zip_names:
        lower = path.lower()
        if lower.endswith("manifest.json") and isinstance(json_files.get(path), dict):
            manifest = json_files[path]
            break

    if manifest:
        return manifest

    for path in zip_names:
        lower = path.lower()
        if lower.endswith("item.metadata.json") and isinstance(json_files.get(path), dict):
            metadata = json_files[path]
            return {
                "name": metadata.get("displayName") or metadata.get("name") or "Fabric Pipeline",
                "type": metadata.get("type") or "DataPipeline",
                "properties": metadata,
            }

    return {}


def _normalize_schedule(payload: Any) -> Dict[str, Any]:
    recurrence = None
    source_key = ""
    for key in ("recurrence", "schedule", "scheduling", "trigger", "triggers"):
        values = _find_values(payload, key)
        if values:
            recurrence = values[0]
            source_key = key
            break

    if isinstance(recurrence, list) and recurrence:
        recurrence = recurrence[0]

    if recurrence in (None, "", [], {}):
        return {
            "frequency": _not_present("pipeline/supporting_files", "No trigger/schedule object present"),
            "interval": _not_present("pipeline/supporting_files", "No trigger/schedule object present"),
            "trigger_type": _not_present("pipeline/supporting_files", "No trigger/schedule object present"),
            "raw": {},
        }

    schedule = {
        "frequency": _unknown(source_key or "pipeline/supporting_files", "Schedule object found but frequency key missing"),
        "interval": _unknown(source_key or "pipeline/supporting_files", "Schedule object found but interval key missing"),
        "trigger_type": _unknown(source_key or "pipeline/supporting_files", "Schedule object found but trigger type missing"),
        "raw": recurrence or {},
    }
    if isinstance(recurrence, dict):
        if recurrence.get("frequency") or recurrence.get("pattern"):
            schedule["frequency"] = _field(
                recurrence.get("frequency") or recurrence.get("pattern"),
                0.98,
                source_key or "pipeline/supporting_files",
                "schedule.frequency/pattern",
            )
        if recurrence.get("interval") or recurrence.get("count") is not None:
            schedule["interval"] = _field(
                recurrence.get("interval") or recurrence.get("count"),
                0.98,
                source_key or "pipeline/supporting_files",
                "schedule.interval/count",
            )
        if recurrence.get("type"):
            schedule["trigger_type"] = _field(
                recurrence.get("type"),
                0.98,
                source_key or "pipeline/supporting_files",
                "schedule.type",
            )
    elif recurrence:
        schedule["trigger_type"] = _field("Configured", 0.7, source_key or "pipeline/supporting_files", "Non-object trigger value present")
        schedule["frequency"] = _field(str(recurrence), 0.7, source_key or "pipeline/supporting_files", "Trigger value stringified")
    return schedule


def _normalize_pipeline_config(source: str, pipeline_json: Dict[str, Any], manifest: Dict[str, Any], supporting_files: Dict[str, Any], zip_inventory: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    normalized = _semantic_pipeline_analysis(pipeline_json, manifest, supporting_files, zip_inventory=zip_inventory)
    normalized["source"] = source
    return normalized


def parse_uploaded_bundle(raw_bytes: bytes, filename: str) -> Dict[str, Any]:
    with zipfile.ZipFile(io.BytesIO(raw_bytes)) as archive:
        names = archive.namelist()
        json_files: Dict[str, Any] = {}
        inventory: List[Dict[str, Any]] = []
        supporting = {
            "datasets": [],
            "linked_services": [],
            "triggers": [],
        }

        for path in names:
            info = archive.getinfo(path)
            inventory.append({"path": path, "size": info.file_size})
            if info.is_dir() or not path.lower().endswith(".json"):
                continue
            payload = _json_load(archive.read(path))
            if payload is None:
                continue
            if isinstance(payload, dict):
                payload["__path"] = path
            json_files[path] = payload
            lower = path.lower()
            if "dataset" in lower:
                supporting["datasets"].append(payload)
            elif "linkedservice" in lower or "linked-service" in lower:
                supporting["linked_services"].append(payload)
            elif "trigger" in lower or "schedule" in lower:
                supporting["triggers"].append(payload)

        manifest = _extract_manifest(names, json_files)
        pipeline_path, pipeline_json = _choose_pipeline_json(json_files)
        if not pipeline_json:
            raise ValueError("Pipeline JSON could not be located in the uploaded ZIP bundle.")

        uploaded = _normalize_pipeline_config(
            "uploaded_bundle",
            pipeline_json,
            manifest,
            supporting,
            zip_inventory=inventory,
        )
        uploaded["raw_uploaded_json"] = pipeline_json
        uploaded["raw_manifest_json"] = manifest
        uploaded["selected_pipeline_path"] = pipeline_path
        uploaded["bundle_name"] = filename
        return uploaded


def _extract_auto_discovered_config(discovery_result: Dict[str, Any]) -> Dict[str, Any]:
    pipeline_json = (
        discovery_result.get("original_config")
        or (((discovery_result.get("data_pipelines") or [{}])[0]).get("configuration"))
        or {}
    )
    manifest = {
        "name": (((discovery_result.get("data_pipelines") or [{}])[0]).get("name")) or discovery_result.get("framework") or "Fabric Pipeline",
        "type": "DataPipeline",
        "properties": {
            "framework": discovery_result.get("framework"),
            "scan_status": discovery_result.get("scan_status"),
        },
    }

    config = _normalize_pipeline_config("fabric_api", pipeline_json, manifest, {})
    config["discovery_summary"] = {
        "framework": discovery_result.get("framework"),
        "ingestion_support": discovery_result.get("ingestion_support") or {},
        "pipeline_capabilities": discovery_result.get("pipeline_capabilities") or {},
        "raw_cloud_scan": discovery_result.get("raw_cloud_scan") or {},
        "reformatted_config": discovery_result.get("reformatted_config") or {},
        "interactive_flow": discovery_result.get("interactive_flow") or [],
        "llm_summary": discovery_result.get("llm_summary"),
    }
    return config


def _merge_unique_dicts(primary: List[Dict[str, Any]], secondary: List[Dict[str, Any]], key_fields: Tuple[str, ...] = ("name", "id", "label")) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    seen = set()
    for source in (primary, secondary):
        for item in source or []:
            if not isinstance(item, dict):
                continue
            marker = None
            for key in key_fields:
                if item.get(key):
                    marker = f"{key}:{item.get(key)}"
                    break
            marker = marker or json.dumps(item, sort_keys=True, default=str)
            if marker in seen:
                continue
            seen.add(marker)
            merged.append(item)
    return merged


def _deep_merge(uploaded: Any, discovered: Any) -> Any:
    if uploaded in (None, "", [], {}):
        return discovered
    if discovered in (None, "", [], {}):
        return uploaded
    if isinstance(uploaded, dict) and isinstance(discovered, dict):
        merged = dict(discovered)
        for key, value in uploaded.items():
            merged[key] = _deep_merge(value, discovered.get(key))
        return merged
    if isinstance(uploaded, list) and isinstance(discovered, list):
        if all(isinstance(item, dict) for item in uploaded + discovered):
            return _merge_unique_dicts(uploaded, discovered)
        merged = list(uploaded)
        for item in discovered:
            if item not in merged:
                merged.append(item)
        return merged
    return uploaded


def merge_pipeline_configs(auto_discovered: Dict[str, Any], uploaded: Dict[str, Any]) -> Dict[str, Any]:
    merged = _deep_merge(uploaded, auto_discovered)
    merged["source"] = "merged"
    merged["auto_discovered_config"] = auto_discovered
    merged["uploaded_pipeline_config"] = uploaded
    uploaded_graph = uploaded.get("activity_graph") or {}
    discovered_graph = auto_discovered.get("activity_graph") or {}
    merged["activity_graph"] = {
        "nodes": _merge_unique_dicts(
            uploaded_graph.get("nodes", []),
            discovered_graph.get("nodes", []),
            key_fields=("id", "label"),
        ),
        "edges": _merge_unique_dicts(
            uploaded_graph.get("edges", []),
            discovered_graph.get("edges", []),
            key_fields=("id", "source"),
        ),
        "execution_paths": uploaded_graph.get("execution_paths") or discovered_graph.get("execution_paths") or [],
        "failure_paths": uploaded_graph.get("failure_paths") or discovered_graph.get("failure_paths") or [],
        "success_paths": uploaded_graph.get("success_paths") or discovered_graph.get("success_paths") or [],
    }
    merged["manifest_json"] = uploaded.get("manifest_json") or auto_discovered.get("manifest_json") or {}
    merged["manifest_metadata"] = _deep_merge(uploaded.get("manifest_metadata"), auto_discovered.get("manifest_metadata"))
    merged["orchestration_flow"] = merged.get("orchestration_flow") or [
        node.get("label") for node in merged["activity_graph"]["nodes"]
    ]
    merged["semantic_analysis"] = _deep_merge(uploaded.get("semantic_analysis") or {}, auto_discovered.get("semantic_analysis") or {})
    merged["resolvedExpressions"] = _merge_unique_dicts(uploaded.get("resolvedExpressions", []), auto_discovered.get("resolvedExpressions", []), key_fields=("field_path", "expression"))
    return merged


def _safe_json_extract(text: str) -> Optional[Dict[str, Any]]:
    match = re.search(r"(\{[\s\S]*\})", text or "")
    if match:
        text = match.group(1)
    try:
        return json.loads(text)
    except Exception:
        return None


def _detect_ingestion_types(final_config: Dict[str, Any]) -> List[str]:
    discovered = (final_config.get("semantic_analysis") or {}).get("discovered") or {}
    items = discovered.get("ingestion_types") or []
    return [item.get("value") for item in items if item.get("value")]


def _rule_based_bundle_ai(final_config: Dict[str, Any]) -> Dict[str, Any]:
    semantic = final_config.get("semantic_analysis") or {}
    discovered = semantic.get("discovered") or {}
    resolved = semantic.get("resolved") or {}
    inferred = semantic.get("inferred") or {}
    unknown = semantic.get("unknown") or {}
    datasets = final_config.get("datasets") or []
    activities = final_config.get("activities") or []
    linked_services = final_config.get("linked_services") or []
    schedule = final_config.get("scheduling") or {}
    expressions = final_config.get("expressions") or []
    activity_types = [str(item.get("type") or "") for item in activities]

    source_system_info = (semantic.get("source_systems") or [{}])[0]
    source_system = source_system_info.get("type") or (linked_services[0] if linked_services else (datasets[0].get("type") if datasets else "UNKNOWN"))
    endpoint = source_system_info.get("endpoint_source") or next((expr for expr in expressions if "http" in str(expr).lower()), "UNKNOWN")
    trigger_type = schedule.get("trigger_type") or "UNKNOWN"
    frequency = schedule.get("frequency") or "UNKNOWN"

    copy_names = [item.get("name") for item in activities if str(item.get("type") or "").lower() == "copy"]
    notebook_names = [item.get("name") for item in activities if "notebook" in str(item.get("type") or "").lower()]
    foreach_names = [item.get("name") for item in activities if str(item.get("type") or "").lower() == "foreach"]
    lookup_names = [item.get("name") for item in activities if str(item.get("type") or "").lower() == "lookup"]
    email_names = [item.get("name") for item in activities if "mail" in json.dumps(item, default=str).lower() or "email" in str(item.get("type") or "").lower()]

    insights = []
    if lookup_names and foreach_names:
        insights.append("Lookup-driven fan-out pattern detected before iterative processing.")
    if copy_names and notebook_names:
        insights.append("Hybrid ingestion + transformation flow detected: copy activity followed by notebook execution.")
    if any(policy.get("retry") for policy in final_config.get("retry_policies") or []):
        insights.append("Retry policies are configured on one or more pipeline activities.")
    if not insights:
        insights.append("Bundle was merged successfully, but advanced orchestration patterns were only partially discoverable from the available metadata.")

    return {
        "agent_role": "Pipeline Discovery & Ingestion Intelligence Agent",
        "discovered": discovered,
        "resolved": resolved,
        "inferred": inferred,
        "unknown": unknown,
        "confidence_scores": semantic.get("confidence_scores") or {},
        "evidence_map": semantic.get("evidence_map") or {},
        "ingestion_type": [
            {
                "type": item.get("value"),
                "evidence": item.get("evidence"),
                "source": item.get("source"),
                "confidence": item.get("confidence"),
            }
            for item in discovered.get("ingestion_types", [])
        ],
        "file_structure_intelligence": {
            "columns": [],
            "datatype": [],
            "nullable_fields": [],
            "mandatory_fields": [],
            "date_formats": [],
            "delimiters": [],
            "nested_structures": [
                {
                    "value": "JSON",
                    "confidence": 1.0,
                    "source": item.get("source"),
                    "evidence": item.get("evidence"),
                }
                for item in discovered.get("ingestion_types", [])
                if item.get("value") == "JSON"
            ],
            "relationships": [],
            "allowed_values": {},
        },
        "ingestion_intelligence": {
            "source_system": discovered.get("source_type") or _not_present("pipeline", "No explicit source type evidence"),
            "endpoint": resolved.get("endpoint") or _not_present("pipeline", "No endpoint expression/value detected"),
            "frequency": schedule.get("frequency") or _not_present("pipeline", "No schedule present"),
            "trigger_type": schedule.get("trigger_type") or _not_present("pipeline", "No trigger present"),
            "batch_logic": inferred.get("metadata_driven_ingestion") or _not_present("semantic_analysis", "No metadata-driven evidence"),
            "watermark_logic": _field(True, 0.9, "sql/expressions", "Incremental signal detected", status="INFERRED") if final_config.get("load_strategy", {}).get("signals", {}).get("incremental") else _not_present("sql/expressions", "No watermark signal detected"),
            "load_mode": _field(final_config.get("load_strategy", {}).get("primary_strategy"), 0.9, "sql/expressions", "Explicit load strategy tokens detected", status="INFERRED") if final_config.get("load_strategy", {}).get("primary_strategy") else _not_present("sql/expressions", "No explicit load strategy tokens detected"),
            "merge_strategy": _field("merge", 0.9, "sql/expressions", "Explicit merge/upsert token detected", status="INFERRED") if final_config.get("load_strategy", {}).get("signals", {}).get("merge") else _not_present("sql/expressions", "No merge/upsert token detected"),
            "truncate_logic": _field(True, 0.9, "sql/expressions", "Explicit truncate token detected", status="INFERRED") if final_config.get("load_strategy", {}).get("signals", {}).get("truncate") else _not_present("sql/expressions", "No truncate token detected"),
        },
        "dq_recommendations": semantic.get("dq_recommendations") or [
            {
                "rule": "No deterministic DQ rule extracted",
                "reason": "Evidence insufficient",
                "evidence": "No qualifying orchestration/SQL/notebook signals found",
            }
        ],
        "activity_intelligence": {
            "lookup_activities": lookup_names,
            "foreach_loops": semantic.get("foreach_intelligence") or foreach_names,
            "notebook_execution": semantic.get("notebook_intelligence") or notebook_names,
            "copy_activities": copy_names,
            "email_notifications": semantic.get("notification_strategy", {}).get("emails") or email_names,
            "failure_handling": semantic.get("failure_handling", {}).get("failed_edges") or [edge for edge in final_config.get("dependencies") or [] if "Failed" in json.dumps(edge, default=str)],
            "retry_logic": final_config.get("retry_policies") or [],
            "branching_logic": [item.get("name") for item in activities if str(item.get("type") or "").lower() in {"ifcondition", "switch", "until"}],
        },
        "source_discovery": {
            "datasets": datasets,
            "linked_services": linked_services,
            "notebook_references": final_config.get("notebook_references") or [],
            "api_patterns": [
                {
                    "activity_type": "WebActivity",
                    "dynamic_expressions": [expr.get("expression") for expr in semantic.get("dynamic_expressions", []) if "url" in expr.get("field_path", "").lower() or "baseurl" in expr.get("expression", "").lower()],
                    "orchestration_role": "Source call",
                    "lineage_role": "Upstream source",
                }
            ] if any("web" in str(item.get("type") or "").lower() for item in activities) else [],
            "source_systems": semantic.get("source_systems") or [],
        },
        "trigger_scheduling_analysis": {
            "triggers": final_config.get("triggers") or [],
            "scheduling": schedule,
            "retry_policies": final_config.get("retry_policies") or [],
        },
        "ai_insights": insights,
        "activity_types": sorted({kind for kind in activity_types if kind}),
        "pipeline_pattern": inferred.get("architecture_pattern"),
        "architecture": inferred.get("architecture") or {},
        "metadata_driven_ingestion": inferred.get("metadata_driven_ingestion") or {},
        "execution_flow": semantic.get("execution_flow") or [],
        "lineage": semantic.get("lineage") or [],
        "dynamic_expressions": semantic.get("dynamic_expressions") or [],
        "audit_framework": semantic.get("audit_framework") or {},
        "operational_monitoring": semantic.get("operational_monitoring") or {},
        "retry_strategy": semantic.get("retry_strategy") or {},
        "notification_strategy": semantic.get("notification_strategy") or {},
    }


def _cloud_bundle_ai(final_config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    azure_key = os.getenv("AZURE_OPENAI_API_KEY", "")
    azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT", "")
    if not azure_key or not azure_endpoint:
        return None

    prompt = {
        "role": "Pipeline Discovery & Ingestion Intelligence Agent",
        "task": "Enrich the deterministic Microsoft Fabric evidence model. Return only valid JSON and never add technologies, formats, triggers, storage types, or load strategies without explicit evidence from the provided config.",
        "required_output": {
            "discovered": {},
            "resolved": {},
            "inferred": {},
            "unknown": {},
            "confidence_scores": {},
            "evidence_map": {},
            "dq_recommendations": []
        },
        "final_pipeline_config": final_config,
    }
    body = {
        "messages": [
            {"role": "system", "content": "You are a deterministic Fabric pipeline evidence enricher. Preserve the discovered/resolved/inferred/unknown split. Never hallucinate technologies. If evidence is absent, keep fields null or NOT_PRESENT. Reply with valid JSON only."},
            {"role": "user", "content": json.dumps(prompt, default=str)},
        ],
        "max_tokens": 4000,
        "temperature": 0,
    }
    headers = {"Content-Type": "application/json", "api-key": azure_key}

    try:
        req = urlrequest.Request(azure_endpoint, data=json.dumps(body).encode("utf-8"), headers=headers, method="POST")
        with urlrequest.urlopen(req, timeout=90) as response:
            payload = json.loads(response.read().decode("utf-8"))
        content = payload.get("choices", [{}])[0].get("message", {}).get("content", "")
        return _safe_json_extract(content)
    except Exception as exc:
        logger.warning("Azure OpenAI bundle analysis failed: %s", exc)
        return None


async def analyze_fabric_bundle(
    client_name: str,
    file_bytes: bytes,
    filename: str,
    workspace_id: Optional[str] = None,
    pipeline_id: Optional[str] = None,
    use_cloud_llm: bool = True,
    authorization_token: Optional[str] = None,
    existing_analysis: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    uploaded_config = parse_uploaded_bundle(file_bytes, filename)

    discovery_result = existing_analysis or {}
    if workspace_id and pipeline_id:
        discovery_result = await analyze_pipeline_live(
            client_name=client_name,
            providers="fabric",
            target="fabric",
            auth_mode="sso" if authorization_token else None,
            credentials=None,
            use_cloud_llm=use_cloud_llm,
            llm_provider="gpt",
            use_local_llm=False,
            scan_mode="live",
            authorization_token=authorization_token,
            payload={"workspace_id": workspace_id, "pipeline_id": pipeline_id},
        )

    auto_discovered_config = _extract_auto_discovered_config(discovery_result or {})
    final_pipeline_config = merge_pipeline_configs(auto_discovered_config, uploaded_config)
    ai_structured_output = _cloud_bundle_ai(final_pipeline_config) if use_cloud_llm else None
    if not ai_structured_output:
        ai_structured_output = _rule_based_bundle_ai(final_pipeline_config)

    return {
        "status": "success",
        "bundle_name": filename,
        "uploaded_pipeline_config": uploaded_config,
        "auto_discovered_config": auto_discovered_config,
        "final_pipeline_config": final_pipeline_config,
        "manifest_analysis": {
            "name": (uploaded_config.get("manifest_json") or {}).get("name"),
            "type": (uploaded_config.get("manifest_json") or {}).get("type"),
            "metadata_keys": sorted(list((uploaded_config.get("manifest_metadata") or {}).keys()))[:40],
            "selected_pipeline_path": uploaded_config.get("selected_pipeline_path"),
            "file_count": len(uploaded_config.get("zip_inventory") or []),
        },
        "source_discovery": ai_structured_output.get("source_discovery") or {},
        "ingestion_intelligence": ai_structured_output.get("ingestion_intelligence") or {},
        "file_structure_intelligence": ai_structured_output.get("file_structure_intelligence") or {},
        "dq_recommendations": ai_structured_output.get("dq_recommendations") or [],
        "trigger_scheduling_analysis": ai_structured_output.get("trigger_scheduling_analysis") or {},
        "activity_dependency_graph": final_pipeline_config.get("activity_graph") or {"nodes": [], "edges": []},
        "ai_structured_output": ai_structured_output,
        "semantic_analysis": final_pipeline_config.get("semantic_analysis") or {},
        "resolvedExpressions": final_pipeline_config.get("resolvedExpressions") or [],
        "lineage": (final_pipeline_config.get("semantic_analysis") or {}).get("lineage") or [],
        "discovered": ai_structured_output.get("discovered") or (final_pipeline_config.get("semantic_analysis") or {}).get("discovered") or {},
        "resolved": ai_structured_output.get("resolved") or (final_pipeline_config.get("semantic_analysis") or {}).get("resolved") or {},
        "inferred": ai_structured_output.get("inferred") or (final_pipeline_config.get("semantic_analysis") or {}).get("inferred") or {},
        "unknown": ai_structured_output.get("unknown") or (final_pipeline_config.get("semantic_analysis") or {}).get("unknown") or {},
        "confidence_scores": ai_structured_output.get("confidence_scores") or (final_pipeline_config.get("semantic_analysis") or {}).get("confidence_scores") or {},
        "evidence_map": ai_structured_output.get("evidence_map") or (final_pipeline_config.get("semantic_analysis") or {}).get("evidence_map") or {},
    }
