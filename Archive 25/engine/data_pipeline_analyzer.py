"""
Strict Fabric / ADF data pipeline extraction.
"""
from __future__ import annotations

from collections import defaultdict, deque
import json
import logging
from urllib.parse import urlparse
import re
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from engine.detectors.base import AnalysisPayload


logger = logging.getLogger(__name__)

UNKNOWN = "unknown"
NOTIFICATION_ACTIVITY_TYPES = {
    "office365email",
    "sendemail",
    "teamsactivity",
    "webhook",
    "webhookactivity",
    "message",
}
VALIDATION_ACTIVITY_TYPES = {
    "ifcondition",
    "assert",
    "validation",
    "fail",
    "until",
    "switch",
}
DESTINATION_ACTIVITY_TYPES = {
    "copy",
    "copyactivity",
    "script",
    "storedprocedure",
    "sqlserverstoredprocedure",
}


def analyze_data_pipelines(payload: AnalysisPayload) -> List[Dict[str, Any]]:
    pipelines: List[Dict[str, Any]] = []
    pipelines.extend(_extract_adf_pipelines(payload))
    pipelines.extend(_extract_fabric_pipelines(payload))
    unique_pipelines = _deduplicate_pipeline_reports(pipelines)
    
    for p in unique_pipelines:
        p["capabilities"] = _generate_capability_matrix(p)
        
    return unique_pipelines

def _generate_capability_matrix(report: Dict[str, Any]) -> Dict[str, Any]:
    matrix = []
    
    flow = report.get("flow", {})
    graph = flow.get("graph", {})
    nodes = graph.get("nodes", [])
    
    source_configs = report.get("source_configs", {})
    dq_rules = report.get("dq_rules", [])
    original = report.get("original", {})
    
    def has_node_type(type_name: str) -> bool:
        return any(str(n.get("type", "")).lower() == type_name.lower() for n in nodes)
    
    def has_node_id_contains(substring: str) -> bool:
        return any(substring.lower() in str(n.get("id", "")).lower() for n in nodes)

    api_supported = has_node_type("WebActivity") or has_node_id_contains("http") or has_node_id_contains("rest")
    if api_supported:
        matrix.append({"capability": "API Ingestion", "status": "SUPPORTED", "reason": "Web/HTTP activity detected in pipeline flow"})
    else:
        matrix.append({"capability": "API Ingestion", "status": "NOT_SUPPORTED", "reason": "No Web/API activity found"})

    source_str = str(source_configs).lower() + str(nodes).lower() + str(original).lower()
    file_supported = any(ext in source_str for ext in ["csv", "json", "adls", "s3", "blob", "parquet", "delimited"])
    if file_supported:
        matrix.append({"capability": "File Ingestion", "status": "SUPPORTED", "reason": "File-based source or format detected"})
    else:
        matrix.append({"capability": "File Ingestion", "status": "NOT_SUPPORTED", "reason": "No file-based sources detected"})

    db_supported = any(ext in source_str for ext in ["warehouse", "lakehouse", "sql", "table", "database"])
    if db_supported:
        matrix.append({"capability": "Database/Table Ingestion", "status": "SUPPORTED", "reason": "Database or table source detected"})
    else:
        matrix.append({"capability": "Database/Table Ingestion", "status": "NOT_SUPPORTED", "reason": "No database or table sources detected"})

    batch_supported = has_node_type("ForEach") or has_node_type("Until") or has_node_id_contains("foreach") or has_node_id_contains("loop")
    if batch_supported:
        matrix.append({"capability": "Batch Processing", "status": "SUPPORTED", "reason": "ForEach or loop activity detected in pipeline flow"})
    else:
        matrix.append({"capability": "Batch Processing", "status": "NOT_SUPPORTED", "reason": "No batch/loop activities found"})

    streaming_supported = any(ext in source_str for ext in ["event", "stream", "kafka", "eventhub"])
    if streaming_supported:
        matrix.append({"capability": "Streaming", "status": "SUPPORTED", "reason": "Streaming or event trigger detected"})
    else:
        matrix.append({"capability": "Streaming", "status": "NOT_SUPPORTED", "reason": "No streaming configs found"})

    if dq_rules:
        matrix.append({"capability": "Data Quality", "status": "SUPPORTED", "reason": "Explicit DQ rules detected"})
    elif has_node_type("IfCondition") or has_node_type("Condition") or has_node_id_contains("condition"):
        matrix.append({"capability": "Data Quality", "status": "PARTIAL", "reason": "Condition present but no explicit DQ rules"})
    else:
        matrix.append({"capability": "Data Quality", "status": "NOT_SUPPORTED", "reason": "No DQ rules or condition activities found"})

    notif_supported = has_node_id_contains("email") or has_node_id_contains("alert") or has_node_id_contains("notification")
    if notif_supported:
        matrix.append({"capability": "Notifications", "status": "SUPPORTED", "reason": "Email or alert activity detected"})
    else:
        matrix.append({"capability": "Notifications", "status": "NOT_SUPPORTED", "reason": "No notification activities found"})

    summary = {
        "supported_count": sum(1 for m in matrix if m["status"] == "SUPPORTED"),
        "not_supported_count": sum(1 for m in matrix if m["status"] == "NOT_SUPPORTED"),
        "partial_count": sum(1 for m in matrix if m["status"] == "PARTIAL"),
    }
    
    logger.info("Capability Matrix generated for %s: Supported: %d, Partial: %d, Not Supported: %d", report.get("pipeline_name", "Unknown"), summary["supported_count"], summary["partial_count"], summary["not_supported_count"])
    for m in matrix:
        logger.debug("Capability %s: %s (%s)", m["capability"], m["status"], m["reason"])

    return {
        "capability_matrix": matrix,
        "summary": summary
    }


def _deduplicate_pipeline_reports(reports: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    unique_reports: List[Dict[str, Any]] = []
    seen_keys: Set[str] = set()
    skipped_count = 0
    total_count = len(reports)

    def norm_node(n_id: str) -> str:
        val = str(n_id).lower().strip()
        val = re.sub(r'_?\d+$', '', val)
        return val

    for report in reports:
        name = str(report.get("pipeline_name", "")).strip()
        flow = report.get("flow", {})
        graph = flow.get("graph", {})
        
        nodes = []
        for n in graph.get("nodes", []):
            nodes.append(norm_node(n.get("id", "")) + "::" + str(n.get("type", "")).lower().strip())
        nodes.sort()
        
        edges = []
        for e in graph.get("edges", []):
            edges.append(norm_node(e.get("from", "")) + "->" + norm_node(e.get("to", "")))
        edges.sort()
        
        sig_dict = {
            "nodes": nodes,
            "edges": edges,
            "text": str(flow.get("text", "")).lower().strip()
        }
        pipeline_key = "sig:" + json.dumps(sig_dict, sort_keys=True)
        logger.debug("Pipeline dedup: flow signature per pipeline %s = %s", name, pipeline_key)
            
        if pipeline_key in seen_keys:
            skipped_count += 1
            logger.info("Pipeline dedup: duplicate flow skipped: %s", name)
            continue
            
        seen_keys.add(pipeline_key)
        unique_reports.append(report)

    if reports:
        rendered_names = [p.get("pipeline_name") for p in unique_reports]
        total_nodes = sum(len(p.get("flow", {}).get("graph", {}).get("nodes", [])) for p in unique_reports)
        total_edges = sum(len(p.get("flow", {}).get("graph", {}).get("edges", [])) for p in unique_reports)
        logger.info("Pipeline dedup: total flows before dedup = %d", total_count)
        logger.info("Pipeline dedup: unique flow structures after dedup = %d", len(unique_reports))
        logger.info("Pipeline dedup: duplicate flow structures skipped = %d", skipped_count)
        logger.info("Pipeline dedup: pipeline names rendered = %s", rendered_names)
        logger.info("Pipeline dedup: final rendered nodes count = %d", total_nodes)
        logger.info("Pipeline dedup: final rendered edges count = %d", total_edges)

    return unique_reports


def _extract_adf_pipelines(payload: AnalysisPayload) -> List[Dict[str, Any]]:
    raw = payload.raw_json
    if str(raw.get("type", "")).lower() != "adf_pipeline":
        return []

    name = _first_string(
        raw.get("name"),
        payload.metadata.get("name"),
        payload.config.get("pipeline_name"),
        payload.metadata.get("pipeline_name"),
    ) or UNKNOWN
    activities = raw.get("activities") if isinstance(raw.get("activities"), list) else []
    analysis = _analyze_pipeline_structure(
        activities=activities,
        payload=payload,
        definition=raw,
        workspace_id=_first_string(payload.metadata.get("workspaceId"), payload.config.get("workspaceId")),
        pipeline_name=name,
        explicit_destination=_first_string(
            payload.config.get("destination"),
            payload.config.get("output"),
            raw.get("destination"),
            raw.get("output"),
        ),
    )
    trigger_type, frequency = _detect_trigger(payload, raw)
    ingestion_config = {
        "mode": _detect_mode(payload, activities, raw),
        "trigger_type": trigger_type,
        "frequency": frequency,
        "data_format": analysis["data_format"],
        "destination": analysis["destination"],
    }

    report = {
        "type": "DataPipeline",
        "pipeline_name": name,
        "platform": "ADF",
        "workspace_id": _first_string(payload.metadata.get("workspaceId"), payload.config.get("workspaceId")),
        "artifact_id": _first_string(raw.get("id"), raw.get("pipelineId")),
        "source_configs": analysis["source_config"],
        "ingestion_configs": ingestion_config,
        "ingestion_details": analysis["ingestion_details"],
        "dq_rules": analysis["dq_rules"],
        "flow": analysis["flow"],
        "reformatted": {
            "pipeline_name": name,
            "platform": "ADF",
            "source_config": analysis["source_config"],
            "source_configs": analysis["source_config"],
            "ingestion_config": ingestion_config,
            "ingestion_configs": ingestion_config,
            "ingestion_details": analysis["ingestion_details"],
            "dq_rules": analysis["dq_rules"],
            "flow": analysis["flow"],
            "missing_fields_analysis": _build_missing_fields_analysis(
                source_config=analysis["source_config"],
                ingestion_config=ingestion_config,
                dq_rules=analysis["dq_rules"],
                flow=analysis["flow"],
                original=raw,
            ),
        },
        "original": raw if isinstance(raw, dict) else {},
    }
    return [report]


def _extract_fabric_pipelines(payload: AnalysisPayload) -> List[Dict[str, Any]]:
    reports: List[Dict[str, Any]] = []
    for item in _cloud_items(payload, "fabric_items"):
        if not isinstance(item, dict):
            continue
        item_id = str(item.get("id", ""))
        config = item.get("configuration", {}) if isinstance(item.get("configuration"), dict) else {}
        item_type = str(config.get("Type", "")).lower()
        if item_type not in {"pipeline", "datapipeline"}:
            continue

        name = item_id.split("||")[-1].strip() if item_id else UNKNOWN
        workspace_id = _first_string(
            config.get("WorkspaceId"),
            config.get("workspaceId"),
            config.get("Workspace"),
        )
        definition = _extract_fabric_definition(config)
        activities = _extract_fabric_activities(definition)
        analysis = _analyze_pipeline_structure(
            activities=activities,
            payload=payload,
            definition=definition or config,
            workspace_id=workspace_id,
            pipeline_name=name,
            explicit_destination=None,
        )
        trigger_type, frequency = _detect_trigger(payload, definition or config)
        ingestion_config = {
            "mode": _detect_mode(payload, activities, definition or config),
            "trigger_type": trigger_type,
            "frequency": frequency,
            "data_format": analysis["data_format"],
            "destination": analysis["destination"],
        }
        report_original = definition if definition else config
        reports.append(
            {
                "type": "DataPipeline",
                "pipeline_name": name,
                "platform": "Fabric",
                "workspace_id": workspace_id,
                "artifact_id": _first_string(config.get("objectId"), config.get("id"), config.get("pipelineId"), item_id),
                "source_configs": analysis["source_config"],
                "ingestion_configs": ingestion_config,
                "ingestion_details": analysis["ingestion_details"],
                "dq_rules": analysis["dq_rules"],
                "flow": analysis["flow"],
                "reformatted": {
                    "pipeline_name": name,
                    "platform": "Fabric",
                    "source_config": analysis["source_config"],
                    "source_configs": analysis["source_config"],
                    "ingestion_config": ingestion_config,
                    "ingestion_configs": ingestion_config,
                    "ingestion_details": analysis["ingestion_details"],
                    "dq_rules": analysis["dq_rules"],
                    "flow": analysis["flow"],
                    "missing_fields_analysis": _build_missing_fields_analysis(
                        source_config=analysis["source_config"],
                        ingestion_config=ingestion_config,
                        dq_rules=analysis["dq_rules"],
                        flow=analysis["flow"],
                        original=report_original,
                    ),
                },
                "original": report_original,
            }
        )

    return reports


def _analyze_pipeline_structure(
    *,
    activities: List[Dict[str, Any]],
    payload: AnalysisPayload,
    definition: Dict[str, Any],
    workspace_id: Optional[str],
    pipeline_name: str,
    explicit_destination: Optional[str],
) -> Dict[str, Any]:
    ordered_activities = _order_activities(activities)
    source_candidates = _collect_source_candidates(ordered_activities, definition, workspace_id)
    if not source_candidates:
        source_candidates = _collect_payload_level_source_candidates(payload, workspace_id, definition)
    dq_rules = _detect_dq_rules(payload, definition, ordered_activities)
    data_format = _detect_data_format(payload, definition, ordered_activities)
    if ordered_activities:
        graph = _build_flow_graph(ordered_activities, source_candidates)
    else:
        graph = {"nodes": [{"id": _slug(pipeline_name), "type": "process"}], "edges": []}
    destination = _detect_destination(
        activities=ordered_activities,
        explicit_destination=explicit_destination,
        workspace_id=workspace_id,
    )
    flow_text = _build_flow_text(
        source_candidates=source_candidates,
        activities=ordered_activities,
        destination=destination,
        pipeline_name=pipeline_name,
        dq_rules=dq_rules,
    )
    ingestion_details = _extract_ingestion_details(
        activities=ordered_activities,
        definition=definition,
        source_candidates=source_candidates,
        destination=destination,
    )

    logger.debug(
        "Pipeline analysis complete for %s | sources=%s destination=%s dq_rules=%s data_format=%s",
        pipeline_name,
        source_candidates,
        destination,
        dq_rules,
        data_format,
    )

    return {
        "source_config": _merge_source_candidates(source_candidates, definition, workspace_id),
        "destination": destination,
        "dq_rules": dq_rules,
        "data_format": data_format,
        "ingestion_details": ingestion_details,
        "flow": {
            "text": flow_text,
            "graph": graph,
        },
    }


def _extract_ingestion_details(
    *,
    activities: List[Dict[str, Any]],
    definition: Dict[str, Any],
    source_candidates: List[Dict[str, Any]],
    destination: Optional[Any],
) -> Dict[str, Any]:
    source_datasets = _collect_activity_datasets(activities, "source")
    sink_datasets = _collect_activity_datasets(activities, "sink")
    combined_text = f"{str(definition).lower()} {str(activities).lower()} {str(source_candidates).lower()} {str(destination).lower()}"
    dataset_types = [
        str(dataset_type).lower()
        for dataset_type in (
            _dataset_type_name(dataset) for dataset in [*source_datasets, *sink_datasets]
        )
        if dataset_type
    ]
    source_services = [
        str(candidate.get("service_name", "")).lower()
        for candidate in source_candidates
        if candidate.get("service_name")
    ]

    ingestion_types: List[str] = []
    if any(
        (
            _activity_type_in(activity, {"webactivity", "http", "rest", "graphql"})
            or _text_contains_any(str(activity).lower(), ("webactivity", "http", "rest", "graphql"))
        )
        for activity in activities
    ):
        ingestion_types.append("API")

    if (
        any(dataset_type in {"delimitedtext", "json", "parquet"} for dataset_type in dataset_types)
        or any(service in {"lakehouse"} for service in source_services)
        or any(_is_file_dataset(dataset) for dataset in [*source_datasets, *sink_datasets])
        or _text_contains_any(combined_text, ("blob", "s3", "adls", "onelake"))
    ):
        ingestion_types.append("File-based")

    if _text_contains_any(combined_text, ("sql", "jdbc", "datawarehouse", "table", "sink to warehouse", "warehousetable")):
        ingestion_types.append("Database")

    if _text_contains_any(combined_text, ("kafka", "eventhub", "kinesis")):
        ingestion_types.append("Streaming")

    file_types_input = _dedupe_strings(
        _normalize_file_type(_dataset_type_name(dataset))
        for dataset in source_datasets
    )
    file_types_output = _dedupe_strings(
        _normalize_file_type(_dataset_type_name(dataset))
        for dataset in sink_datasets
    )

    if "API" in ingestion_types and not file_types_input:
        file_types_input = ["JSON"]

    delimiters = {
        "column_delimiter": _find_first_key_value(definition, "columnDelimiter"),
        "escape_char": _find_first_key_value(definition, "escapeChar"),
        "quote_char": _find_first_key_value(definition, "quoteChar"),
        "header": _find_first_key_value(definition, "firstRowAsHeader"),
    }

    loading_flow = _build_loading_flow(
        activities=activities,
        source_candidates=source_candidates,
        source_datasets=source_datasets,
        sink_datasets=sink_datasets,
        destination=destination,
    )

    return {
        "ingestion_types": ingestion_types,
        "file_types": {
            "input": file_types_input,
            "output": file_types_output,
        },
        "delimiters": delimiters,
        "loading_flow": loading_flow,
    }


def _collect_activity_datasets(activities: List[Dict[str, Any]], preferred_key: str) -> List[Dict[str, Any]]:
    datasets: List[Dict[str, Any]] = []
    for activity in activities:
        type_properties = activity.get("typeProperties", {}) if isinstance(activity.get("typeProperties"), dict) else {}
        dataset = _find_dataset_settings(type_properties, preferred_key)
        if dataset:
            datasets.append(dataset)
    return datasets


def _activity_type_in(activity: Dict[str, Any], values: Set[str]) -> bool:
    activity_type = str(activity.get("type", "")).lower()
    return activity_type in values


def _text_contains_any(text: str, values: Tuple[str, ...]) -> bool:
    return any(value in text for value in values)


def _dataset_type_name(dataset: Dict[str, Any]) -> Optional[str]:
    return _first_string(
        dataset.get("type"),
        dataset.get("format"),
        dataset.get("formatType"),
        dataset.get("fileFormat"),
    )


def _normalize_file_type(dataset_type: Optional[str]) -> Optional[str]:
    if not dataset_type:
        return None
    normalized = str(dataset_type).strip()
    lowered = normalized.lower()
    mapping = {
        "delimitedtext": "DelimitedText",
        "json": "JSON",
        "parquet": "Parquet",
    }
    return mapping.get(lowered)


def _dedupe_strings(values: Iterable[Optional[str]]) -> List[str]:
    deduped: List[str] = []
    seen: Set[str] = set()
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def _build_loading_flow(
    *,
    activities: List[Dict[str, Any]],
    source_candidates: List[Dict[str, Any]],
    source_datasets: List[Dict[str, Any]],
    sink_datasets: List[Dict[str, Any]],
    destination: Optional[Any],
) -> str:
    stages: List[Tuple[str, str]] = []
    stage_seen: Set[Tuple[str, str]] = set()

    def add_stage(stage: str, label: Optional[str]) -> None:
        if not label:
            return
        entry = (stage, label)
        if entry in stage_seen:
            return
        stage_seen.add(entry)
        stages.append(entry)

    for candidate in source_candidates:
        if str(candidate.get("source_type")) == "API":
            add_stage("SOURCE", _source_candidate_entity_label(candidate))

    for dataset in source_datasets:
        if _is_file_dataset(dataset):
            add_stage("SOURCE", _dataset_entity_label(dataset))
        elif _is_database_dataset(dataset):
            add_stage("SOURCE", _dataset_entity_label(dataset))

    for activity in activities:
        activity_type = str(activity.get("type", "")).lower()
        type_properties = activity.get("typeProperties", {}) if isinstance(activity.get("typeProperties"), dict) else {}
        sink_dataset = _find_dataset_settings(type_properties, "sink")

        if activity_type == "webactivity":
            add_stage("SOURCE", _web_activity_label(activity))

        if sink_dataset and _is_file_dataset(sink_dataset):
            add_stage("STAGING", _dataset_entity_label(sink_dataset))

        if activity_type in {"tridentnotebook", "databricksnotebook", "synapsenotebook", "notebook", "script", "storedprocedure", "sqlserverstoredprocedure"}:
            add_stage("TRANSFORMATION", _activity_label(activity))

        if sink_dataset and _is_database_dataset(sink_dataset):
            add_stage("DESTINATION", _dataset_entity_label(sink_dataset))

    for dataset in sink_datasets:
        if _is_database_dataset(dataset):
            add_stage("DESTINATION", _dataset_entity_label(dataset))

    if not any(stage == "DESTINATION" for stage, _ in stages) and destination is not None:
        destination_label = _display_value(destination)
        add_stage("DESTINATION", destination_label)

    return " -> ".join(f"{stage}: {label}" for stage, label in stages)


def _source_candidate_entity_label(candidate: Dict[str, Any]) -> str:
    details = candidate.get("connection_details", {}) if isinstance(candidate.get("connection_details"), dict) else {}
    endpoint = _first_string(details.get("endpoint"))
    if endpoint:
        return _endpoint_entity_label(endpoint)
    return _source_candidate_label(candidate)


def _web_activity_label(activity: Dict[str, Any]) -> str:
    type_properties = activity.get("typeProperties", {}) if isinstance(activity.get("typeProperties"), dict) else {}
    endpoint = _first_string(type_properties.get("url"), type_properties.get("relativeUrl"))
    if endpoint:
        return _endpoint_entity_label(endpoint)
    return _activity_label(activity)


def _endpoint_entity_label(endpoint: str) -> str:
    try:
        parsed = urlparse(endpoint)
        path_parts = [part for part in parsed.path.split("/") if part]
        if path_parts:
            return f"{path_parts[-1]} API"
    except Exception:
        logger.debug("Could not parse endpoint label from %s", endpoint)
    return endpoint


def _dataset_entity_label(dataset: Dict[str, Any]) -> str:
    return (
        _first_string(
            dataset.get("table"),
            dataset.get("tableName"),
            dataset.get("entity"),
            dataset.get("path"),
            dataset.get("folderPath"),
            dataset.get("fileName"),
            dataset.get("artifactId"),
            dataset.get("type"),
        )
        or "unknown"
    )


def _is_file_dataset(dataset: Dict[str, Any]) -> bool:
    dataset_text = str(dataset).lower()
    return any(token in dataset_text for token in ("delimitedtext", "json", "parquet", "blob", "s3", "adls", "lakehouse", "file", "onelake"))


def _is_database_dataset(dataset: Dict[str, Any]) -> bool:
    dataset_text = str(dataset).lower()
    return any(token in dataset_text for token in ("sql", "jdbc", "datawarehouse", "table", "warehousetable"))


def _collect_source_candidates(
    activities: List[Dict[str, Any]],
    definition: Dict[str, Any],
    workspace_id: Optional[str],
) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []

    for activity in activities:
        if not isinstance(activity, dict):
            continue
        activity_type = str(activity.get("type", "")).lower()
        type_properties = activity.get("typeProperties", {}) if isinstance(activity.get("typeProperties"), dict) else {}
        logger.debug("Inspecting source candidates from activity '%s' (%s)", activity.get("name"), activity_type)

        if activity_type == "webactivity":
            endpoint = _first_string(type_properties.get("url"), type_properties.get("relativeUrl"))
            candidates.append(
                {
                    "source_type": "API",
                    "service_name": "REST API",
                    "connection_details": _compact(
                        {
                            "workspaceId": workspace_id,
                            "endpoint": endpoint,
                            "method": _first_string(type_properties.get("method")),
                        }
                    ),
                    "authentication_type": _detect_auth_from_definition(definition),
                }
            )
            continue

        source_dataset = _find_dataset_settings(type_properties, "source")
        if source_dataset:
            source_type, service_name, details = _classify_dataset_settings(source_dataset, workspace_id)
            candidates.append(
                {
                    "source_type": source_type,
                    "service_name": service_name,
                    "connection_details": details,
                    "authentication_type": _detect_auth_from_definition(definition),
                }
            )

    normalized = _dedupe_records(candidates)
    logger.debug("Source candidates found: %s", normalized)
    return normalized


def _merge_source_candidates(
    candidates: List[Dict[str, Any]],
    definition: Dict[str, Any],
    workspace_id: Optional[str],
) -> Dict[str, Any]:
    if not candidates:
        return {
            "source_type": None,
            "service_name": None,
            "connection_details": _compact({"workspaceId": workspace_id, "workspace": workspace_id}),
            "authentication_type": _normalize_missing(_detect_auth_from_definition(definition)),
        }

    source_types = {candidate.get("source_type") for candidate in candidates if candidate.get("source_type")}
    service_names = {candidate.get("service_name") for candidate in candidates if candidate.get("service_name")}
    auth_types = {
        candidate.get("authentication_type")
        for candidate in candidates
        if candidate.get("authentication_type") not in (None, UNKNOWN)
    }
    connection_details = [candidate.get("connection_details", {}) for candidate in candidates]

    merged_details: Dict[str, Any]
    if len(connection_details) == 1:
        merged_details = dict(connection_details[0])
    else:
        merged_details = {"sources": connection_details}
    if workspace_id and "workspaceId" not in merged_details:
        merged_details["workspaceId"] = workspace_id
    if workspace_id and "workspace" not in merged_details:
        merged_details["workspace"] = workspace_id

    return {
        "source_type": next(iter(source_types)) if len(source_types) == 1 else ("Mixed" if source_types else None),
        "service_name": next(iter(service_names)) if len(service_names) == 1 else ("Mixed" if service_names else None),
        "connection_details": _compact(merged_details),
        "authentication_type": next(iter(auth_types)) if len(auth_types) == 1 else ("Mixed" if len(auth_types) > 1 else None),
    }


def _collect_payload_level_source_candidates(
    payload: AnalysisPayload,
    workspace_id: Optional[str],
    definition: Dict[str, Any],
) -> List[Dict[str, Any]]:
    text = payload.all_text()
    auth_type = _detect_auth_from_definition(definition)

    abfss_path = _first_match(text, r"abfss?://[^\s\"'>,]+")
    if abfss_path:
        container = None
        storage_account = None
        match = re.match(r"abfss?://([^@]+)@([^.]+)", abfss_path)
        if match:
            container = match.group(1)
            storage_account = match.group(2)
        return [
            {
                "source_type": "File",
                "service_name": "ADLS Gen2",
                "connection_details": _compact(
                    {
                        "workspaceId": workspace_id,
                        "workspace": workspace_id,
                        "path": abfss_path,
                        "container": container,
                        "storage_account": storage_account,
                        "endpoint": f"{storage_account}.dfs.core.windows.net" if storage_account else None,
                    }
                ),
                "authentication_type": auth_type,
            }
        ]

    api_url = _first_match(text, r"https?://[^\s\"'>,]+")
    if api_url:
        return [
            {
                "source_type": "API",
                "service_name": "REST API",
                "connection_details": _compact(
                    {
                        "workspaceId": workspace_id,
                        "workspace": workspace_id,
                        "endpoint": api_url,
                        "method": _first_string(payload.raw_json.get("method"), payload.config.get("method")),
                    }
                ),
                "authentication_type": auth_type,
            }
        ]

    jdbc_url = _first_match(text, r"jdbc:[a-z0-9]+://[^\s\"'>,]+")
    if jdbc_url:
        db_type = jdbc_url.split(":")[1].split("//")[0]
        return [
            {
                "source_type": "DB",
                "service_name": db_type.upper(),
                "connection_details": _compact(
                    {
                        "workspaceId": workspace_id,
                        "workspace": workspace_id,
                        "jdbc_url": jdbc_url,
                    }
                ),
                "authentication_type": auth_type,
            }
        ]

    return []


def _detect_mode(payload: AnalysisPayload, activities: List[Dict[str, Any]], definition: Dict[str, Any]) -> str:
    text = f"{payload.all_text()} {str(definition).lower()}"
    if any(term in text for term in ("stream", "streaming", "eventhub", "kafka")):
        return "streaming"
    if any(str(activity.get("type", "")).lower() == "foreach" for activity in activities):
        return "batch"
    return "batch"


def _detect_trigger(payload: AnalysisPayload, definition: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    trigger = _first_string(
        definition.get("trigger_type"),
        definition.get("trigger"),
        payload.raw_json.get("trigger_type"),
        payload.raw_json.get("trigger"),
        payload.config.get("trigger_type"),
        payload.config.get("trigger"),
    )
    frequency = _first_string(
        definition.get("frequency"),
        payload.raw_json.get("frequency"),
        payload.config.get("frequency"),
        definition.get("schedule"),
        payload.raw_json.get("schedule"),
        payload.config.get("schedule"),
        definition.get("cron"),
        payload.raw_json.get("cron"),
        payload.config.get("cron"),
    )
    recurrence = _find_first_key_value(definition, "recurrence")
    if isinstance(recurrence, dict):
        trigger = trigger or "schedule"
        frequency = frequency or _first_string(
            recurrence.get("frequency"),
            recurrence.get("interval"),
        )

    trigger = _normalize_missing(trigger.lower() if isinstance(trigger, str) else trigger)
    frequency = _normalize_missing(str(frequency) if frequency is not None else None)
    logger.debug("Trigger detection | trigger_type=%s frequency=%s", trigger, frequency)
    return trigger, frequency


def _detect_data_format(
    payload: AnalysisPayload,
    definition: Dict[str, Any],
    activities: List[Dict[str, Any]],
) -> Optional[Any]:
    formats: List[str] = []
    explicit = _first_string(
        payload.raw_json.get("format"),
        payload.config.get("format"),
        payload.raw_json.get("file_format"),
        payload.config.get("file_format"),
        payload.raw_json.get("data_format"),
        payload.config.get("data_format"),
    )
    if explicit:
        formats.append(explicit)

    text = f"{payload.all_text()} {str(definition).lower()}"
    for label in ("parquet", "json", "csv", "delta"):
        if label in text:
            formats.append(label.upper() if label != "delta" else "Delta")

    for activity in activities:
        type_properties = activity.get("typeProperties", {}) if isinstance(activity.get("typeProperties"), dict) else {}
        for dataset in (
            _find_dataset_settings(type_properties, "source"),
            _find_dataset_settings(type_properties, "sink"),
        ):
            if not dataset:
                continue
            dataset_format = _first_string(
                dataset.get("format"),
                dataset.get("formatType"),
                dataset.get("fileFormat"),
                dataset.get("type"),
            )
            if dataset_format:
                formats.append(dataset_format)

    deduped = list(dict.fromkeys(formats))
    logger.debug("Detected data formats: %s", deduped)
    if not deduped:
        return None
    return deduped[0] if len(deduped) == 1 else deduped


def _detect_destination(
    *,
    activities: List[Dict[str, Any]],
    explicit_destination: Optional[str],
    workspace_id: Optional[str],
) -> Optional[Any]:
    if explicit_destination:
        logger.debug("Destination resolved from explicit config: %s", explicit_destination)
        return explicit_destination

    outgoing, roots = _dependency_maps(activities)
    terminal_names = {name for name in roots.keys() if not outgoing.get(name)}
    sink_candidates: List[Dict[str, Any]] = []
    process_fallbacks: List[str] = []

    for activity in activities:
        name = _activity_label(activity)
        if name not in terminal_names:
            continue
        if _is_notification_activity(activity):
            logger.debug("Excluding notification activity '%s' from destination candidates", name)
            continue

        candidate = _extract_destination_candidate(activity, workspace_id)
        if candidate:
            sink_candidates.append(candidate)
            continue

        if not _is_validation_activity(activity):
            process_fallbacks.append(name)

    if sink_candidates:
        labels = [candidate["label"] for candidate in sink_candidates if candidate.get("label")]
        logger.debug("Destination candidates found: %s", sink_candidates)
        return labels[0] if len(labels) == 1 else labels

    for activity in reversed(activities):
        if _is_notification_activity(activity):
            continue
        candidate = _extract_destination_candidate(activity, workspace_id)
        if candidate:
            logger.debug("Destination resolved from non-terminal sink candidate: %s", candidate)
            return candidate["label"]

    if process_fallbacks:
        logger.debug("No sink artifact found; falling back to terminal process nodes: %s", process_fallbacks)
        return process_fallbacks[0] if len(process_fallbacks) == 1 else process_fallbacks

    logger.debug("No destination candidates found")
    return None


def _extract_destination_candidate(activity: Dict[str, Any], workspace_id: Optional[str]) -> Optional[Dict[str, Any]]:
    type_properties = activity.get("typeProperties", {}) if isinstance(activity.get("typeProperties"), dict) else {}
    sink_dataset = _find_dataset_settings(type_properties, "sink")
    if sink_dataset:
        source_type, service_name, details = _classify_dataset_settings(sink_dataset, workspace_id)
        label = _destination_label(details, service_name, activity)
        return {
            "label": label,
            "type": "destination",
            "service_name": service_name,
            "source_type": source_type,
            "details": details,
        }

    activity_type = str(activity.get("type", "")).lower()
    if activity_type in {"tridentnotebook", "databricksnotebook", "synapsenotebook", "notebook"}:
        return {
            "label": _activity_label(activity),
            "type": "destination",
            "service_name": "Notebook",
            "source_type": "Process",
            "details": _compact({"workspaceId": workspace_id, "notebookId": _first_string(type_properties.get("notebookId"))}),
        }

    return None


def _build_flow_graph(
    activities: List[Dict[str, Any]],
    source_candidates: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, str]]]:
    nodes: List[Dict[str, str]] = []
    edges: List[Dict[str, str]] = []
    activity_ids: Dict[str, str] = {}
    included_activity_names: Set[str] = set()

    for index, candidate in enumerate(source_candidates, start=1):
        node_id = f"source_{index}"
        nodes.append({"id": node_id, "type": "source"})

    for index, activity in enumerate(activities, start=1):
        if _is_notification_activity(activity):
            continue
        node_id = _slug(_activity_label(activity)) or f"activity_{index}"
        activity_ids[_activity_label(activity)] = node_id
        included_activity_names.add(_activity_label(activity))
        nodes.append({"id": node_id, "type": _activity_node_type(activity)})

    outgoing, incoming = _dependency_maps(activities)
    root_names = [name for name in activity_ids if not [dep for dep in incoming.get(name, []) if dep in included_activity_names]]
    for index, root_name in enumerate(root_names, start=1):
        if index <= len(source_candidates):
            edges.append({"from": f"source_{index}", "to": activity_ids[root_name]})

    for source_name, target_names in outgoing.items():
        for target_name in target_names:
            if source_name in activity_ids and target_name in activity_ids:
                edges.append({"from": activity_ids[source_name], "to": activity_ids[target_name]})

    logger.debug("Flow graph nodes=%s edges=%s", nodes, edges)
    return {"nodes": nodes, "edges": edges}


def _build_flow_text(
    *,
    source_candidates: List[Dict[str, Any]],
    activities: List[Dict[str, Any]],
    destination: Optional[Any],
    pipeline_name: str,
    dq_rules: List[str],
) -> str:
    source_part = ", ".join(
        _source_candidate_label(candidate) for candidate in source_candidates
    ) if source_candidates else "No explicit source"
    visible_activities = [activity for activity in activities if not _is_notification_activity(activity)]
    activity_part = " -> ".join(_activity_label(activity) for activity in visible_activities) if visible_activities else pipeline_name
    destination_part = _display_value(destination) or "no explicit sink"
    validation_part = " with validation gates" if dq_rules else ""
    return f"{source_part} -> {activity_part} -> {destination_part}{validation_part}"


def _detect_dq_rules(
    payload: AnalysisPayload,
    extra_obj: Any = None,
    activities: Optional[List[Dict[str, Any]]] = None,
) -> List[str]:
    rules: List[str] = []
    text_parts = [payload.all_text()]
    if extra_obj is not None:
        text_parts.append(str(extra_obj).lower())
    if activities:
        text_parts.append(str(activities).lower())
    text = " ".join(text_parts)

    expectations = _collect_values(payload.raw_json, "expectations")
    for collection in expectations:
        if not isinstance(collection, list):
            continue
        for item in collection:
            if not isinstance(item, dict):
                continue
            exp_type = str(item.get("expectation_type", ""))
            column = item.get("kwargs", {}).get("column") if isinstance(item.get("kwargs"), dict) else None
            if exp_type == "expect_column_values_to_not_be_null":
                rules.append(f"null_check:{column or 'column'}")
            elif exp_type == "expect_column_values_to_be_unique":
                rules.append(f"uniqueness_check:{column or 'column'}")
            elif exp_type == "expect_column_values_to_be_between":
                rules.append(f"range_check:{column or 'column'}")

    for phrase, rule in (
        ("count(*)", "row_count_check"),
        ("rowcount", "row_count_check"),
        ("is null", "null_check"),
        ("duplicate", "uniqueness_check"),
        ("distinct", "uniqueness_check"),
        ("schema", "schema_check"),
        ("status", "status_check"),
        ("ifcondition", "conditional_quality_gate"),
        ("@equals", "conditional_quality_gate"),
        ("failed", "status_check"),
    ):
        if phrase in text:
            rules.append(rule)

    deduped = list(dict.fromkeys(rules))
    logger.debug("DQ rules detected: %s", deduped)
    return deduped


def _extract_fabric_definition(config: Dict[str, Any]) -> Dict[str, Any]:
    definition = config.get("Definition")
    return definition if isinstance(definition, dict) else {}


def _extract_fabric_activities(definition: Dict[str, Any]) -> List[Dict[str, Any]]:
    pipeline_content = definition.get("pipeline-content.json", {})
    if not isinstance(pipeline_content, dict):
        return []
    properties = pipeline_content.get("properties", {})
    if not isinstance(properties, dict):
        return []
    activities = properties.get("activities", [])
    return activities if isinstance(activities, list) else []


def _detect_auth_from_definition(definition: Dict[str, Any]) -> Optional[str]:
    text = str(definition).lower()
    if "managedidentity" in text or "managed identity" in text:
        return "Managed Identity"
    if "oauth" in text:
        return "OAuth"
    if "apikey" in text or "api_key" in text:
        return "Key"
    if "basic" in text:
        return "Basic"
    if "connection" in text:
        return "Connection Reference"
    return None


def _find_dataset_settings(type_properties: Dict[str, Any], preferred_key: Optional[str] = None) -> Dict[str, Any]:
    if not isinstance(type_properties, dict):
        return {}

    keys: Iterable[str]
    if preferred_key == "source":
        keys = ("source", "datasetSettings", "storeSettings")
    elif preferred_key == "sink":
        keys = ("sink", "datasetSettings", "storeSettings")
    else:
        keys = ("source", "sink", "datasetSettings", "storeSettings")

    for key in keys:
        value = type_properties.get(key)
        if not isinstance(value, dict) or not value:
            continue
        nested = value.get("datasetSettings") if key in {"source", "sink"} else None
        if isinstance(nested, dict) and nested:
            return nested
        return value
    return {}


def _classify_dataset_settings(dataset: Dict[str, Any], workspace_id: Optional[str]) -> Tuple[Optional[str], Optional[str], Dict[str, Any]]:
    text = str(dataset).lower()
    details = _compact(
        {
            "workspaceId": workspace_id,
            "artifactId": _first_string(dataset.get("artifactId"), dataset.get("artifact_id")),
            "schema": _first_string(dataset.get("schema"), dataset.get("schemaName")),
            "table": _first_string(dataset.get("table"), dataset.get("tableName"), dataset.get("entity")),
            "path": _first_string(dataset.get("folderPath"), dataset.get("path"), dataset.get("fileName")),
            "endpoint": _first_string(dataset.get("relativeUrl"), dataset.get("url")),
            "format": _first_string(dataset.get("format"), dataset.get("formatType"), dataset.get("fileFormat")),
        }
    )

    if "http" in text or "rest" in text:
        return "API", "REST API", details
    if "warehouse" in text:
        return "DB", "DataWarehouse", details
    if "lakehouse" in text:
        if details.get("table"):
            return "DB", "Lakehouse", details
        return "File", "Lakehouse", details
    if any(marker in text for marker in ("datalake", "blob", "file", "deltalake", "onelake")):
        return "File", "File", details
    if details.get("table"):
        return "DB", "Database", details
    return None, None, details


def _order_activities(activities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not activities:
        return []

    name_to_activity: Dict[str, Dict[str, Any]] = {}
    indegree: Dict[str, int] = {}
    edges: Dict[str, List[str]] = defaultdict(list)
    unnamed: List[Dict[str, Any]] = []

    for index, activity in enumerate(activities, start=1):
        if not isinstance(activity, dict):
            continue
        name = _first_string(activity.get("name"), activity.get("activity"), activity.get("type"))
        if not name:
            unnamed.append(activity)
            continue
        if name in name_to_activity:
            name = f"{name}_{index}"
        name_to_activity[name] = activity
        indegree.setdefault(name, 0)

    for name, activity in name_to_activity.items():
        depends_on = activity.get("dependsOn") if isinstance(activity.get("dependsOn"), list) else []
        for dep in depends_on:
            dep_name = None
            if isinstance(dep, dict):
                dep_name = _first_string(dep.get("activity"), dep.get("name"), dep.get("referenceName"))
            elif isinstance(dep, str):
                dep_name = dep
            if dep_name and dep_name in name_to_activity:
                edges[dep_name].append(name)
                indegree[name] = indegree.get(name, 0) + 1

    queue = deque(sorted(name for name, degree in indegree.items() if degree == 0))
    ordered_names: List[str] = []
    while queue:
        current = queue.popleft()
        ordered_names.append(current)
        for nxt in edges.get(current, []):
            indegree[nxt] -= 1
            if indegree[nxt] == 0:
                queue.append(nxt)

    if len(ordered_names) != len(name_to_activity):
        ordered = list(name_to_activity.values())
    else:
        ordered = [name_to_activity[name] for name in ordered_names]

    ordered.extend(unnamed)
    return ordered


def _activity_label(activity: Dict[str, Any]) -> str:
    return _first_string(activity.get("name")) or _first_string(activity.get("type")) or "Activity"


def _activity_node_type(activity: Dict[str, Any]) -> str:
    activity_type = str(activity.get("type", "")).lower()
    if _is_notification_activity(activity):
        return "notification"
    if _is_validation_activity(activity):
        return "validation"
    if "copy" in activity_type:
        return "ingestion"
    if "notebook" in activity_type or "spark" in activity_type:
        return "process"
    return "process"


def _is_notification_activity(activity: Dict[str, Any]) -> bool:
    activity_type = str(activity.get("type", "")).lower()
    return activity_type in NOTIFICATION_ACTIVITY_TYPES or "email" in activity_type


def _is_validation_activity(activity: Dict[str, Any]) -> bool:
    activity_type = str(activity.get("type", "")).lower()
    return activity_type in VALIDATION_ACTIVITY_TYPES


def _dependency_maps(activities: List[Dict[str, Any]]) -> Tuple[Dict[str, List[str]], Dict[str, List[str]]]:
    outgoing: Dict[str, List[str]] = defaultdict(list)
    incoming: Dict[str, List[str]] = defaultdict(list)
    names = {_activity_label(activity): activity for activity in activities}
    for activity in activities:
        current_name = _activity_label(activity)
        depends_on = activity.get("dependsOn") if isinstance(activity.get("dependsOn"), list) else []
        for dep in depends_on:
            dep_name = None
            if isinstance(dep, dict):
                dep_name = _first_string(dep.get("activity"), dep.get("name"), dep.get("referenceName"))
            elif isinstance(dep, str):
                dep_name = dep
            if dep_name and dep_name in names:
                outgoing[dep_name].append(current_name)
                incoming[current_name].append(dep_name)
    for name in names:
        outgoing.setdefault(name, [])
        incoming.setdefault(name, [])
    return outgoing, incoming


def _source_candidate_label(candidate: Dict[str, Any]) -> str:
    details = candidate.get("connection_details", {})
    return (
        _first_string(
            candidate.get("service_name"),
            details.get("table"),
            details.get("endpoint"),
            details.get("path"),
        )
        or "Source"
    )


def _destination_label(details: Dict[str, Any], service_name: Optional[str], activity: Dict[str, Any]) -> str:
    return (
        _first_string(
            details.get("table"),
            details.get("path"),
            details.get("artifactId"),
            service_name,
            _activity_label(activity),
        )
        or "Sink"
    )


def _display_value(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, list):
        return ", ".join(str(item) for item in value)
    return str(value)


def _collect_values(obj: Any, target_key: str) -> List[Any]:
    values: List[Any] = []
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key.lower() == target_key.lower():
                values.append(value)
            values.extend(_collect_values(value, target_key))
    elif isinstance(obj, list):
        for item in obj:
            values.extend(_collect_values(item, target_key))
    return values


def _find_first_key_value(obj: Any, target_key: str) -> Any:
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key.lower() == target_key.lower():
                return value
            nested = _find_first_key_value(value, target_key)
            if nested is not None:
                return nested
    elif isinstance(obj, list):
        for item in obj:
            nested = _find_first_key_value(item, target_key)
            if nested is not None:
                return nested
    return None


def _cloud_items(payload: AnalysisPayload, service_key: str) -> List[Dict[str, Any]]:
    raw_dump = payload.raw_json.get("raw_cloud_dump", [])
    items: List[Dict[str, Any]] = []
    if isinstance(raw_dump, list):
        for dump in raw_dump:
            if isinstance(dump, dict):
                service_items = dump.get(service_key, [])
                if isinstance(service_items, list):
                    items.extend(service_items)
    return items


def _first_string(*values: Any) -> Optional[str]:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _first_match(text: str, pattern: str) -> Optional[str]:
    match = re.search(pattern, text, re.IGNORECASE)
    return match.group(0) if match else None


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return slug or "node"


def _compact(value: Dict[str, Any]) -> Dict[str, Any]:
    return {key: item for key, item in value.items() if item not in (None, "", [], {}, UNKNOWN)}


def _normalize_missing(value: Any) -> Any:
    return None if value in (None, "", UNKNOWN) else value


def _dedupe_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    for record in records:
        serialized = json.dumps(record, sort_keys=True, default=str)
        if serialized in seen:
            continue
        seen.add(serialized)
        deduped.append(record)
    return deduped


def _build_missing_fields_analysis(
    *,
    source_config: Dict[str, Any],
    ingestion_config: Dict[str, Any],
    dq_rules: List[str],
    flow: Dict[str, Any],
    original: Dict[str, Any],
) -> List[Dict[str, str]]:
    missing: List[Dict[str, str]] = []

    for field in ("source_type", "service_name", "authentication_type"):
        if source_config.get(field) in (None, ""):
            missing.append({"field": f"source_configs.{field}", "reason": _reason_for_field(field, original)})

    connection_details = source_config.get("connection_details", {}) if isinstance(source_config.get("connection_details"), dict) else {}
    for field in ("workspaceId", "artifactId", "schema", "table", "endpoint", "path"):
        if field not in connection_details:
            missing.append({"field": f"source_configs.connection_details.{field}", "reason": _reason_for_connection_field(field, original)})

    for field in ("trigger_type", "frequency", "data_format", "destination"):
        if ingestion_config.get(field) in (None, ""):
            missing.append({"field": f"ingestion_configs.{field}", "reason": _reason_for_field(field, original)})

    if not dq_rules:
        missing.append(
            {
                "field": "dq_rules",
                "reason": "No expectations, conditional validations, status checks, row-count checks, or schema validation patterns were detected in the pipeline JSON.",
            }
        )

    if flow.get("text") in (None, ""):
        missing.append(
            {
                "field": "flow.text",
                "reason": "No ordered execution path could be built from activity dependencies in the pipeline definition.",
            }
        )

    return missing


def _reason_for_field(field: str, original: Dict[str, Any]) -> str:
    original_text = str(original).lower()
    reasons = {
        "source_type": "No source activity or source dataset metadata was present to classify the upstream source.",
        "service_name": "No linked service, dataset type, or activity pattern identified the backing source service.",
        "authentication_type": "No explicit authentication, credential, or connection-reference metadata was found in the pipeline JSON.",
        "trigger_type": "No trigger metadata, recurrence block, or schedule definition was present in the pipeline JSON.",
        "frequency": "No recurrence frequency, interval, cron, or schedule metadata was present in the pipeline JSON.",
        "data_format": "No source/sink format settings or recognizable format markers were found in the pipeline JSON.",
        "destination": "No warehouse, lakehouse, file sink, or final processing target could be inferred from terminal activities.",
    }
    if field in {"trigger_type", "frequency"} and "recurrence" in original_text:
        return "Recurrence metadata exists but does not contain a normalized frequency or trigger value."
    return reasons.get(field, "Field not present in JSON or not inferable with current parser logic.")


def _reason_for_connection_field(field: str, original: Dict[str, Any]) -> str:
    reasons = {
        "workspaceId": "Workspace metadata was not present on the pipeline item or the dataset definition.",
        "artifactId": "No lakehouse or warehouse artifact identifier was present in the referenced dataset settings.",
        "schema": "No explicit schema field was present in the referenced dataset settings.",
        "table": "No explicit table name was present in the referenced dataset settings.",
        "endpoint": "No API endpoint or service URL was present in the source activity metadata.",
        "path": "No folder path, file path, or OneLake path was present in the source dataset settings.",
    }
    if field == "endpoint" and "webactivity" in str(original).lower():
        return "A WebActivity exists, but no explicit URL field was present in its typeProperties."
    return reasons.get(field, "Connection detail not present in pipeline JSON.")
