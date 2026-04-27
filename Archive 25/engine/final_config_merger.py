from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional, Set, Tuple

from engine.data_pipeline_analyzer import analyze_data_pipelines
from engine.detectors.base import AnalysisPayload
from llm.inference import llm_infer_data_pipeline_reasoning


NOTIFICATION_MARKERS = ("email", "notification", "alert", "teams", "webhook")


def finalize_pipeline_config(
    *,
    raw_pipeline_json: Dict[str, Any],
    extracted_config: Dict[str, Any],
    example_config: Dict[str, Any],
    ui_inputs: Dict[str, Any],
    use_llm: bool = False,
) -> Dict[str, Any]:
    extracted = _normalize_config(extracted_config)
    example = _normalize_config(example_config)
    raw_derived = _derive_from_raw(raw_pipeline_json, extracted, ui_inputs)

    merge_report = {
        "fields_from_extracted": [],
        "fields_from_example": [],
        "fields_corrected": [],
        "conflicts_resolved": [],
    }
    architect_notes: List[str] = []

    final_core = {
        "source_configs": {},
        "ingestion_configs": {},
        "dq_rules": [],
        "flow": {},
        "missing_fields_analysis": [],
    }

    final_core["source_configs"] = _merge_section(
        section_name="source_configs",
        extracted=extracted.get("source_configs", {}),
        raw=raw_derived.get("source_configs", {}),
        example=example.get("source_configs", {}),
        merge_report=merge_report,
    )
    final_core["ingestion_configs"] = _merge_section(
        section_name="ingestion_configs",
        extracted=extracted.get("ingestion_configs", {}),
        raw=raw_derived.get("ingestion_configs", {}),
        example=example.get("ingestion_configs", {}),
        merge_report=merge_report,
    )

    _apply_ui_inputs_if_needed(final_core, ui_inputs, merge_report, architect_notes)

    supported_dq_rules = _supported_dq_rules(raw_pipeline_json, raw_derived)
    final_core["dq_rules"] = _merge_dq_rules(
        extracted_rules=extracted.get("dq_rules", []),
        raw_rules=raw_derived.get("dq_rules", []),
        example_rules=example.get("dq_rules", []),
        supported_rules=supported_dq_rules,
        merge_report=merge_report,
    )

    final_core["flow"] = _merge_flow(
        extracted_flow=extracted.get("flow", {}),
        raw_flow=raw_derived.get("flow", {}),
        example_flow=example.get("flow", {}),
        merge_report=merge_report,
    )

    destination = final_core["ingestion_configs"].get("destination")
    if _looks_like_notification(destination):
        corrected = raw_derived.get("ingestion_configs", {}).get("destination")
        merge_report["fields_corrected"].append(
            {
                "field": "ingestion_configs.destination",
                "reason": "Removed notification-style destination; only real sinks are allowed.",
                "old_value": destination,
                "new_value": corrected,
            }
        )
        final_core["ingestion_configs"]["destination"] = corrected
        architect_notes.append("Removed a notification-style destination and kept the actual sink target.")

    final_core["missing_fields_analysis"] = _build_missing_fields_analysis(final_core, raw_pipeline_json)
    final_config = _compose_final_document(
        example_config=example_config,
        final_core=final_core,
        raw_pipeline_json=raw_pipeline_json,
        ui_inputs=ui_inputs,
    )

    if merge_report["fields_corrected"]:
        architect_notes.append("Corrected extracted values where raw pipeline evidence contradicted them.")
    if merge_report["fields_from_example"]:
        architect_notes.append("Used example config only for missing-field enrichment and structure alignment.")
    if not merge_report["conflicts_resolved"]:
        architect_notes.append("No unresolved extracted-vs-example conflicts remained after raw validation.")

    validation_report = _build_validation_report(
        final_config=final_core,
        raw_derived=raw_derived,
        supported_dq_rules=supported_dq_rules,
    )
    architect_notes.append(f"Confidence level: {validation_report['accuracy_score']}.")

    if use_llm:
        llm_review = _run_llm_review(final_config, raw_pipeline_json)
        if llm_review:
            summary = llm_review.get("summary")
            if isinstance(summary, str) and summary.strip():
                architect_notes.append(f"LLM review: {summary.strip()}")
            field_reasoning = llm_review.get("field_reasoning")
            if isinstance(field_reasoning, dict):
                for field, reason in field_reasoning.items():
                    if isinstance(reason, str) and reason.strip():
                        architect_notes.append(f"LLM {field}: {reason.strip()}")

    return {
        "final_config": final_config,
        "final_core": final_core,
        "merge_report": merge_report,
        "validation_report": validation_report,
        "architect_notes": architect_notes,
    }


def _normalize_config(config: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(config, dict):
        return {
            "source_configs": {},
            "ingestion_configs": {},
            "dq_rules": [],
            "flow": {},
            "missing_fields_analysis": [],
        }

    source_configs = config.get("source_configs")
    if not isinstance(source_configs, dict):
        source_configs = config.get("source_config", {}) if isinstance(config.get("source_config"), dict) else {}

    ingestion_configs = config.get("ingestion_configs")
    if not isinstance(ingestion_configs, dict):
        ingestion_configs = config.get("ingestion_config", {}) if isinstance(config.get("ingestion_config"), dict) else {}

    flow = config.get("flow", {})
    if isinstance(flow, str):
        flow = {"text": flow}
    if not isinstance(flow, dict):
        flow = {}

    dq_rules = config.get("dq_rules", [])
    if not isinstance(dq_rules, list):
        dq_rules = []

    missing = config.get("missing_fields_analysis", [])
    if not isinstance(missing, list):
        missing = []

    return {
        "source_configs": deepcopy(source_configs),
        "ingestion_configs": deepcopy(ingestion_configs),
        "dq_rules": list(dq_rules),
        "flow": deepcopy(flow),
        "missing_fields_analysis": deepcopy(missing),
    }


def _derive_from_raw(
    raw_pipeline_json: Dict[str, Any],
    extracted: Dict[str, Any],
    ui_inputs: Dict[str, Any],
) -> Dict[str, Any]:
    if not isinstance(raw_pipeline_json, dict) or not raw_pipeline_json:
        return _normalize_config({})

    metadata: Dict[str, Any] = {}
    platform = ui_inputs.get("platform") or extracted.get("platform")
    if platform:
        metadata["platform"] = platform
    pipeline_name = extracted.get("pipeline_name") or raw_pipeline_json.get("name")
    if pipeline_name:
        metadata["name"] = pipeline_name

    payload = AnalysisPayload(metadata=metadata, config={}, raw_json=raw_pipeline_json)
    reports = analyze_data_pipelines(payload)
    if reports:
        reformatted = reports[0].get("reformatted", {})
        return _normalize_config(reformatted if isinstance(reformatted, dict) else {})

    fallback = _normalize_config(extracted)
    fallback["dq_rules"] = list(_supported_dq_rules(raw_pipeline_json, {}))
    if not fallback["flow"]:
        fallback["flow"] = {"text": None, "graph": {"nodes": [], "edges": []}}
    return fallback


def _merge_section(
    *,
    section_name: str,
    extracted: Dict[str, Any],
    raw: Dict[str, Any],
    example: Dict[str, Any],
    merge_report: Dict[str, List[Any]],
) -> Dict[str, Any]:
    result = deepcopy(extracted) if isinstance(extracted, dict) else {}
    raw = raw if isinstance(raw, dict) else {}
    example = example if isinstance(example, dict) else {}

    for path, raw_value in _flatten(raw):
        if _is_missing(raw_value):
            continue
        extracted_value = _get_nested(result, path)
        if _is_missing(extracted_value):
            _set_nested(result, path, raw_value)
            merge_report["fields_corrected"].append(
                {"field": f"{section_name}.{path}", "reason": "Filled from raw pipeline evidence.", "new_value": raw_value}
            )
            continue
        if extracted_value != raw_value:
            _set_nested(result, path, raw_value)
            merge_report["fields_corrected"].append(
                {
                    "field": f"{section_name}.{path}",
                    "reason": "Extracted value disagreed with raw pipeline evidence.",
                    "old_value": extracted_value,
                    "new_value": raw_value,
                }
            )
            merge_report["conflicts_resolved"].append(
                {
                    "field": f"{section_name}.{path}",
                    "winner": "raw_pipeline_json",
                    "reason": "Ground truth overrides conflicting extracted/example values.",
                }
            )

    for path, value in _flatten(result):
        if not _is_missing(value):
            merge_report["fields_from_extracted"].append(f"{section_name}.{path}")

    for path, example_value in _flatten(example):
        if _is_missing(example_value):
            continue
        current = _get_nested(result, path)
        if _is_missing(current):
            _set_nested(result, path, example_value)
            merge_report["fields_from_example"].append(f"{section_name}.{path}")
        elif current != example_value and _get_nested(raw, path) != current:
            merge_report["conflicts_resolved"].append(
                {
                    "field": f"{section_name}.{path}",
                    "winner": "extracted_config",
                    "reason": "Validated extracted value was retained over example enrichment.",
                }
            )

    return result


def _merge_dq_rules(
    *,
    extracted_rules: List[Any],
    raw_rules: List[Any],
    example_rules: List[Any],
    supported_rules: Set[str],
    merge_report: Dict[str, List[Any]],
) -> List[str]:
    final: List[str] = []

    for rule in raw_rules:
        normalized = str(rule)
        if normalized in supported_rules and normalized not in final:
            final.append(normalized)
            merge_report["fields_corrected"].append(
                {"field": f"dq_rules.{normalized}", "reason": "Validated directly from raw pipeline logic."}
            )

    if not final:
        for rule in extracted_rules:
            normalized = str(rule)
            if normalized in supported_rules and normalized not in final:
                final.append(normalized)
                merge_report["fields_from_extracted"].append(f"dq_rules.{normalized}")

    for rule in example_rules:
        normalized = str(rule)
        if normalized in supported_rules and normalized not in final:
            final.append(normalized)
            merge_report["fields_from_example"].append(f"dq_rules.{normalized}")
        elif normalized not in supported_rules:
            merge_report["conflicts_resolved"].append(
                {
                    "field": f"dq_rules.{normalized}",
                    "winner": "ignored",
                    "reason": "Example DQ rule was not supported by raw pipeline logic.",
                }
            )

    return final


def _merge_flow(
    *,
    extracted_flow: Dict[str, Any],
    raw_flow: Dict[str, Any],
    example_flow: Dict[str, Any],
    merge_report: Dict[str, List[Any]],
) -> Dict[str, Any]:
    if isinstance(raw_flow, dict) and raw_flow.get("text"):
        merge_report["fields_corrected"].append({"field": "flow.text", "reason": "Execution order validated from raw activities."})
        return _sanitize_flow(deepcopy(raw_flow))
    if isinstance(extracted_flow, dict) and extracted_flow.get("text"):
        merge_report["fields_from_extracted"].append("flow.text")
        return _sanitize_flow(deepcopy(extracted_flow))
    if isinstance(example_flow, dict) and example_flow.get("text"):
        merge_report["fields_from_example"].append("flow.text")
        return _sanitize_flow(deepcopy(example_flow))
    return {"text": None, "graph": {"nodes": [], "edges": []}}


def _apply_ui_inputs_if_needed(
    final_config: Dict[str, Any],
    ui_inputs: Dict[str, Any],
    merge_report: Dict[str, List[Any]],
    architect_notes: List[str],
) -> None:
    ingestion_type = ui_inputs.get("ingestion_type")
    if ingestion_type and _is_missing(final_config["ingestion_configs"].get("mode")):
        final_config["ingestion_configs"]["mode"] = ingestion_type
        merge_report["fields_from_example"].append("ui_inputs.ingestion_type")
        architect_notes.append("Used UI ingestion_type only because ingestion mode was missing.")

    dq_preference = ui_inputs.get("dq_preference")
    if dq_preference and not final_config["dq_rules"]:
        architect_notes.append("DQ preference was noted but not forced because only raw-supported DQ rules are allowed.")


def _supported_dq_rules(raw_pipeline_json: Dict[str, Any], raw_derived: Dict[str, Any]) -> Set[str]:
    supported: Set[str] = set()
    text = str(raw_pipeline_json).lower()

    for rule in raw_derived.get("dq_rules", []) if isinstance(raw_derived, dict) else []:
        supported.add(str(rule))

    if "count(*)" in text or "rowcount" in text:
        supported.add("row_count_check")
    if "is null" in text:
        supported.add("null_check")
    if "duplicate" in text or "distinct" in text:
        supported.add("uniqueness_check")
    if "schema" in text:
        supported.add("schema_check")
    if "between" in text or "range" in text:
        supported.add("range_check")
    return supported


def _build_missing_fields_analysis(final_config: Dict[str, Any], raw_pipeline_json: Dict[str, Any]) -> List[Dict[str, str]]:
    missing: List[Dict[str, str]] = []
    source = final_config.get("source_configs", {})
    ingestion = final_config.get("ingestion_configs", {})
    flow = final_config.get("flow", {})

    for field in ("source_type", "service_name", "authentication_type"):
        if _is_missing(source.get(field)):
            missing.append({"field": f"source_configs.{field}", "reason": _missing_reason(field, raw_pipeline_json)})

    connection_details = source.get("connection_details", {}) if isinstance(source.get("connection_details"), dict) else {}
    for field in ("workspaceId", "artifactId", "schema", "table", "endpoint", "path"):
        if _is_missing(connection_details.get(field)):
            missing.append({"field": f"source_configs.connection_details.{field}", "reason": _missing_reason(field, raw_pipeline_json)})

    for field in ("trigger_type", "frequency", "data_format", "destination"):
        if _is_missing(ingestion.get(field)):
            missing.append({"field": f"ingestion_configs.{field}", "reason": _missing_reason(field, raw_pipeline_json)})

    if not final_config.get("dq_rules"):
        missing.append({"field": "dq_rules", "reason": "Not present in pipeline JSON or not derivable from validation activities."})
    if _is_missing(flow.get("text")):
        missing.append({"field": "flow.text", "reason": "Not derivable from activities."})
    return missing


def _build_validation_report(
    *,
    final_config: Dict[str, Any],
    raw_derived: Dict[str, Any],
    supported_dq_rules: Set[str],
) -> Dict[str, Any]:
    issues: List[str] = []
    review_fields: List[str] = []

    source_score = 1.0 if final_config.get("source_configs", {}).get("service_name") else 0.6

    final_destination = final_config.get("ingestion_configs", {}).get("destination")
    raw_destination = raw_derived.get("ingestion_configs", {}).get("destination")
    destination_score = 1.0
    if _looks_like_notification(final_destination):
        destination_score = 0.0
        issues.append("Destination still looks like a notification target, which is invalid.")
        review_fields.append("ingestion_configs.destination")
    elif raw_destination and final_destination != raw_destination:
        destination_score = 0.5
        issues.append("Final destination differs from raw-derived sink.")
        review_fields.append("ingestion_configs.destination")

    final_dq = set(final_config.get("dq_rules", []))
    if final_dq:
        unsupported = sorted(final_dq - supported_dq_rules)
        if unsupported:
            dq_score = 0.4
            issues.append(f"Unsupported DQ rules remained: {', '.join(unsupported)}.")
            review_fields.append("dq_rules")
        else:
            dq_score = 1.0
    else:
        dq_score = 0.9

    raw_flow_text = raw_derived.get("flow", {}).get("text")
    final_flow_text = final_config.get("flow", {}).get("text")
    if raw_flow_text and final_flow_text == raw_flow_text:
        flow_score = 1.0
    elif final_flow_text:
        flow_score = 0.7
        if raw_flow_text:
            issues.append("Flow text could not be fully aligned with the raw activity order.")
            review_fields.append("flow.text")
    else:
        flow_score = 0.4
        issues.append("Flow is missing.")
        review_fields.append("flow.text")

    accuracy = (source_score + destination_score + dq_score + flow_score) / 4

    if final_config.get("missing_fields_analysis"):
        review_fields.extend(item["field"] for item in final_config["missing_fields_analysis"] if isinstance(item, dict) and item.get("field"))

    return {
        "accuracy_score": f"{accuracy:.2f}",
        "issues_found": list(dict.fromkeys(issues)),
        "fields_needing_review": list(dict.fromkeys(review_fields)),
    }


def _missing_reason(field: str, raw_pipeline_json: Dict[str, Any]) -> str:
    text = str(raw_pipeline_json).lower()
    if field in {"workspaceId", "artifactId", "schema", "table", "endpoint", "path"}:
        return "Requires external metadata." if field == "artifactId" else "Not present in pipeline JSON."
    if field in {"trigger_type", "frequency", "data_format", "destination", "source_type", "service_name", "authentication_type"}:
        if field in {"destination", "source_type", "service_name"} and "activities" not in text:
            return "Not derivable from activities."
        return "Not present in pipeline JSON."
    return "Not derivable from activities."


def _flatten(value: Dict[str, Any], prefix: str = "") -> List[Tuple[str, Any]]:
    items: List[Tuple[str, Any]] = []
    for key, item in value.items():
        path = f"{prefix}.{key}" if prefix else key
        if isinstance(item, dict):
            items.extend(_flatten(item, path))
        else:
            items.append((path, item))
    return items


def _get_nested(data: Dict[str, Any], path: str) -> Any:
    current: Any = data
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def _set_nested(data: Dict[str, Any], path: str, value: Any) -> None:
    current = data
    parts = path.split(".")
    for part in parts[:-1]:
        if part not in current or not isinstance(current[part], dict):
            current[part] = {}
        current = current[part]
    current[parts[-1]] = value


def _is_missing(value: Any) -> bool:
    return value in (None, "", [], {})


def _looks_like_notification(value: Any) -> bool:
    text = str(value or "").lower()
    return any(marker in text for marker in NOTIFICATION_MARKERS)


def _sanitize_flow(flow: Dict[str, Any]) -> Dict[str, Any]:
    text = flow.get("text")
    if not isinstance(text, str) or not text.strip():
        return flow

    suffix = ""
    if " with validation gates" in text:
        text, suffix = text.split(" with validation gates", 1)
        suffix = " with validation gates" + suffix

    parts = [part.strip() for part in text.split("->")]
    parts = [part for part in parts if part and not _looks_like_notification(part)]
    flow["text"] = " -> ".join(parts) + suffix if parts else None
    return flow


def _run_llm_review(final_config: Dict[str, Any], raw_pipeline_json: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        report = {
            "type": "DataPipeline",
            "pipeline_name": raw_pipeline_json.get("name", "Final Config Review"),
            "platform": final_config.get("source_configs", {}).get("service_name") or "unknown",
            "reformatted": final_config,
            "original": raw_pipeline_json,
        }
        result = llm_infer_data_pipeline_reasoning(report)
        return result if isinstance(result, dict) else None
    except Exception:
        return None


def _compose_final_document(
    *,
    example_config: Dict[str, Any],
    final_core: Dict[str, Any],
    raw_pipeline_json: Dict[str, Any],
    ui_inputs: Dict[str, Any],
) -> Dict[str, Any]:
    if isinstance(example_config, dict) and example_config:
        doc = deepcopy(example_config)
    else:
        return deepcopy(final_core)

    doc["source_configs"] = deepcopy(final_core.get("source_configs", {}))
    doc["ingestion_configs"] = deepcopy(final_core.get("ingestion_configs", {}))
    doc["dq_rules"] = deepcopy(final_core.get("dq_rules", []))
    doc["flow"] = deepcopy(final_core.get("flow", {}))
    doc["missing_fields_analysis"] = deepcopy(final_core.get("missing_fields_analysis", []))

    pipeline_name = _first_non_empty(
        raw_pipeline_json.get("name"),
        doc.get("pipeline_name"),
        ui_inputs.get("pipeline_name"),
    )
    if pipeline_name:
        doc["pipeline_name"] = pipeline_name

    source = final_core.get("source_configs", {})
    ingestion = final_core.get("ingestion_configs", {})
    dq_rules = final_core.get("dq_rules", [])
    flow = final_core.get("flow", {})
    missing = final_core.get("missing_fields_analysis", [])

    overview = doc.setdefault("ingestion_overview", {}) if isinstance(doc.get("ingestion_overview", {}), dict) else {}
    if isinstance(overview, dict):
        if _is_missing(overview.get("orchestrator")):
            overview["orchestrator"] = _platform_label(ui_inputs.get("platform"), raw_pipeline_json)
        triggers = overview.setdefault("triggers", {}) if isinstance(overview.get("triggers", {}), dict) else {}
        if isinstance(triggers, dict):
            if ingestion.get("trigger_type") and _is_missing(triggers.get("batch")):
                triggers["batch"] = ingestion.get("trigger_type")
            if ingestion.get("frequency") and _is_missing(triggers.get("streaming")):
                triggers["streaming"] = ingestion.get("frequency")

    source_config = doc.setdefault("source_configuration", {}) if isinstance(doc.get("source_configuration", {}), dict) else {}
    if isinstance(source_config, dict):
        overlay_source = {
            "source_type": source.get("source_type"),
            "service_name": source.get("service_name"),
            "connection_details": source.get("connection_details", {}),
            "authentication_type": source.get("authentication_type"),
        }
        source_config.setdefault("pipeline_extracted_source", _compact_dict(overlay_source))

    adf_pipeline = doc.setdefault("adf_ingestion_pipeline", {}) if isinstance(doc.get("adf_ingestion_pipeline", {}), dict) else {}
    if isinstance(adf_pipeline, dict):
        adf_pipeline.setdefault("pipeline_extracted_ingestion", _compact_dict(ingestion))

    sink = doc.setdefault("sink_configuration", {}) if isinstance(doc.get("sink_configuration", {}), dict) else {}
    if isinstance(sink, dict):
        if ingestion.get("destination") and _is_missing(sink.get("storage")):
            sink["storage"] = ingestion.get("destination")
        fmt = sink.setdefault("format", {}) if isinstance(sink.get("format", {}), dict) else {}
        if isinstance(fmt, dict) and ingestion.get("data_format") and _is_missing(fmt.get("type")):
            fmt["type"] = ingestion.get("data_format")

    governance = doc.setdefault("governance_lineage", {}) if isinstance(doc.get("governance_lineage", {}), dict) else {}
    if isinstance(governance, dict):
        if dq_rules and _is_missing(governance.get("data_quality_rules")):
            governance["data_quality_rules"] = dq_rules
        if flow.get("text") and _is_missing(governance.get("lineage_tracking")):
            governance["lineage_tracking"] = flow.get("text")

    if flow.get("text") and _is_missing(doc.get("final_definition")):
        doc["final_definition"] = flow.get("text")

    doc["pipeline_extracted_overlay"] = {
        "source_configs": source,
        "ingestion_configs": ingestion,
        "dq_rules": dq_rules,
        "flow": flow,
        "missing_fields_analysis": missing,
    }
    return doc


def _platform_label(platform_hint: Optional[str], raw_pipeline_json: Dict[str, Any]) -> Optional[str]:
    text = str(raw_pipeline_json).lower()
    hint = str(platform_hint or "").lower()
    if hint == "adf" or "datafactory" in text:
        return "Azure Data Factory"
    if hint == "fabric" or "fabric" in text:
        return "Microsoft Fabric"
    if hint == "glue" or "glue" in text:
        return "AWS Glue"
    return platform_hint or None


def _compact_dict(value: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in value.items() if not _is_missing(v)}


def _first_non_empty(*values: Any) -> Optional[str]:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None
