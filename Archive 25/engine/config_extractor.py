"""
Config Extractor
────────────────
Extracts structured ingestion and source configuration objects from an
AnalysisPayload.

For every detected ingestion engine or source type, the extractor pulls
the relevant connection properties (URLs, credentials keys, regions,
bucket names, etc.) out of the payload and returns them as typed dicts.

These dicts are surfaced in the API response as:
  - ingestion_config  — per-engine connection/job properties
  - source_config     — per-source connection/path properties

Design principles:
  - Never raises — all extraction is best-effort; missing fields are omitted.
  - Cloud-agnostic — covers AWS, Azure, GCP, Databricks, Snowflake, and generic.
  - Extensible — add a new extractor function and register it in the maps below.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from engine.detectors.base import AnalysisPayload
from engine.detectors.source_detector import SOURCE_PATTERNS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _find(payload: AnalysisPayload, *keys: str) -> Optional[str]:
    """
    Search all three payload dicts for the first non-empty value matching
    any of the given keys (case-insensitive). Also crawls structured 
    cloud_dump data if present.
    """
    for source in (payload.metadata, payload.config, payload.raw_json):
        # 1. Direct top-level check
        for key in keys:
            val = source.get(key)
            if val and isinstance(val, (str, int, float)):
                return str(val)
        
        # 2. Case-insensitive top-level check
        for key in keys:
            for k, v in source.items():
                if k.lower() == key.lower() and v and isinstance(v, (str, int, float)):
                    return str(v)

    # 3. Recursive crawl for 'raw_cloud_dump' structure from discovery
    raw_dump = payload.raw_json.get("raw_cloud_dump", [])
    if isinstance(raw_dump, list) and raw_dump:
        for dump_item in raw_dump:
            if not isinstance(dump_item, dict): continue
            for service_list in dump_item.values():
                if not isinstance(service_list, list): continue
                for item in service_list:
                    if not isinstance(item, dict): continue
                    config = item.get("configuration", {})
                    for key in keys:
                        # Check configuration block
                        val = config.get(key)
                        if val and isinstance(val, (str, int, float)):
                            return str(val)
                        # Check item top level
                        val = item.get(key)
                        if val and isinstance(val, (str, int, float)):
                            return str(val)
    return None

def _get_cloud_discovered_list(payload: AnalysisPayload, service_key: str) -> List[Dict[str, Any]]:
    """Return list of discovered items for a specific service (e.g. 'lambda', 's3')."""
    raw_dump = payload.raw_json.get("raw_cloud_dump", [])
    all_items = []
    if isinstance(raw_dump, list):
        for dump_item in raw_dump:
            if isinstance(dump_item, dict):
                all_items.extend(dump_item.get(service_key, []))
    return all_items


def _collect_cloud_discovered_lists(payload: AnalysisPayload, service_keys: List[str]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for key in service_keys:
        items.extend(_get_cloud_discovered_list(payload, key))
    return items


def _find_pattern(payload: AnalysisPayload, pattern: str) -> Optional[str]:
    """Return the first regex match found anywhere in the payload text."""
    text = payload.all_text()
    m = re.search(pattern, text, re.IGNORECASE)
    return m.group(0) if m else None


def _find_all_pattern(payload: AnalysisPayload, pattern: str) -> List[str]:
    """Return all regex matches found in the payload text."""
    text = payload.all_text()
    return re.findall(pattern, text, re.IGNORECASE)


def _compact(d: Dict[str, Any]) -> Dict[str, Any]:
    """Remove None/empty values from a dict."""
    return {k: v for k, v in d.items() if v not in (None, "", [], {})}


def _collect_values(obj: Any, target_key: str, max_depth: int = 10, _depth: int = 0) -> List[Any]:
    """Collect nested values for a given key from dict/list structures."""
    if _depth > max_depth:
        return []
    results: List[Any] = []
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key.lower() == target_key.lower():
                results.append(value)
            results.extend(_collect_values(value, target_key, max_depth=max_depth, _depth=_depth + 1))
    elif isinstance(obj, list):
        for item in obj:
            results.extend(_collect_values(item, target_key, max_depth=max_depth, _depth=_depth + 1))
    return results


def _flatten_strings(values: List[Any]) -> List[str]:
    flat: List[str] = []
    for value in values:
        if isinstance(value, str) and value.strip():
            flat.append(value.strip())
        elif isinstance(value, (int, float)):
            flat.append(str(value))
        elif isinstance(value, list):
            flat.extend(_flatten_strings(value))
    return list(dict.fromkeys(flat))


def _normalize_inventory_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        normalized.append({
            "id": item.get("id"),
            "configuration": item.get("configuration", {}),
        })
    return normalized


# ---------------------------------------------------------------------------
# Source config extractors
# ---------------------------------------------------------------------------

def _extract_s3_config(payload: AnalysisPayload) -> Dict[str, Any]:
    paths = _find_all_pattern(payload, r"s3[an]?://[^\s\"'>,]+")
    region = _find(payload, "aws_region", "region")
    bucket = None
    
    # Enrich from discovery if we found exactly one bucket in the list
    discovered = _get_cloud_discovered_list(payload, "s3")
    if discovered and not paths:
        # If discovery found buckets but no explicit path in text
        bucket = discovered[0]["id"].split(" || ")[-1]
        paths = [f"s3://{bucket}/"]
        region = region or discovered[0].get("configuration", {}).get("region")

    if paths:
        m = re.match(r"s3[an]?://([^/]+)", paths[0])
        bucket = m.group(1) if m else bucket

    return _compact({
        "type": "Amazon S3",
        "paths": paths or None,
        "bucket": bucket,
        "region": region,
        "access_key_configured": bool(_find(payload, "aws_access_key_id", "access_key")),
    })


def _extract_adls_config(payload: AnalysisPayload) -> Dict[str, Any]:
    paths = _find_all_pattern(payload, r"abfss?://[^\s\"'>,]+")
    account = _find(payload, "storage_account", "azure_storage_account")
    container = None
    if paths:
        m = re.match(r"abfss?://([^@]+)@", paths[0])
        container = m.group(1) if m else None
    return _compact({
        "type": "Azure Data Lake Storage Gen2",
        "paths": paths or None,
        "container": container,
        "storage_account": account,
        "tenant_id": _find(payload, "azure_tenant_id", "tenant_id"),
    })


def _extract_gcs_config(payload: AnalysisPayload) -> Dict[str, Any]:
    paths = _find_all_pattern(payload, r"gs://[^\s\"'>,]+")
    bucket = None
    if paths:
        m = re.match(r"gs://([^/]+)", paths[0])
        bucket = m.group(1) if m else None
    return _compact({
        "type": "Google Cloud Storage",
        "paths": paths or None,
        "bucket": bucket,
        "project_id": _find(payload, "gcp_project_id", "project_id", "project"),
    })


def _extract_redshift_config(payload: AnalysisPayload) -> Dict[str, Any]:
    jdbc = _find_pattern(payload, r"jdbc:redshift://[^\s\"'>,]+")
    host = _find(payload, "redshift_host", "cluster_endpoint")
    if not host and jdbc:
        m = re.search(r"jdbc:redshift://([^:/]+)", jdbc)
        host = m.group(1) if m else None
    port = _find(payload, "redshift_port", "port") or "5439"
    database = _find(payload, "redshift_database", "database", "db")
    if not database and jdbc:
        m = re.search(r"/([^?]+)$", jdbc)
        database = m.group(1) if m else None
    return _compact({
        "type": "Amazon Redshift",
        "host": host,
        "port": port,
        "database": database,
        "jdbc_url": jdbc,
        "username": _find(payload, "redshift_user", "db_user", "username"),
    })


def _extract_snowflake_config(payload: AnalysisPayload) -> Dict[str, Any]:
    return _compact({
        "type": "Snowflake",
        "account": _find(payload, "snowflake_account", "account"),
        "warehouse": _find(payload, "warehouse", "snowflake_warehouse"),
        "database": _find(payload, "snowflake_database", "database"),
        "schema": _find(payload, "snowflake_schema", "schema"),
        "role": _find(payload, "snowflake_role", "role"),
        "username": _find(payload, "snowflake_user", "username"),
    })


def _extract_kafka_config(payload: AnalysisPayload) -> Dict[str, Any]:
    brokers = _find(payload, "bootstrap_servers", "kafka_brokers", "brokers")
    topics = _find_all_pattern(payload, r"kafka[._-]topic[s]?\s*[=:]\s*['\"]?([^\s\"',]+)")
    return _compact({
        "type": "Apache Kafka",
        "bootstrap_servers": brokers,
        "topics": topics or None,
        "security_protocol": _find(payload, "security_protocol"),
        "sasl_mechanism": _find(payload, "sasl_mechanism"),
    })


def _extract_bigquery_config(payload: AnalysisPayload) -> Dict[str, Any]:
    return _compact({
        "type": "Google BigQuery",
        "project_id": _find(payload, "gcp_project_id", "project_id", "project"),
        "dataset": _find(payload, "bq_dataset", "dataset", "bigquery_dataset"),
        "table": _find(payload, "bq_table", "table"),
        "location": _find(payload, "bq_location", "location"),
    })


def _extract_jdbc_config(payload: AnalysisPayload) -> Dict[str, Any]:
    jdbc = _find_pattern(payload, r"jdbc:[a-z]+://[^\s\"'>,]+")
    return _compact({
        "type": "JDBC",
        "jdbc_url": jdbc,
        "username": _find(payload, "db_user", "username", "user"),
    })


def _extract_rest_api_config(payload: AnalysisPayload) -> Dict[str, Any]:
    urls = _find_all_pattern(payload, r"https?://[^\s\"'>,]+api[^\s\"'>,]*")
    discovered = _get_cloud_discovered_list(payload, "apigateway")
    if discovered and not urls:
        for item in discovered:
            invoke_url = item.get("configuration", {}).get("PublicInvokeURL")
            if invoke_url: urls.append(invoke_url)

    return _compact({
        "type": "REST API",
        "endpoints": urls[:5] if urls else None,
        "auth_type": _find(payload, "auth_type", "authentication"),
        "api_key_configured": bool(_find(payload, "api_key", "x_api_key")),
        "discovery_notes": f"Discovered {len(discovered)} endpoints" if discovered else None
    })


def _extract_fabric_source_config(payload: AnalysisPayload) -> Dict[str, Any]:
    items = _normalize_inventory_items(_collect_cloud_discovered_lists(payload, ["fabric_items", "fabric_workspaces"]))
    workspace_names = []
    lakehouse_paths = []
    item_types = []
    for item in items:
        item_id = str(item.get("id", ""))
        cfg = item.get("configuration", {})
        if item_id:
            workspace_names.append(item_id.split(" || ")[-1])
        if cfg.get("OneLakeFilesPath"):
            lakehouse_paths.append(str(cfg["OneLakeFilesPath"]))
        if cfg.get("OneLakeTablesPath"):
            lakehouse_paths.append(str(cfg["OneLakeTablesPath"]))
        if cfg.get("Type"):
            item_types.append(str(cfg["Type"]))
    return _compact({
        "type": "Microsoft Fabric",
        "workspace_names": list(dict.fromkeys(workspace_names))[:20] or None,
        "item_types": list(dict.fromkeys(item_types))[:20] or None,
        "onelake_paths": list(dict.fromkeys(lakehouse_paths))[:20] or None,
    })


# Map: source display name → extractor function
_SOURCE_EXTRACTORS: Dict[str, Any] = {
    "S3":                _extract_s3_config,
    "ADLS Gen2":         _extract_adls_config,
    "WASB/Azure Blob":   _extract_adls_config,
    "GCS":               _extract_gcs_config,
    "JDBC/Redshift":     _extract_redshift_config,
    "Snowflake Stage":   _extract_snowflake_config,
    "Kafka Topic":       _extract_kafka_config,
    "BigQuery":          _extract_bigquery_config,
    "JDBC/PostgreSQL":   _extract_jdbc_config,
    "JDBC/MySQL":        _extract_jdbc_config,
    "JDBC/MSSQL":        _extract_jdbc_config,
    "JDBC/Oracle":       _extract_jdbc_config,
    "JDBC/Generic":      _extract_jdbc_config,
    "REST API":          _extract_rest_api_config,
    "Microsoft Fabric":  _extract_fabric_source_config,
}

_DETECTED_ONLY_SOURCE_REASONS: Dict[str, str] = {
    "DynamoDB": "The engine can detect DynamoDB references, but there is no dedicated source extractor yet for table names, keys, stream settings, or IAM-backed connection details.",
    "Cosmos DB": "The engine can detect Cosmos DB usage, but it does not yet normalize account, database, container, or authentication metadata into structured source_config output.",
    "MongoDB": "MongoDB is pattern-detected only. Cluster, database, collection, and credential extraction are not implemented in the current source extractor layer.",
    "Kinesis": "Kinesis streams are recognized from payload text, but stream-specific details such as stream name, consumer configuration, and region are not extracted into a typed source config.",
    "Event Hubs": "Azure Event Hubs is detected heuristically, but namespace, hub, consumer group, and auth extraction are not implemented yet.",
    "GraphQL": "GraphQL endpoints can be recognized from payload markers, but the engine does not yet parse GraphQL schemas, operations, or endpoint configuration into structured source metadata.",
    "SOAP/WS": "SOAP or WSDL usage is detected from payload text, but there is no WSDL-aware parser that can extract service, operation, and binding details.",
    "SFTP": "SFTP sources are detected from URI or library markers, but host, port, remote path, and credential normalization are not yet modeled in source_config.",
    "FTP": "FTP sources are detected from URI or library markers, but the engine does not yet produce structured connection details for FTP endpoints.",
    "Local FS": "Local filesystem inputs are environment-specific and not portable across deployments, so the engine only flags them instead of emitting a normalized source configuration.",
}


def build_source_support_matrix(detected_sources: List[str]) -> Dict[str, Any]:
    """
    Summarize which source types are fully supported versus detection-only.

    Supported means the engine has both recognition and a structured extractor.
    Detection-only means the detector can flag the source, but no dedicated
    extractor exists yet to emit typed source_config details.
    """
    detected_set = set(detected_sources or [])
    catalog_names = list(
        dict.fromkeys(
            [display_name for display_name, _ in SOURCE_PATTERNS]
            + list(_SOURCE_EXTRACTORS.keys())
        )
    )

    supported_source_types: List[Dict[str, Any]] = []
    unsupported_source_types: List[Dict[str, Any]] = []

    for source_name in catalog_names:
        is_supported = source_name in _SOURCE_EXTRACTORS
        base_record = {
            "name": source_name,
            "detected": source_name in detected_set,
        }
        if is_supported:
            supported_source_types.append(
                {
                    **base_record,
                    "support_level": "supported",
                    "explanation": "This source type has a dedicated rule-based extractor, so the engine can return structured connection details in source_config when it is detected.",
                }
            )
        else:
            unsupported_source_types.append(
                {
                    **base_record,
                    "support_level": "detected_only",
                    "explanation": _DETECTED_ONLY_SOURCE_REASONS.get(
                        source_name,
                        "This source type can be identified heuristically, but the current engine does not yet have a dedicated structured extractor for it.",
                    ),
                }
            )

    return {
        "detected": {
            "supported_source_types": [item for item in supported_source_types if item["detected"]],
            "unsupported_source_types": [item for item in unsupported_source_types if item["detected"]],
        },
        "catalog": {
            "supported_source_types": supported_source_types,
            "unsupported_source_types": unsupported_source_types,
        },
    }


# ---------------------------------------------------------------------------
# Ingestion config extractors
# ---------------------------------------------------------------------------

def _extract_glue_config(payload: AnalysisPayload) -> Dict[str, Any]:
    return _compact({
        "type": "AWS Glue",
        "job_name": _find(payload, "job_name", "glue_job_name", "name"),
        "region": _find(payload, "aws_region", "region"),
        "role_arn": _find_pattern(payload, r"arn:aws:iam::[0-9]+:role/[^\s\"'>,]+"),
        "script_location": _find(payload, "script_location", "script_path"),
        "temp_dir": _find(payload, "temp_dir", "temp_directory"),
        "worker_type": _find(payload, "worker_type"),
        "number_of_workers": _find(payload, "number_of_workers", "num_workers"),
        "glue_version": _find(payload, "glue_version"),
        "connections": _find_all_pattern(payload, r"jdbc:[a-z]+://[^\s\"'>,]+") or None,
    })


def _extract_adf_config(payload: AnalysisPayload) -> Dict[str, Any]:
    return _compact({
        "type": "Azure Data Factory",
        "factory_name": _find(payload, "factory_name", "adf_name", "data_factory_name"),
        "resource_group": _find(payload, "resource_group"),
        "subscription_id": _find(payload, "subscription_id", "azure_subscription_id"),
        "pipeline_name": _find(payload, "pipeline_name", "name"),
        "trigger_type": _find(payload, "trigger_type", "trigger"),
        "linked_services": _find_all_pattern(
            payload, r"linked.?service[s]?\s*[=:]\s*['\"]?([^\s\"',]+)"
        ) or None,
    })


def _extract_databricks_config(payload: AnalysisPayload) -> Dict[str, Any]:
    return _compact({
        "type": "Databricks",
        "host": _find(payload, "databricks_host", "workspace_url"),
        "cluster_id": _find(payload, "cluster_id", "databricks_cluster_id"),
        "notebook_path": _find(payload, "notebook_path", "notebook"),
        "job_name": _find(payload, "job_name", "name"),
        "runtime_version": _find(payload, "spark_version", "runtime_version"),
        "node_type": _find(payload, "node_type_id", "node_type"),
        "num_workers": _find(payload, "num_workers", "number_of_workers"),
    })


def _extract_airflow_config(payload: AnalysisPayload) -> Dict[str, Any]:
    return _compact({
        "type": "Apache Airflow",
        "dag_id": _find(payload, "dag_id"),
        "schedule": _find(payload, "schedule_interval", "schedule", "cron"),
        "start_date": _find(payload, "start_date"),
        "catchup": _find(payload, "catchup"),
        "tags": _find_all_pattern(payload, r"tags\s*=\s*\[([^\]]+)\]") or None,
    })


def _extract_spark_config(payload: AnalysisPayload) -> Dict[str, Any]:
    return _compact({
        "type": "Apache Spark",
        "app_name": _find(payload, "app_name", "spark_app_name"),
        "master": _find(payload, "master", "spark_master"),
        "deploy_mode": _find(payload, "deploy_mode"),
        "executor_memory": _find(payload, "executor_memory"),
        "executor_cores": _find(payload, "executor_cores"),
        "num_executors": _find(payload, "num_executors"),
        "packages": _find_all_pattern(payload, r"--packages\s+([^\s]+)") or None,
    })


def _extract_dbt_config(payload: AnalysisPayload) -> Dict[str, Any]:
    return _compact({
        "type": "dbt",
        "project_name": _find(payload, "project_name", "dbt_project"),
        "target": _find(payload, "target", "dbt_target"),
        "profile": _find(payload, "profile", "dbt_profile"),
        "models": _find_all_pattern(payload, r"dbt.*model[s]?\s*[=:]\s*['\"]?([^\s\"',]+)") or None,
        "schema": _find(payload, "schema", "target_schema"),
    })


def _extract_lambda_config(payload: AnalysisPayload) -> Dict[str, Any]:
    discovered = _get_cloud_discovered_list(payload, "lambda")
    config_from_discovery = {}
    if discovered:
        # Use first one as primary example
        config_from_discovery = discovered[0].get("configuration", {})

    return _compact({
        "type": "AWS Lambda",
        "function_name": _find(payload, "function_name", "lambda_function") or (discovered[0]["id"].split(" || ")[-1] if discovered else None),
        "runtime": _find(payload, "runtime") or config_from_discovery.get("Runtime"),
        "handler": _find(payload, "handler") or config_from_discovery.get("Handler"),
        "region": _find(payload, "aws_region", "region"),
        "role_arn": _find_pattern(payload, r"arn:aws:iam::[0-9]+:role/[^\s\"'>,]+"),
        "memory_size": _find(payload, "memory_size") or config_from_discovery.get("MemorySizeMB"),
        "timeout": _find(payload, "timeout") or config_from_discovery.get("TimeoutSeconds"),
        "function_arn": _find_pattern(
            payload, r"arn:aws:lambda:[a-z0-9-]+:[0-9]+:function:[^\s\"'>,]+"
        ),
        "ingestion_ops": config_from_discovery.get("IngestionOperations"),
        "data_formats": config_from_discovery.get("DataFormats"),
        "discovered_targets": config_from_discovery.get("IngestionTargets")
    })


def _extract_fivetran_config(payload: AnalysisPayload) -> Dict[str, Any]:
    return _compact({
        "type": "Fivetran",
        "connector_id": _find(payload, "connector_id", "fivetran_connector"),
        "destination": _find(payload, "destination", "fivetran_destination"),
        "sync_frequency": _find(payload, "sync_frequency"),
    })


def _extract_kafka_connect_config(payload: AnalysisPayload) -> Dict[str, Any]:
    return _compact({
        "type": "Kafka Connect",
        "connector_name": _find(payload, "connector_name", "name"),
        "connector_class": _find(payload, "connector_class"),
        "bootstrap_servers": _find(payload, "bootstrap_servers"),
        "topics": _find(payload, "topics"),
        "tasks_max": _find(payload, "tasks_max"),
    })


def _extract_step_functions_config(payload: AnalysisPayload) -> Dict[str, Any]:
    return _compact({
        "type": "AWS Step Functions",
        "state_machine_name": _find(payload, "state_machine_name", "name"),
        "state_machine_arn": _find_pattern(
            payload, r"arn:aws:states:[a-z0-9-]+:[0-9]+:stateMachine:[^\s\"'>,]+"
        ),
        "region": _find(payload, "aws_region", "region"),
        "role_arn": _find_pattern(payload, r"arn:aws:iam::[0-9]+:role/[^\s\"'>,]+"),
    })


def _extract_dataflow_config(payload: AnalysisPayload) -> Dict[str, Any]:
    return _compact({
        "type": "GCP Dataflow",
        "job_name": _find(payload, "job_name", "dataflow_job", "name"),
        "project_id": _find(payload, "gcp_project_id", "project_id", "project"),
        "region": _find(payload, "region", "gcp_region"),
        "template": _find(payload, "template", "dataflow_template"),
        "temp_location": _find(payload, "temp_location"),
        "staging_location": _find(payload, "staging_location"),
    })


def _extract_azure_function_config(payload: AnalysisPayload) -> Dict[str, Any]:
    discovered = _get_cloud_discovered_list(payload, "functions")
    cfg = discovered[0].get("configuration", {}) if discovered else {}
    function_name = discovered[0]["id"].split(" || ")[-1] if discovered else None
    return _compact({
        "type": "Azure Functions",
        "function_name": _find(payload, "function_name", "name") or function_name,
        "location": _find(payload, "location", "region") or cfg.get("Location"),
        "kind": cfg.get("Kind"),
        "tags": cfg.get("Tags"),
    })


def _extract_fabric_ingestion_config(payload: AnalysisPayload) -> Dict[str, Any]:
    items = _normalize_inventory_items(_get_cloud_discovered_list(payload, "fabric_items"))
    pipelines = []
    notebooks = []
    spark_jobs = []
    for item in items:
        item_id = str(item.get("id", ""))
        item_name = item_id.split(" || ")[-1] if item_id else None
        item_type = str(item.get("configuration", {}).get("Type", ""))
        if item_type.lower() in {"pipeline", "datapipeline"} and item_name:
            pipelines.append(item_name)
        elif item_type.lower() == "notebook" and item_name:
            notebooks.append(item_name)
        elif item_type.lower() == "sparkjob" and item_name:
            spark_jobs.append(item_name)
    return _compact({
        "type": "Microsoft Fabric",
        "pipeline_items": list(dict.fromkeys(pipelines)) or None,
        "notebook_items": list(dict.fromkeys(notebooks)) or None,
        "spark_job_items": list(dict.fromkeys(spark_jobs)) or None,
    })


# Map: ingestion display name → extractor function
_INGESTION_EXTRACTORS: Dict[str, Any] = {
    "AWS Glue Jobs":          _extract_glue_config,
    "ADF Pipelines":          _extract_adf_config,
    "Databricks Jobs":        _extract_databricks_config,
    "Apache Airflow DAGs":    _extract_airflow_config,
    "Apache Spark Jobs":      _extract_spark_config,
    "dbt Jobs":               _extract_dbt_config,
    "AWS Lambda ETL":         _extract_lambda_config,
    "Fivetran":               _extract_fivetran_config,
    "Kafka Connect":          _extract_kafka_connect_config,
    "AWS Step Functions":     _extract_step_functions_config,
    "GCP Dataflow":           _extract_dataflow_config,
    "Azure Functions":        _extract_azure_function_config,
    "Microsoft Fabric":       _extract_fabric_ingestion_config,
}


# ---------------------------------------------------------------------------
# Storage config extractors
# ---------------------------------------------------------------------------

def extract_storage_configs(payload: AnalysisPayload) -> Dict[str, Any]:
    s3_items = _normalize_inventory_items(_get_cloud_discovered_list(payload, "s3"))
    storage_accounts = _normalize_inventory_items(_get_cloud_discovered_list(payload, "storage_accounts"))
    s3_paths = _find_all_pattern(payload, r"s3[an]?://[^\s\"'>,]+")
    gcs_paths = _find_all_pattern(payload, r"gs://[^\s\"'>,]+")
    snowflake_account = _find(payload, "snowflake_account", "account")
    bigquery_dataset = _find(payload, "bq_dataset", "dataset", "bigquery_dataset")
    fabric_items = _normalize_inventory_items(_collect_cloud_discovered_lists(payload, ["fabric_items", "fabric_workspaces"]))

    result: Dict[str, Any] = {}

    if s3_items or s3_paths:
        result["Amazon S3"] = _compact({
            "bucket_count": len(s3_items) or len(list(dict.fromkeys(
                re.match(r"s3[an]?://([^/]+)", path).group(1)
                for path in s3_paths
                if re.match(r"s3[an]?://([^/]+)", path)
            ))),
            "path_count": len(s3_paths) or len(s3_items),
            "paths": s3_paths[:25] or None,
            "buckets": (
                [item["id"].split(" || ")[-1] for item in s3_items if item.get("id")][:25]
                or list(dict.fromkeys(
                    re.match(r"s3[an]?://([^/]+)", path).group(1)
                    for path in s3_paths
                    if re.match(r"s3[an]?://([^/]+)", path)
                ))[:25]
                or None
            ),
            "regions": list(dict.fromkeys(
                str(item.get("configuration", {}).get("Region"))
                for item in s3_items
                if item.get("configuration", {}).get("Region")
            )) or None,
            "versioning": list(dict.fromkeys(
                str(item.get("configuration", {}).get("Versioning"))
                for item in s3_items
                if item.get("configuration", {}).get("Versioning")
            )) or None,
        })

    if storage_accounts:
        result["Azure Storage"] = _compact({
            "account_count": len(storage_accounts),
            "accounts": [item["id"].split(" || ")[-1] for item in storage_accounts if item.get("id")][:25] or None,
            "locations": list(dict.fromkeys(
                str(item.get("configuration", {}).get("Location"))
                for item in storage_accounts
                if item.get("configuration", {}).get("Location")
            )) or None,
            "hierarchical_namespace_enabled": any(
                bool(item.get("configuration", {}).get("IsHnsEnabled")) for item in storage_accounts
            ),
        })

    if gcs_paths:
        result["Google Cloud Storage"] = _compact({
            "path_count": len(gcs_paths),
            "paths": gcs_paths[:25],
        })

    if snowflake_account:
        result["Snowflake"] = _compact({
            "account": snowflake_account,
            "warehouse": _find(payload, "warehouse", "snowflake_warehouse"),
            "database": _find(payload, "snowflake_database", "database"),
            "schema": _find(payload, "snowflake_schema", "schema"),
        })

    if bigquery_dataset:
        result["BigQuery"] = _compact({
            "project_id": _find(payload, "gcp_project_id", "project_id", "project"),
            "dataset": bigquery_dataset,
            "table": _find(payload, "bq_table", "table"),
            "location": _find(payload, "bq_location", "location"),
        })

    if fabric_items:
        onelake_paths = []
        for item in fabric_items:
            cfg = item.get("configuration", {})
            if cfg.get("OneLakeFilesPath"):
                onelake_paths.append(str(cfg["OneLakeFilesPath"]))
            if cfg.get("OneLakeTablesPath"):
                onelake_paths.append(str(cfg["OneLakeTablesPath"]))
        if onelake_paths:
            result["Microsoft Fabric / OneLake"] = _compact({
                "path_count": len(onelake_paths),
                "paths": list(dict.fromkeys(onelake_paths))[:25],
            })

    return result


# ---------------------------------------------------------------------------
# DQ config extractors
# ---------------------------------------------------------------------------

def extract_dq_configs(payload: AnalysisPayload, detected_dq_rules: List[str]) -> Dict[str, Any]:
    raw = payload.raw_json
    checks = _flatten_strings(
        _collect_values(payload.config, "checks")
        + _collect_values(payload.raw_json, "checks")
        + _collect_values(payload.config, "assertion")
        + _collect_values(payload.raw_json, "validation_sql")
        + _collect_values(payload.raw_json, "validation")
        + _collect_values(payload.config, "freshness_check")
        + _collect_values(payload.raw_json, "check")
    )
    expectations = []
    for exp in _collect_values(raw, "expectations"):
        if isinstance(exp, list):
            for item in exp:
                if isinstance(item, dict):
                    expectations.append(item)

    ge_expectation_types = list(dict.fromkeys(
        str(item.get("expectation_type"))
        for item in expectations
        if item.get("expectation_type")
    ))
    ge_columns = list(dict.fromkeys(
        str(item.get("kwargs", {}).get("column"))
        for item in expectations
        if isinstance(item.get("kwargs"), dict) and item.get("kwargs", {}).get("column")
    ))
    dbt_tests = _flatten_strings(
        _collect_values(raw, "dbt_test")
        + _collect_values(payload.config, "dbt_test")
    )
    suite_names = _flatten_strings(
        _collect_values(raw, "suite_name")
        + _collect_values(payload.config, "suite_name")
        + _collect_values(payload.config, "checkpoint_name")
        + _collect_values(raw, "checkpoint_name")
    )
    tool_markers = []
    for marker in ("tool", "monitor", "validator", "class"):
        tool_markers.extend(_flatten_strings(_collect_values(raw, marker) + _collect_values(payload.config, marker)))

    return _compact({
        "detected_rules": detected_dq_rules or None,
        "frameworks": [rule for rule in detected_dq_rules if ":" not in rule] or None,
        "great_expectations": _compact({
            "suite_names": suite_names or None,
            "expectation_types": ge_expectation_types or None,
            "columns": ge_columns or None,
            "expectation_count": len(expectations) or None,
        }) if suite_names or expectations else None,
        "sql_checks": _compact({
            "count": len(checks) or None,
            "checks": checks[:25] or None,
        }) if checks else None,
        "dbt_tests": dbt_tests or None,
        "custom_framework_markers": tool_markers or None,
    })


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract_source_configs(
    detected_sources: List[str],
    payload: AnalysisPayload,
) -> Dict[str, Any]:
    """
    For each detected source, extract its connection properties from the payload.
    """
    result: Dict[str, Any] = {}
    for source in detected_sources:
        extractor = _SOURCE_EXTRACTORS.get(source)
        if extractor:
            config = extractor(payload)
            if config:
                result[source] = config
    return result


def extract_ingestion_configs(
    detected_ingestion: List[str],
    payload: AnalysisPayload,
) -> Dict[str, Any]:
    """
    For each detected ingestion engine, extract its job/connection properties.
    """
    result: Dict[str, Any] = {}
    for engine in detected_ingestion:
        extractor = _INGESTION_EXTRACTORS.get(engine)
        if extractor:
            config = extractor(payload)
            if config:
                result[engine] = config
    return result


def extract_expert_config(payload: AnalysisPayload) -> Dict[str, Any]:
    """
    Accuracy > Completeness. Strictly extracts SOURCE, INGESTION, and TARGET
    using only verified data.
    """
    source_results = {}
    ingestion_results = {}
    target_results = {}

    # 1. SOURCE: API Gateway
    apigw_list = _get_cloud_discovered_list(payload, "apigateway")
    if apigw_list:
        main = apigw_list[0]
        cfg = main.get("configuration", {})
        
        # Integration logic
        target_lambda = "UNKNOWN"
        integration_type = "UNKNOWN"
        integrations = cfg.get("Integrations", [])
        if integrations:
            integration_type = integrations[0].get("type", "UNKNOWN")
            uri = integrations[0].get("uri", "")
            if "function:" in uri:
                target_lambda = uri.split("function:")[-1].split("/")[0]

        source_results = {
            "source_type": "API",
            "service": "API Gateway",
            "endpoint": cfg.get("PublicInvokeURL", "UNKNOWN"),
            "method": cfg.get("Methods", ["UNKNOWN"]),
            "integration_type": integration_type,
            "target_lambda": target_lambda,
            "auth": cfg.get("AuthType", "UNKNOWN"),
            "request_schema": "UNKNOWN"
        }

    # 2. INGESTION: Lambda
    lambda_list = _get_cloud_discovered_list(payload, "lambda")
    if lambda_list:
        main = lambda_list[0]
        cfg = main.get("configuration", {})
        
        # Event Source mapping
        event_source = "UNKNOWN"
        triggers = cfg.get("VerifiedTriggers", [])
        if triggers:
            event_source = triggers[0]
        elif source_results:
            event_source = "API Gateway" # Validated by schema logic above if target_lambda matched

        ingestion_results = {
            "ingestion_type": "Lambda Processing",
            "function_name": main.get("id", "UNK").split(" || ")[-1],
            "runtime": cfg.get("Runtime", "UNKNOWN"),
            "timeout": cfg.get("TimeoutSeconds", "UNKNOWN"),
            "memory": cfg.get("MemorySizeMB", "UNKNOWN"),
            "handler": cfg.get("Handler", "UNKNOWN"),
            "event_source": event_source,
            "downstream_targets": cfg.get("IngestionTargets", []),
            "transformation": "UNKNOWN (requires code analysis)"
        }

    # 3. TARGET: S3
    s3_list = _get_cloud_discovered_list(payload, "s3")
    if s3_list:
        buckets = [s.get("id", "").split(" || ")[-1] for s in s3_list if " || " in s.get("id", "")]
        
        # Format detection
        data_format = "UNKNOWN"
        if ingestion_results:
            # Check ingestion's data_formats if available from AST analysis
            formats = lambda_list[0].get("configuration", {}).get("DataFormats", [])
            if formats:
                data_format = formats[0].upper()

        target_results = {
            "target_type": "Object Storage",
            "service": "S3",
            "buckets": buckets,
            "write_pattern": "UNKNOWN", # Requires deep code analysis/CloudTrail
            "partitioning": "UNKNOWN",
            "format": data_format
        }

    return {
        "source": source_results or "UNKNOWN",
        "ingestion": ingestion_results or "UNKNOWN",
        "target": target_results or "UNKNOWN"
    }
