import logging
import os
import json
from typing import Optional, Dict, Any, List
from urllib import request as urlrequest

from engine.scanner.manager import scanner_manager

logger = logging.getLogger(__name__)

class DummySettings:
    def __init__(self, credentials: Optional[Dict[str, Any]] = None):
        creds = credentials or {}
        self.aws_access_key_id = creds.get("access_key") or creds.get("aws_access_key_id") or os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = creds.get("secret_key") or creds.get("aws_secret_access_key") or os.getenv("AWS_SECRET_ACCESS_KEY")
        self.aws_region = creds.get("region") or os.getenv("AWS_REGION")
        self.aws_role_arn = creds.get("role_arn") or os.getenv("AWS_ROLE_ARN")
        self.azure_client_id = creds.get("client_id") or os.getenv("AZURE_CLIENT_ID")
        self.azure_client_secret = creds.get("client_secret") or os.getenv("AZURE_CLIENT_SECRET")
        self.azure_tenant_id = creds.get("tenant_id") or os.getenv("AZURE_TENANT_ID")
        self.azure_subscription_id = creds.get("subscription_id") or os.getenv("AZURE_SUBSCRIPTION_ID")
        self.azure_resource_group = creds.get("resource_group") or os.getenv("AZURE_RESOURCE_GROUP")
        self.databricks_host = os.getenv("DATABRICKS_HOST")
        self.databricks_token = os.getenv("DATABRICKS_TOKEN")

def _normalize_target(target: Optional[str], providers: Optional[str]) -> str:
    raw = (target or providers or "aws").split(",")[0].strip().lower()
    if raw in {"amazon", "s3", "glue"}:
        return "aws"
    if raw in {"adls", "adf"}:
        return "azure"
    if raw in {"microsoft fabric", "msfabric"}:
        return "fabric"
    return raw if raw in {"aws", "azure", "fabric"} else "aws"


def _fallback_raw_assets(target: str) -> Dict[str, List[Any]]:
    if target == "azure":
        return {
            "raw_cloud_dump": [{
                "storage_accounts": [{
                    "id": "azure || demo-landing-adls",
                    "configuration": {
                        "Kind": "StorageV2",
                        "IsHnsEnabled": True,
                        "DataFormats": ["CSV", "JSON", "Parquet"],
                    },
                }],
                "datafactory": [{
                    "id": "azure || DEA-Ingestion-ADF",
                    "configuration": {"ProvisioningState": "Succeeded", "Activities": ["Copy", "Validation"]},
                }],
            }]
        }
    if target == "fabric":
        return {
            "raw_cloud_dump": [{
                "fabric_workspaces": [{
                    "id": "fabric || DEA-Analytics",
                    "configuration": {"Type": "Workspace"},
                }],
                "fabric_items": [
                    {"id": "fabric || Bronze-Lakehouse", "configuration": {"Type": "Lakehouse"}},
                    {"id": "fabric || DEA-Ingestion-Pipeline", "configuration": {"Type": "Pipeline", "DataFormats": ["CSV", "Parquet"]}},
                ],
            }]
        }
    return {
        "raw_cloud_dump": [{
            "s3": [{
                "id": "aws || dea-demo-landing",
                "configuration": {
                    "StorageClass": "Standard",
                    "DataFormats": ["CSV", "JSON", "Parquet"],
                    "IngestionTargets": ["s3://dea-demo-landing/raw"],
                },
            }],
            "glue": [{
                "id": "aws || dea-bronze-ingestion",
                "configuration": {"GlueVersion": "4.0", "CommandScript": "s3://scripts/dea_bronze.py"},
            }],
        }]
    }


def _flatten_assets(raw: Any) -> List[Dict[str, Any]]:
    assets: List[Dict[str, Any]] = []

    def visit(value: Any, group: str = "asset"):
        if isinstance(value, dict):
            if "id" in value or "configuration" in value:
                assets.append({
                    "type": group,
                    "name": str(value.get("id") or value.get("name") or group),
                    "configuration": value.get("configuration", value),
                })
                return
            for key, nested in value.items():
                visit(nested, key)
        elif isinstance(value, list):
            for item in value:
                visit(item, group)
        elif value:
            assets.append({"type": group, "name": str(value), "configuration": {}})

    visit(raw)
    return assets


def _build_config(client_name: str, target: str, file_types: List[str], delimiter_config: Dict[str, Any], assets: List[Dict[str, Any]]) -> Dict[str, Any]:
    source_type = "S3" if target == "aws" else "ADLS" if target == "azure" else "LOCAL"
    source_path = (
        "s3://dea-demo-landing/raw"
        if target == "aws"
        else "az://demo-landing-adls/raw"
        if target == "azure"
        else f"upload/{client_name}"
    )
    return {
        "client_name": client_name,
        "source_type": source_type,
        "source_path": source_path,
        "file_types": file_types,
        "delimiter_config": delimiter_config,
        "asset_count": len(assets),
    }


def _safe_json_loads(text: str) -> Optional[Dict[str, Any]]:
    import re
    match = re.search(r"(\{[\s\S]*\})", text or "")
    if match:
        text = match.group(1)
    text = (text or "").strip().replace("```json", "").replace("```", "").strip()
    try:
        return json.loads(text)
    except Exception:
        return None


def _cloud_llm_extract(scan_result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    azure_key = os.getenv("AZURE_OPENAI_API_KEY", "")
    azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT", "")
    if not azure_key or not azure_endpoint:
        logger.info("GPT extraction skipped because Azure OpenAI env vars are not configured.")
        return None

    prompt = {
        "task": "Normalize cloud framework discovery into DEA pipeline intelligence JSON.",
        "required_keys": [
            "source_systems",
            "ingestion_support",
            "ingestion_types",
            "file_types",
            "delimiter_config",
            "dq_rules",
            "pipeline_capabilities",
            "reformatted_config",
            "llm_summary",
        ],
        "scan_result": scan_result,
    }
    body = {
        "messages": [
            {
                "role": "system",
                "content": "You extract data engineering pipeline facts from cloud inventory. Respond with only valid JSON. Do not invent secrets or credentials.",
            },
            {"role": "user", "content": json.dumps(prompt, default=str)},
        ],
        "max_tokens": 3000,
        "temperature": 0,
    }
    headers = {"Content-Type": "application/json", "api-key": azure_key}

    try:
        req = urlrequest.Request(azure_endpoint, data=json.dumps(body).encode("utf-8"), headers=headers, method="POST")
        with urlrequest.urlopen(req, timeout=90) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        content = payload.get("choices", [{}])[0].get("message", {}).get("content", "")
        parsed = _safe_json_loads(content)
        if not parsed:
            logger.warning("GPT extraction returned non-JSON content; using rule-based discovery response.")
        return parsed
    except Exception as exc:
        logger.warning(f"GPT extraction failed; using rule-based discovery response. Reason: {exc}")
        return None


def _merge_llm_overlay(base: Dict[str, Any], overlay: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not overlay:
        base["llm_summary"] = base.get("llm_summary") or "Rule-based extraction used; GPT extraction was unavailable."
        return base

    for key in [
        "source_systems",
        "ingestion_support",
        "ingestion_types",
        "file_types",
        "delimiter_config",
        "dq_rules",
        "pipeline_capabilities",
        "reformatted_config",
        "llm_summary",
    ]:
        value = overlay.get(key)
        if value not in (None, "", [], {}):
            base[key] = value
    base["llm_summary"] = base.get("llm_summary") or "GPT extraction completed."
    return base


async def analyze_pipeline_live(
    client_name: str,
    providers: Optional[str] = None,
    target: Optional[str] = None,
    auth_mode: Optional[str] = None,
    credentials: Optional[Dict[str, Any]] = None,
    use_cloud_llm: bool = True,
    llm_provider: str = "gpt",
    use_local_llm: bool = False,
    scan_mode: str = "live",
    authorization_token: Optional[str] = None,
):
    """
    Runs live cloud scan and extracts DEA capabilities.
    Fallback to local analysis if cloud scan fails or returns empty.
    """
    settings = DummySettings(credentials)
    target_key = _normalize_target(target, providers)
    provider_list = [p.strip().lower() for p in providers.split(",")] if providers else [target_key]
    has_request_credentials = bool(credentials) or bool(authorization_token)
    resolved_auth_mode = auth_mode or ("sso" if target_key == "fabric" and authorization_token else "credentials" if has_request_credentials else "none")
    scan_status = "success"
    is_fallback = False

    live_data = None
    try:
        live_data = await scanner_manager.scan_all(
            settings,
            providers=provider_list,
            azure_token=authorization_token,
            azure_token_fabric=authorization_token,
        )
    except Exception as e:
        logger.warning(f"Live scan failed: {e}. Using rule-based fallback.")
        scan_status = "partial"
        is_fallback = True

    if not live_data or not _flatten_assets(live_data):
        live_data = _fallback_raw_assets(target_key)
        scan_status = "partial"
        is_fallback = True

    combined_text = json.dumps(live_data).lower() if live_data else ""
    
    # 1. Framework detection
    detected_framework = "Microsoft Fabric" if target_key == "fabric" else "AWS Glue" if target_key == "aws" else "Azure Data Factory"
    if "fabric" in combined_text or "lakehouse" in combined_text:
        detected_framework = "Microsoft Fabric"
    elif "databricks" in combined_text or "spark" in combined_text:
        detected_framework = "Databricks"
    elif "adf" in combined_text or "typeproperties" in combined_text:
        detected_framework = "Azure Data Factory"
    elif "glue" in combined_text or "lambda" in combined_text:
        detected_framework = "AWS Glue"

    # 2. Ingestion Support
    file_based = any(ext in combined_text for ext in ["csv", "json", "parquet", "blob", "s3", "adls", "delimitedtext", "storage"])
    api_based = any(ext in combined_text for ext in ["webactivity", "http", "rest", "graphql", "apigateway"])
    database = any(ext in combined_text for ext in ["sql", "jdbc", "datawarehouse", "table", "warehousetable", "database"])
    streaming = any(ext in combined_text for ext in ["kafka", "eventhub", "stream", "kinesis"])
    batch = any(ext in combined_text for ext in ["foreach", "until", "loop", "batch", "lambda"])
    
    if not any([file_based, api_based, database]):
        file_based = True 
        batch = True

    # 3. File Types
    file_types = []
    if "csv" in combined_text or "delimited" in combined_text: file_types.append("CSV")
    if "json" in combined_text: file_types.append("JSON")
    if "parquet" in combined_text: file_types.append("Parquet")
    if "excel" in combined_text: file_types.append("Excel")
    if not file_types:
        file_types = ["CSV", "JSON", "Parquet"] # Provide common defaults
        
    # 4. Delimiter Config
    col_delim = ","
    quote_char = "\""
    escape_char = "\\\\"
    header = True
    
    # 5. DQ Rules
    schema_val = "schema" in combined_text or "validation" in combined_text or "datatype" in combined_text
    null_check = "null" in combined_text or "notnull" in combined_text
    dup_check = "duplicate" in combined_text or "distinct" in combined_text
    datatype = "datatype" in combined_text or "cast" in combined_text
    
    # 6. Loading Flow
    flow = ["Source", "Raw", "Bronze", "DQ Validation", "Silver", "Gold"]
    discovered_assets = _flatten_assets(live_data)
    data_pipelines = [
        asset for asset in discovered_assets
        if any(token in f"{asset.get('type')} {asset.get('name')} {asset.get('configuration')}".lower() for token in ["pipeline", "glue", "factory", "lambda", "function"])
    ]
    delimiter_config = {
        "column_delimiter": col_delim,
        "quote_char": quote_char,
        "escape_char": escape_char,
        "header": header,
    }
    ingestion_support = {
        "file_based": file_based,
        "api": api_based,
        "database": database,
        "streaming": streaming,
        "batch": batch,
    }
    ingestion_types = [k for k, v in ingestion_support.items() if v]
    source_systems = [
        {
            "name": asset.get("name"),
            "type": asset.get("type"),
            "configuration": asset.get("configuration", {}),
        }
        for asset in discovered_assets
        if any(token in f"{asset.get('type')} {asset.get('name')} {asset.get('configuration')}".lower() for token in ["s3", "storage", "api", "lakehouse", "blob", "adls"])
    ]
    dq_rules = {
        "schema_validation": schema_val or True,
        "null_check": null_check or True,
        "duplicate_check": dup_check,
        "datatype_check": datatype or True,
    }
    pipeline_capabilities = {
        "bronze": True,
        "silver": True,
        "gold": True,
        "cloud_llm_requested": bool(use_cloud_llm),
        "llm_provider": llm_provider,
        "scan_mode": scan_mode,
    }
    reformatted_config = _build_config(client_name, target_key, file_types, delimiter_config, discovered_assets)
    ingestion_details = {
        "target": target_key,
        "source_type": reformatted_config["source_type"],
        "source_path": reformatted_config["source_path"],
        "supported_modes": [k for k, v in ingestion_support.items() if v],
    }

    result = {
        "framework": detected_framework,
        "auth_mode": resolved_auth_mode,
        "scan_status": scan_status,
        "is_fallback": is_fallback,
        "discovered_assets": discovered_assets,
        "data_pipelines": data_pipelines,
        "source_systems": source_systems,
        "ingestion_support": ingestion_support,
        "ingestion_types": ingestion_types,
        "file_types": file_types,
        "delimiter_config": delimiter_config,
        "dq_rules": dq_rules,
        "pipeline_capabilities": pipeline_capabilities,
        "interactive_flow": flow,
        "ingestion_details": ingestion_details,
        "original_config": live_data or {},
        "reformatted_config": reformatted_config,
        "llm_summary": "",
        "loading_flow": flow,
        "raw_analysis": {"scanned_cloud_assets": len(discovered_assets)}
    }

    if use_cloud_llm and llm_provider == "gpt":
        result = _merge_llm_overlay(result, _cloud_llm_extract(result))
    else:
        result["llm_summary"] = "GPT extraction not requested; rule-based extraction used."

    return result
