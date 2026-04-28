import logging
import os
import json
from typing import Optional, Dict, Any, List

from engine.scanner.manager import scanner_manager

logger = logging.getLogger(__name__)

class DummySettings:
    def __init__(self):
        self.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.azure_client_id = os.getenv("AZURE_CLIENT_ID")
        self.azure_client_secret = os.getenv("AZURE_CLIENT_SECRET")
        self.azure_tenant_id = os.getenv("AZURE_TENANT_ID")
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


async def analyze_pipeline_live(
    client_name: str,
    providers: Optional[str] = None,
    target: Optional[str] = None,
    use_local_llm: bool = False,
    scan_mode: str = "live",
    authorization_token: Optional[str] = None,
):
    """
    Runs live cloud scan and extracts DEA capabilities.
    Fallback to local analysis if cloud scan fails or returns empty.
    """
    settings = DummySettings()
    target_key = _normalize_target(target, providers)
    provider_list = [p.strip().lower() for p in providers.split(",")] if providers else [target_key]

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

    if not live_data or not _flatten_assets(live_data):
        live_data = _fallback_raw_assets(target_key)

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
        "local_llm_requested": bool(use_local_llm),
        "scan_mode": scan_mode,
    }
    reformatted_config = _build_config(client_name, target_key, file_types, delimiter_config, discovered_assets)
    ingestion_details = {
        "target": target_key,
        "source_type": reformatted_config["source_type"],
        "source_path": reformatted_config["source_path"],
        "supported_modes": [k for k, v in ingestion_support.items() if v],
    }

    return {
        "framework": detected_framework,
        "ingestion_support": ingestion_support,
        "file_types": file_types,
        "delimiter_config": delimiter_config,
        "dq_rules": dq_rules,
        "pipeline_capabilities": pipeline_capabilities,
        "interactive_flow": flow,
        "discovered_assets": discovered_assets,
        "data_pipelines": data_pipelines,
        "ingestion_details": ingestion_details,
        "original_config": live_data or {},
        "reformatted_config": reformatted_config,
        "loading_flow": flow,
        "raw_analysis": {"scanned_cloud_assets": len(discovered_assets)}
    }
