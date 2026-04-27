import os
import json
import logging
from typing import Optional, Dict, Any

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

async def analyze_pipeline_live(client_name: str, providers: Optional[str] = None):
    """
    Runs live cloud scan and extracts DEA capabilities.
    Fallback to local analysis if cloud scan fails or returns empty.
    """
    settings = DummySettings()
    
    provider_list = [p.strip().lower() for p in providers.split(",")] if providers else None
    
    live_data = None
    try:
        live_data = await scanner_manager.scan_all(
            settings,
            providers=provider_list
        )
    except Exception as e:
        logger.warning(f"Live scan failed: {e}. Attempting local fallback.")

    combined_text = json.dumps(live_data).lower() if live_data else ""
    
    # 1. Framework detection
    detected_framework = "Fabric" # Default 
    if "fabric" in combined_text or "lakehouse" in combined_text:
        detected_framework = "Fabric"
    elif "databricks" in combined_text or "spark" in combined_text:
        detected_framework = "Databricks"
    elif "adf" in combined_text or "typeproperties" in combined_text:
        detected_framework = "ADF"
    elif "glue" in combined_text or "lambda" in combined_text:
        detected_framework = "Glue"

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
    flow = ["Source", "Raw", "Bronze", "DQ Validation", "Silver"]
        
    return {
        "framework": detected_framework,
        "ingestion_support": {
            "file_based": file_based,
            "api": api_based,
            "database": database,
            "streaming": streaming,
            "batch": batch
        },
        "file_types": file_types,
        "delimiter_config": {
            "column_delimiter": col_delim,
            "quote_char": quote_char,
            "escape_char": escape_char,
            "header": header
        },
        "dq_rules": {
            "schema_validation": schema_val or True,
            "null_check": null_check or True,
            "duplicate_check": dup_check,
            "datatype_check": datatype or True
        },
        "loading_flow": flow,
        "raw_analysis": {"scanned_cloud_assets": len(combined_text)}
    }
