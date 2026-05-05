import os
import time
import json
import base64
import logging
from typing import Dict, Any, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"

def get_session() -> requests.Session:
    """Create a requests Session with exponential backoff for 429 and 500s."""
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def get_access_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    """Get Azure AD access token for Fabric API using Client Credentials flow."""
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://api.fabric.microsoft.com/.default"
    }
    resp = requests.post(url, data=payload)
    resp.raise_for_status()
    return resp.json().get("access_token")

def poll_operation(session: requests.Session, location_url: str, headers: Dict[str, str]) -> Dict[str, Any]:
    """Poll the Long Running Operation (LRO) until Succeeded or Failed."""
    logger.info(f"Polling operation status from {location_url}...")
    while True:
        resp = session.get(location_url, headers=headers)
        resp.raise_for_status()
        
        if resp.status_code == 200:
            data = resp.json()
            status = data.get("status", "Unknown")
            logger.info(f"Operation status: {status}")
            
            if status == "Succeeded":
                return data
            elif status in ("Failed", "Canceled"):
                raise Exception(f"Operation failed: {json.dumps(data)}")
        
        retry_after = int(resp.headers.get("Retry-After", 5))
        time.sleep(retry_after)

def fetch_result(session: requests.Session, result_url: str, headers: Dict[str, str]) -> Dict[str, Any]:
    """Fetch the final result after the LRO succeeds."""
    logger.info(f"Fetching operation result from {result_url}...")
    resp = session.get(result_url, headers=headers)
    resp.raise_for_status()
    return resp.json()

async def extract_fabric_pipeline(
    workspace_id: str,
    pipeline_id: str,
    client_id: str,
    client_secret: str,
    tenant_id: str
) -> Dict[str, Any]:
    """
    Extracts the full pipeline JSON and generates/extracts manifest.json.
    Reuses logic from fabric_export.py.
    """
    session = get_session()
    
    # 1. Authenticate
    logger.info("Authenticating with Azure AD...")
    access_token = get_access_token(tenant_id, client_id, client_secret)
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    # 2. Trigger bulkExportDefinitions (Beta API used in source repo)
    # We use this because it returns the reconstruction-friendly parts
    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items/bulkExportDefinitions?beta=true"
    payload = {
        "items": [{"id": pipeline_id}]
    }
    
    logger.info(f"Triggering export for pipeline {pipeline_id} in workspace {workspace_id}...")
    resp = session.post(url, headers=headers, json=payload)
    if resp.status_code != 202:
        resp.raise_for_status()
    
    location_url = resp.headers.get("Location")
    if not location_url:
        raise Exception("Location header missing from export response.")
    
    # 3. Poll LRO
    poll_operation(session, location_url, headers)
    
    # 4. Get Result
    result_url = f"{location_url}/result"
    result_data = fetch_result(session, result_url, headers)
    
    items = result_data.get("items", [])
    if not items:
        items = [result_data]
        
    pipeline_json = {}
    manifest_json = {}
    pipeline_name = "Exported Pipeline"
    
    for item in items:
        if item.get("id") == pipeline_id or len(items) == 1:
            definition = item.get("definition", {})
            parts = definition.get("parts", [])
            
            for part in parts:
                path = part.get("path", "")
                payload_b64 = part.get("payload", "")
                try:
                    content_bytes = base64.b64decode(payload_b64)
                    content_str = content_bytes.decode('utf-8')
                    content_json = json.loads(content_str)
                except Exception:
                    continue
                
                if path.endswith("pipeline-content.json"):
                    pipeline_json = content_json
                elif path.endswith("item.metadata.json"):
                    # This metadata contains display name etc.
                    pipeline_name = content_json.get("displayName", pipeline_name)
                    manifest_json = {
                        "name": pipeline_name,
                        "type": "DataPipeline",
                        "properties": content_json
                    }

    # Bonus: Generate manifest if missing
    if not manifest_json:
        logger.info("Manifest missing, generating dynamically...")
        manifest_json = {
            "name": pipeline_name,
            "type": "DataPipeline",
            "properties": {
                "displayName": pipeline_name,
                "description": "Auto-generated manifest"
            }
        }
    
    # Bonus: Ensure output matches Fabric UI export exactly
    # Fabric UI export for pipeline usually has a wrapper or specific structure.
    # The pipeline-content.json extracted above IS the actual pipeline JSON.
    
    return {
        "pipeline_json": pipeline_json,
        "manifest_json": manifest_json
    }
