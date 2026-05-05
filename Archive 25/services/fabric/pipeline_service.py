import httpx
import json
import asyncio
import base64
from fastapi import HTTPException
import logging

logger = logging.getLogger(__name__)
FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"

class FabricPipelineService:
    def __init__(self, access_token: str):
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

    async def list_pipelines(self, workspace_id: str):
        async with httpx.AsyncClient(timeout=30.0) as client:
            # Note: Fabric items API
            resp = await client.get(f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items?type=DataPipeline", headers=self.headers)
            resp.raise_for_status()
            return resp.json().get("value", [])

    async def bulk_export_definitions(self, workspace_id: str, pipeline_ids: list):
        """Polls LRO and returns a dict of {pipeline_id: {filename: content}}"""
        url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items/bulkExportDefinitions?beta=true"
        payload = {
            "mode": "Selective",
            "items": [{"id": pid, "type": "DataPipeline"} for pid in pipeline_ids]
        }
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(url, headers=self.headers, json=payload)
            if not resp.is_success:
                raise HTTPException(status_code=resp.status_code, detail=f"Export failed: {resp.text}")
            
            location_url = resp.headers.get("Location")
            if not location_url:
                raise Exception("Location header missing")
                
            # Poll
            while True:
                poll_resp = await client.get(location_url, headers=self.headers)
                poll_resp.raise_for_status()
                if poll_resp.status_code == 200:
                    status_data = poll_resp.json()
                    if status_data.get("status") == "Succeeded":
                        break
                    elif status_data.get("status") in ("Failed", "Canceled"):
                        raise Exception(f"Export failed: {status_data}")
                await asyncio.sleep(2)

            # Result
            res_resp = await client.get(f"{location_url}/result", headers=self.headers)
            res_resp.raise_for_status()
            result_data = res_resp.json()
            
            results = {}
            item_index = result_data.get("itemDefinitionsIndex", [])
            definition_parts = result_data.get("definitionParts", [])
            
            for idx_entry in item_index:
                pid = idx_entry.get("id")
                root_path = idx_entry.get("rootPath")
                files = {}
                for part in definition_parts:
                    path = part.get("path", "")
                    if path.startswith(root_path):
                        rel_path = path[len(root_path):].lstrip("/")
                        payload_b64 = part.get("payload", "")
                        content = base64.b64decode(payload_b64)
                        
                        if rel_path == "pipeline-content.json":
                            files["pipeline.json"] = content
                        elif rel_path == "item.metadata.json":
                            try:
                                metadata = json.loads(content)
                                manifest = {
                                    "name": metadata.get("displayName", "Pipeline"),
                                    "type": "DataPipeline",
                                    "properties": metadata
                                }
                                files["manifest.json"] = json.dumps(manifest, indent=2).encode('utf-8')
                            except:
                                files["manifest.json"] = content
                        else:
                            files[rel_path] = content
                results[pid] = files
            return results

    def analyze_pipeline_json(self, pipeline_json: dict, client_name: str):
        """Analyzes a Fabric pipeline JSON and returns intelligence-style data"""
        activities = pipeline_json.get("properties", {}).get("activities", [])
        
        # Simple extraction logic similar to DiscoveryEngine
        ingestion_support = {
            "file_based": any("S3" in str(a) or "ADLS" in str(a) or "File" in str(a) for a in activities),
            "api": any("Rest" in str(a) or "Http" in str(a) for a in activities),
            "database": any("Sql" in str(a) or "Jdbc" in str(a) for a in activities),
            "streaming": "EventHub" in str(pipeline_json),
            "batch": True
        }
        
        file_types = []
        raw_str = json.dumps(pipeline_json).lower()
        if "csv" in raw_str: file_types.append("CSV")
        if "json" in raw_str: file_types.append("JSON")
        if "parquet" in raw_str: file_types.append("Parquet")
        
        return {
            "framework": "Microsoft Fabric",
            "scan_status": "success",
            "auth_mode": "sso",
            "is_fallback": False,
            "source_systems": [{"type": "Fabric", "name": client_name}],
            "discovered_assets": [{"type": "Pipeline", "name": pipeline_json.get("name", "Unknown")}],
            "data_pipelines": [{"name": pipeline_json.get("name", "Fabric Pipeline"), "type": "Fabric"}],
            "ingestion_support": ingestion_support,
            "ingestion_details": {"source_type": "FABRIC", "target": "fabric"},
            "pipeline_capabilities": {"discovery": True, "export": True},
            "file_types": file_types or ["Not Available"],
            "original_config": pipeline_json,
            "reformatted_config": {
                "client": client_name,
                "source_type": "FABRIC",
                "activities_count": len(activities)
            },
            "interactive_flow": ["Connect to Fabric", "Extract Pipeline", "Analyze Logic", "Generate Config"]
        }
