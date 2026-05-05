import httpx
import json
import base64
from fastapi import HTTPException

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"

class FabricDeployService:
    def __init__(self, access_token: str):
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

    async def deploy_pipeline(self, workspace_id: str, pipeline_name: str, definition_content: dict):
        """Creates or updates a pipeline from a JSON definition"""
        url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items"
        
        # Check if exists
        items_resp = await httpx.AsyncClient().get(f"{url}?type=DataPipeline", headers=self.headers)
        items = items_resp.json().get("value", [])
        existing = next((i for i in items if i['displayName'] == pipeline_name), None)
        
        definition_b64 = base64.b64encode(json.dumps(definition_content).encode('utf-8')).decode('utf-8')
        
        if existing:
            # Update
            update_url = f"{url}/{existing['id']}/updateDefinition"
            payload = {
                "definition": {
                    "parts": [{"path": "pipeline-content.json", "payload": definition_b64, "payloadType": "InlineBase64"}]
                }
            }
            resp = await httpx.AsyncClient().post(update_url, headers=self.headers, json=payload)
        else:
            # Create
            payload = {
                "displayName": pipeline_name,
                "type": "DataPipeline",
                "definition": {
                    "parts": [{"path": "pipeline-content.json", "payload": definition_b64, "payloadType": "InlineBase64"}]
                }
            }
            resp = await httpx.AsyncClient().post(url, headers=self.headers, json=payload)
        
        if not resp.is_success:
             raise HTTPException(status_code=resp.status_code, detail=f"Deployment failed: {resp.text}")
        
        result = resp.json()
        pipeline_id = result.get('id')
        
        # Fetch full definition for intelligence
        get_url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items/{pipeline_id}/getDefinition"
        get_resp = await httpx.AsyncClient().post(get_url, headers=self.headers)
        
        pipeline_json = {}
        if get_resp.is_success:
            parts = get_resp.json().get("definition", {}).get("parts", [])
            for part in parts:
                if part.get("path") == "pipeline-content.json":
                    pipeline_json = json.loads(base64.b64decode(part.get("payload")).decode('utf-8'))
                    break
        
        return {
            "id": pipeline_id,
            "displayName": pipeline_name,
            "pipeline_json": pipeline_json
        }
