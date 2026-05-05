import httpx
import json
import base64
import zipfile
import io
from fastapi import HTTPException

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"

class FabricDeployService:
    def __init__(self, access_token: str):
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

    async def deploy_pipeline(self, workspace_id: str, file_bytes: bytes):
        """Automated ZIP-based deployment: Dynamically detects content and deploys"""
        
        # 1. Extract content from ZIP with flexible detection
        try:
            with zipfile.ZipFile(io.BytesIO(file_bytes)) as z:
                json_files = [f for f in z.namelist() if f.endswith('.json')]
                
                pipeline_file = None
                manifest_file = None
                
                for f in json_files:
                    if 'manifest.json' in f.lower():
                        manifest_file = f
                    else:
                        # Assume the first other JSON is the pipeline content
                        pipeline_file = f
                
                if not pipeline_file:
                    raise HTTPException(status_code=400, detail="No pipeline JSON file found in ZIP")
                
                pipeline_content = z.read(pipeline_file).decode('utf-8')
                pipeline_name = "New Pipeline"
                
                if manifest_file:
                    manifest_data = json.loads(z.read(manifest_file).decode('utf-8'))
                    pipeline_name = manifest_data.get('displayName', pipeline_name)
                else:
                    # Fallback to filename (without .json)
                    pipeline_name = pipeline_file.split('/')[-1].replace('.json', '')
                
                definition_dict = json.loads(pipeline_content)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to process ZIP: {str(e)}")

        # 2. Check if pipeline exists in workspace
        url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items"
        async with httpx.AsyncClient(timeout=30.0) as client:
            items_resp = await client.get(f"{url}?type=DataPipeline", headers=self.headers)
            items = items_resp.json().get("value", [])
            existing = next((i for i in items if i['displayName'] == pipeline_name), None)
            
            definition_b64 = base64.b64encode(json.dumps(definition_dict).encode('utf-8')).decode('utf-8')
            
            if existing:
                # Update Definition
                update_url = f"{url}/{existing['id']}/updateDefinition"
                payload = {
                    "definition": {
                        "parts": [{"path": "pipeline-content.json", "payload": definition_b64, "payloadType": "InlineBase64"}]
                    }
                }
                resp = await client.post(update_url, headers=self.headers, json=payload)
                pipeline_id = existing['id']
            else:
                # Create New
                payload = {
                    "displayName": pipeline_name,
                    "type": "DataPipeline",
                    "definition": {
                        "parts": [{"path": "pipeline-content.json", "payload": definition_b64, "payloadType": "InlineBase64"}]
                    }
                }
                resp = await client.post(url, headers=self.headers, json=payload)
                pipeline_id = resp.json().get('id') if resp.is_success else None

            if not resp.is_success:
                 raise HTTPException(status_code=resp.status_code, detail=f"Fabric API error: {resp.text}")
            
            return {
                "id": pipeline_id,
                "displayName": pipeline_name,
                "status": "Success"
            }
