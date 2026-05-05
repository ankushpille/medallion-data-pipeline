from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File, Form
from typing import List, Optional, Dict
from services.fabric.auth_service import FabricAuthService
from services.fabric.workspace_service import FabricWorkspaceService
from services.fabric.pipeline_service import FabricPipelineService
from services.fabric.deploy_service import FabricDeployService
import json

router = APIRouter(prefix="/fabric", tags=["fabric"])

# We'll use a simple in-memory or session-based token storage for this demo
# In production, use secure cookies or database
temp_tokens = {}

router = APIRouter(prefix="/fabric", tags=["fabric"])

@router.get("/workspaces")
async def list_workspaces(token: str):
    ws_service = FabricWorkspaceService(token)
    return await ws_service.list_workspaces()

@router.get("/pipelines")
async def list_pipelines(workspace_id: str, token: str):
    p_service = FabricPipelineService(token)
    return await p_service.list_pipelines(workspace_id)

@router.post("/discover")
async def discover(workspace_id: str, token: str, pipeline_ids: List[str]):
    p_service = FabricPipelineService(token)
    return await p_service.bulk_export_definitions(workspace_id, pipeline_ids)

@router.post("/deploy")
async def deploy(
    workspace_id: str = Form(...),
    pipeline_name: str = Form(...),
    token: str = Form(...),
    file: UploadFile = File(...)
):
    deploy_service = FabricDeployService(token)
    content = await file.read()
    try:
        definition = json.loads(content)
    except:
        raise HTTPException(status_code=400, detail="Invalid JSON file")
    
    return await deploy_service.deploy_pipeline(workspace_id, pipeline_name, definition)

@router.post("/analyze")
async def analyze_fabric_pipeline(
    client_name: str,
    pipeline_json: dict
):
    p_service = FabricPipelineService("") # No token needed for static analysis
    return p_service.analyze_pipeline_json(pipeline_json, client_name)

@router.post("/complete")
async def complete(
    source_type: str = "fabric",
    mode: str = "discover",
    workspace_id: str = None,
    pipeline_id: str = None,
    token: str = None
):
    if mode == "discover" and pipeline_id and token:
        p_service = FabricPipelineService(token)
        results = await p_service.bulk_export_definitions(workspace_id, [pipeline_id])
        pipeline_json = json.loads(results.get(pipeline_id, {}).get("pipeline.json", b"{}").decode('utf-8'))
        return {
            "source_type": "fabric",
            "mode": mode,
            "workspace_id": workspace_id,
            "pipeline_json": pipeline_json
        }
    return {
        "source_type": "fabric",
        "mode": mode,
        "workspace_id": workspace_id
    }

@router.post("/extract")
async def extract(workspace_id: str, pipeline_id: str, token: str):
    # Backward compatibility or shortcut
    p_service = FabricPipelineService(token)
    results = await p_service.bulk_export_definitions(workspace_id, [pipeline_id])
    if pipeline_id in results:
        # Return the first one in a friendly format
        files = results[pipeline_id]
        return {
            "pipeline": json.loads(files.get("pipeline.json", b"{}").decode('utf-8')),
            "manifest": json.loads(files.get("manifest.json", b"{}").decode('utf-8'))
        }
    raise HTTPException(status_code=404, detail="Pipeline not found")
