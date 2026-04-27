from fastapi import APIRouter, HTTPException, Query
from typing import List, Dict, Any
from core.mcp_connector import get_mcp_connector, SourceType, DatasetInfo

router = APIRouter(prefix="/connect", tags=["MCP Connectivity"])

@router.get("/list", response_model=List[dict])
def list_datasets(
    source_type: SourceType,
    client_name: str,
    folder_path: str
):
    """
    Directly invoke MCP Connector to list datasets.
    Test your Source Connectivity here.
    """
    try:
        connector = get_mcp_connector(source_type.value)
        datasets = connector.list_datasets(client_name, folder_path)
        return [ds.to_dict() for ds in datasets]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/browse")
def browse_children(
    source_type: SourceType,
    client_name: str,
    path: str = Query(default="", description="Relative path under client (e.g., 'Clinical/Jan')")
) -> Dict[str, Any]:
    """
    Lists immediate folders and files under the given path to support drill-down UI.
    - path is relative to client root; use empty string for top-level.
    Returns {"path": path, "folders": [...], "files": [...]}
    """
    try:
        connector = get_mcp_connector(source_type.value)
        children = connector.list_children(client_name, path)
        return {"path": path, **children}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
