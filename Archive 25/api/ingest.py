from fastapi import APIRouter, HTTPException, BackgroundTasks
from core.ingestion_service import IngestionService
from core.mcp_connector import SourceType

router = APIRouter(prefix="/ingest", tags=["Ingestion Execution"])

@router.post("/run")
async def run_ingestion(
    source_type: SourceType,
    client_name: str,
    folder_path: str,
    background_tasks: BackgroundTasks
):
    """
    Triggers the Ingestion Service:
    1. List files from Source (via MCP)
    2. Copy to Raw Layer
    3. Update Master Config
    """
    service = IngestionService()
    
    # Run in background to avoid timeout
    background_tasks.add_task(service.run_ingestion, source_type.value, client_name, folder_path)
    
    return {
        "status": "ACCEPTED",
        "message": f"Ingestion started for {client_name}/{folder_path} ({source_type}). Check logs or Master Config for updates."
    }
