from fastapi import APIRouter, HTTPException
from core.pipeline_service import PipelineService

router = APIRouter(prefix="/pipeline", tags=["Pipeline"])

@router.post("/run/{dataset_id}")
def run_pipeline(dataset_id: str):
    try:
        service = PipelineService()
        metrics = service.run(dataset_id)
        return {"status": "SUCCESS", "dataset_id": dataset_id, "metrics": metrics}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))