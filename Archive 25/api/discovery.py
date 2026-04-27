from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from typing import Optional
import logging
from services.pipeline_intelligence_service import analyze_pipeline_live

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/discovery", tags=["Pipeline Intelligence"])

class AnalyzeRequest(BaseModel):
    client_name: str
    providers: Optional[str] = None

@router.post("/analyze")
async def run_discovery_analyze(request: AnalyzeRequest):
    """
    Analyzes the live cloud environment or configs for a client,
    and infers the underlying pipeline capabilities, DQ rules, and flow.
    """
    logger.info(f"Running live pipeline intelligence for {request.client_name}")
    try:
        result = await analyze_pipeline_live(
            client_name=request.client_name,
            providers=request.providers
        )
        return result
    except Exception as e:
        logger.error(f"Error analyzing live pipeline: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
