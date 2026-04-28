from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from typing import Optional, Dict, Any
import logging
from services.pipeline_intelligence_service import analyze_pipeline_live

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/discovery", tags=["Pipeline Intelligence"])

class AnalyzeRequest(BaseModel):
    client_name: str
    target: Optional[str] = None
    auth_mode: Optional[str] = None
    credentials: Optional[Dict[str, Any]] = None
    use_cloud_llm: bool = True
    llm_provider: str = "gpt"
    use_local_llm: bool = False
    scan_mode: str = "live"
    providers: Optional[str] = None

@router.post("/analyze")
async def run_discovery_analyze(request: AnalyzeRequest, http_request: Request):
    """
    Analyzes the live cloud environment or configs for a client,
    and infers the underlying pipeline capabilities, DQ rules, and flow.
    """
    logger.info(f"Running live pipeline intelligence for {request.client_name}")
    try:
        authorization = http_request.headers.get("authorization")
        bearer_token = None
        if authorization and authorization.lower().startswith("bearer "):
            bearer_token = authorization.split(" ", 1)[1].strip()

        providers = request.providers or request.target
        result = await analyze_pipeline_live(
            client_name=request.client_name,
            providers=providers,
            target=request.target,
            auth_mode=request.auth_mode,
            credentials=request.credentials,
            use_cloud_llm=request.use_cloud_llm,
            llm_provider=request.llm_provider,
            use_local_llm=request.use_local_llm,
            scan_mode=request.scan_mode,
            authorization_token=bearer_token,
        )
        return result
    except Exception as e:
        logger.error(f"Error analyzing live pipeline: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
