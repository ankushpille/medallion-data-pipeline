from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional
import uuid
from loguru import logger

from agents.configuration import ConfigurationAgent
from core.job_store import create_job

router = APIRouter(prefix="/config", tags=["Configuration"])

class ConfigRequest(BaseModel):
    bucket: str
    key: str
    sample_rows: Optional[int] = 100

@router.post("/generate")
def generate_config(request: ConfigRequest):
    """
    Trigger the Configuration Agent to generate a config for a dataset in S3.
    """
    job = create_job()
    job_id = job.job_id
    logger.info(f"Received request to generate config for s3://{request.bucket}/{request.key} (Job ID: {job_id})")

    try:
        agent = ConfigurationAgent()
        
        config = agent.generate_config(
            bucket=request.bucket,
            key=request.key,
            job_id=job_id,
            sample_rows=request.sample_rows
        )
        
        return {
            "status": "SUCCESS",
            "job_id": job_id,
            "config": config
        }

    except Exception as e:
        logger.error(f"Failed to generate config: {e}")
        raise HTTPException(status_code=500, detail=str(e))
