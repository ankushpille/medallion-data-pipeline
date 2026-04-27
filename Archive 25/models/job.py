from pydantic import BaseModel
from datetime import datetime
from enum import Enum

class JobStatus(str, Enum):
    CREATED = "CREATED"
    RUNNING = "RUNNING"
    FAILED = "FAILED"
    COMPLETED = "COMPLETED"

class Job(BaseModel):
    job_id: str
    status: JobStatus
    created_at: datetime
    message: str = ""
