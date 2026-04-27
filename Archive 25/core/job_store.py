from models.job import Job, JobStatus
from datetime import datetime
import uuid

_JOB_STORE = {}

def create_job() -> Job:
    job_id = str(uuid.uuid4())
    job = Job(
        job_id=job_id,
        status=JobStatus.CREATED,
        created_at=datetime.utcnow()
    )
    _JOB_STORE[job_id] = job
    return job

def update_job(job_id: str, status: JobStatus, message: str = ""):
    job = _JOB_STORE[job_id]
    job.status = status
    job.message = message
