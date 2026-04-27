"""
handlers/blob_trigger.py
========================
Azure Function that replaces the AWS Lambda  handlers/s3_trigger.py.

AWS → Azure mapping:
  AWS Lambda + S3 Event Notification  →  Azure Function + Blob Storage trigger
  Lambda handler(event, context)      →  main(myblob: func.InputStream)
  s3://bucket/key                     →  az://container/blob_path

Deployment:
  - Runtime : Python 3.11
  - Trigger  : Azure Blob Storage  (storage account / container / path pattern)
  - Binding  : configure in function.json  or  via @app.blob_trigger decorator

function.json example
---------------------
{
  "scriptFile": "blob_trigger.py",
  "bindings": [
    {
      "name": "myblob",
      "type": "blobTrigger",
      "direction": "in",
      "path": "landing/{client_name}/{folder_path}/{name}",
      "connection": "AZURE_STORAGE_CONNECTION_STRING"
    }
  ]
}

Environment variables required (same as the rest of the project):
  AZURE_STORAGE_ACCOUNT
  AZURE_STORAGE_CONNECTION_STRING  (or AZURE_CLIENT_ID / SECRET / TENANT_ID)
  AZURE_CONTAINER_NAME
  ADLS_CONTAINER_NAME
  DATABASE_URL
"""

import json
import logging

# Azure Functions SDK
import azure.functions as func

from loguru import logger
from agents.configuration import ConfigurationAgent
from core.job_store import create_job

# -----------------------------------------------------------------------
# Azure Function App entry point
# -----------------------------------------------------------------------

app = func.FunctionApp()


@app.blob_trigger(
    arg_name="myblob",
    path="landing/{name}",              # adjust to your container / path pattern
    connection="AZURE_STORAGE_CONNECTION_STRING",
)
def blob_trigger(myblob: func.InputStream) -> None:
    """
    Triggered whenever a new blob is created in the configured container/path.

    myblob.name  → full blob path  e.g. "landing/ClientA/IDC/file.csv"
    myblob.uri   → full URI       e.g. "https://<account>.blob.core.windows.net/landing/..."
    """

    blob_name: str = myblob.name        # e.g. "landing/ClientA/IDC/file.csv"
    container: str = blob_name.split("/")[0] if "/" in blob_name else "landing"
    blob_path: str = "/".join(blob_name.split("/")[1:]) if "/" in blob_name else blob_name

    logger.info(f"Blob trigger fired: container={container}  blob={blob_path}")

    try:
        # Create a job record
        job = create_job()
        job_id = job.job_id
        logger.info(f"Processing az://{container}/{blob_path}  job_id={job_id}")

        # Run the Configuration Agent (same logic as before)
        agent = ConfigurationAgent()
        config = agent.generate_config(container, blob_path, job_id)

        logger.info(f"Successfully generated config for job {job_id}")

    except Exception as exc:
        logger.error(f"Blob trigger execution failed: {exc}")
        # Re-raise so the Azure Functions runtime marks the invocation as failed
        # (enables retry policy / dead-letter queue configured in host.json)
        raise


# -----------------------------------------------------------------------
# Local test entry point  (python handlers/blob_trigger.py)
# -----------------------------------------------------------------------

if __name__ == "__main__":
    """
    Simulate a blob trigger locally for quick smoke-testing.

    Usage:
        AZURE_STORAGE_ACCOUNT=myaccount \
        AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=..." \
        python handlers/blob_trigger.py
    """
    from dotenv import load_dotenv
    load_dotenv()

    class _FakeBlobStream:
        """Minimal mock of func.InputStream for local testing."""
        name: str = "landing/TestClient/IDC/sample.csv"
        uri: str = "https://localhost/landing/TestClient/IDC/sample.csv"

        def read(self) -> bytes:
            return b""

    blob_trigger.build().get_user_function()(_FakeBlobStream())
