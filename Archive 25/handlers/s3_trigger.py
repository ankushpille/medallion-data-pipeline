import json
import uuid
import boto3
from loguru import logger
from agents.configuration import ConfigurationAgent
from core.job_store import create_job

def lambda_handler(event, context):
    """
    AWS Lambda handler for S3 Object Created events.
    """
    try:
        logger.info(f"Received event: {json.dumps(event)}")

        # Parse S3 event
        record = event['Records'][0]
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        # Generate Job ID
        job = create_job()
        job_id = job.job_id
        logger.info(f"Processing s3://{bucket}/{key} with Job ID: {job_id}")

        # Initialize Agent
        agent = ConfigurationAgent()
        
        # Run Agent
        config = agent.generate_config(bucket, key, job_id)
        
        logger.info(f"Successfully generated config for job {job_id}")
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Config generated successfully",
                "job_id": job_id,
                "config_location": f"s3://{bucket}/configs/{config['dataset_id']}.json"
            })
        }

    except Exception as e:
        logger.error(f"Lambda execution failed: {str(e)}")
        # Re-raise to mark Lambda computation as failed (triggers retries/DLQ)
        raise e
