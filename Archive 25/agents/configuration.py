"""
Configuration Agent

Responsibility:
- Inspect incoming dataset from S3
- Validate basic structure
- Infer schema from sample data
- Generate configuration file
- Log dataset metadata
- Send email notification if dataset is invalid
"""

import json
import uuid
from datetime import datetime
from typing import Dict, List

import pandas as pd
from loguru import logger

from core.job_store import update_job
from core.job_store import update_job
from models.job import JobStatus
from core.settings import settings
from core.azure_storage import get_storage_client
from core.notifications import NotificationService
# Database imports
import hashlib
from sqlalchemy.orm import Session
from core.database import SessionLocal, engine, Base
from models.master_config import MasterConfig
from tools.config_exporter import export_master_config_to_storage


class ConfigurationAgent:
    def __init__(self):
        self.storage = get_storage_client()
        self.container = settings.AZURE_CONTAINER_NAME or "datalake"
        self.notifier = NotificationService()

    # -------------------------
    # PUBLIC ENTRY POINT
    # -------------------------
    def generate_config(
        self,
        bucket: str,
        key: str,
        job_id: str,
        sample_rows: int = 100
    ) -> Dict:
        """
        Main method called by orchestration or lambda trigger
        """

        logger.info(f"Configuration Agent started for job {job_id}")

        try:
            # STEP 1: Basic file validation
            metadata = self._validate_file(bucket, key)

            # STEP 2: Read sample data
            df = self._read_sample(bucket, key, sample_rows)

            # STEP 3: Validate dataset structure
            self._validate_structure(df)

            # STEP 4: Infer schema
            schema = self._infer_schema(df)

            # STEP 5: Register in Master Config Table (Postgres)
            # We construct a minimal config dict to pass to register logic 
            # or refactor register to take schema directly.
            # For now, let's create a temporary context dict.
            file_format = key.split(".")[-1]
            config_context = {
                "ingestion": {"file_format": file_format},
                "dataset_schema": {"columns": schema},
                # Dataset ID will be computed by hash inside check
                "dataset_id": "PENDING_COMPUTATION" 
            }
            
            self._register_config_in_db(config_context, bucket, key)
            
            # STEP 6: Re-generate Master Config Excel in S3
            export_master_config_to_storage(self.container)

            # STEP 7: Promote to Raw Layer (Ingestion)
            # Use computed hash? Or just file path?
            # User wants raw/<client>/<batch>/... 
            # We'll rely on time-based batch for now as per legacy helper, 
            # but ideally should match IngestionService.
            self._promote_to_raw(bucket, key, "RAW_INGEST")

            # STEP 8: Log success
            update_job(job_id, JobStatus.COMPLETED, "CONFIG_REGISTERED_AND_RAW_INGESTED")

            logger.info(f"Config processing successfully for job {job_id}")
            return config_context

        except Exception as exc:
            logger.error(f"Config generation failed: {exc}")
            update_job(job_id, JobStatus.FAILED, str(exc))
            self._send_failure_email(bucket, key, str(exc))
            raise

    # -------------------------
    # VALIDATION DELEGATED TO CORE
    # -------------------------
    def _validate_file(self, bucket: str, key: str) -> Dict:
        from core.validation import ValidationService
        return ValidationService.validate_file(self.storage, bucket, key)
    
    # ... _read_sample stays ...

    def _validate_structure(self, df: pd.DataFrame):
        from core.validation import ValidationService
        ValidationService.validate_structure(df)

    # -------------------------
    # SCHEMA INFERENCE
    # -------------------------
    def _infer_schema(self, df: pd.DataFrame) -> List[Dict]:
        """
        Infer column names and data types
        """

        schema = []

        for col in df.columns:
            dtype = str(df[col].dtype)

            if "int" in dtype:
                col_type = "integer"
            elif "float" in dtype:
                col_type = "float"
            elif "datetime" in dtype:
                col_type = "date"
            else:
                col_type = "string"

            schema.append(
                {
                    "name": col,
                    "type": col_type,
                    "nullable": bool(df[col].isnull().any()),
                }
            )

        logger.info("Schema inference completed")
        return schema



    # -------------------------
    # DATA PROMOTION (INGESTION)
    # -------------------------
    def _promote_to_raw(self, bucket: str, key: str, dataset_id: str):
        """
        Copy the validated source file to the Raw Layer
        """
        extension = key.split(".")[-1]
        
        # Format: Month_Day_Hour (e.g., January_05_12)
        timestamp_folder = datetime.utcnow().strftime("%B_%d_%H")
        
        raw_key = f"raw/{timestamp_folder}/data.{extension}"

        logger.info(f"Promoting file to Raw Layer: az://{self.container}/{raw_key}")

        # Read source blob then write to raw path
        obj = self.storage.get_object(Key=key, Container=self.container)
        data = obj["Body"].read()
        self.storage.put_object(Container=self.container, Key=raw_key, Body=data)

        logger.info("Promotion to Raw Layer successful")

    # -------------------------
    # EMAIL NOTIFICATION
    # -------------------------
    def _send_failure_email(self, bucket: str, key: str, reason: str):
        """
        Send email notification for invalid dataset
        """

        message = f"""
        Dataset validation failed.

        Bucket: {bucket}
        Key: {key}

        Reason:
        {reason}

        Please correct the dataset and resend.
        """

        # Use NotificationService instead of SES
        self.notifier.send_email(
            subject="Dataset Rejected",
            body=message,
        )

        logger.info("Failure email triggered")


    # -------------------------
    # DB REGISTRATION (MASTER CONFIG)
    # -------------------------
    def _register_config_in_db(self, config: Dict, bucket: str, key: str):
        """
        Idempotent insertion into Master Configuration Table
        """
        # Create Tables if not exist (Should be done via migration, but ok for demo)
        Base.metadata.create_all(bind=engine)
        
        session: Session = SessionLocal()
        try:
            # 1. Compute Identity Hash
            # Formula: hash(client_name + source_adls_folder_path + source_object)
            # In our S3 simulation:
            # Client = Bucket (or derived from path)
            # Folder = Directory of Key
            # Object = Filename
            
            # Assuming key structure: incoming/file.csv -> Client is unknown?
            # Or assume key structure: incoming/AMGEN/IDC/file.csv
            
            parts = key.split("/")
            if len(parts) > 2:
                # Example: AMGEN/IDC/file.csv
                client_name = parts[0] # AMGEN
                folder_path = "/".join(parts[1:-1]) # IDC
                filename = parts[-1] # file.csv
            else:
                # Fallback
                client_name = "DEFAULT_CLIENT"
                folder_path = "root"
                filename = key
                
            raw_string = f"{client_name}{folder_path}{filename}"
            dataset_id = hashlib.sha256(raw_string.encode()).hexdigest()
            
            # Check existance
            existing = session.query(MasterConfig).filter(MasterConfig.dataset_id == dataset_id).first()
            
            if existing:
                logger.info(f"Dataset {dataset_id} already exists in Master Config. Skipping Insert.")
                # We do NOT overwrite as per requirements.
                return
            
            # Insert New
            new_config = MasterConfig(
                dataset_id=dataset_id,
                pipeline_id=uuid.UUID(config["dataset_id"]), # Use the one we generated or generate new? 
                # Requirement sets pipeline_id as "Auto-filled". We can use the json's ID or new one.
                # Let's match the JSON ID for consistency.
                
                source_system=f"s3://{bucket}/{folder_path}",
                source_schema="public", # Default
                source_object=filename,
                
                target_layer="Bronze",
                target_table=filename.split(".")[0], # Simple cleaning
                
                file_format=config["ingestion"]["file_format"],
                
                # Heuristics
                watermark_column=self._guess_watermark(config["dataset_schema"]["columns"]),
                upsert_key=self._guess_primary_key(config["dataset_schema"]["columns"]),
                
                validation_rules=json.dumps({"schema_check": "strict"}), # Placeholder
                sensitive_data_flag=self._check_pii(config["dataset_schema"]["columns"]),
                
                is_active=False # Default False
            )
            
            session.add(new_config)
            session.commit()
            logger.info(f"Registered new dataset {dataset_id} in Master Config Table.")
            
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to register in DB: {e}")
            raise e
        finally:
            session.close()

    def _guess_watermark(self, columns: List[Dict]) -> str:
        # Simple Heuristic: First 'date' column
        for col in columns:
            if col["type"] == "date":
                return col["name"]
        return None

    def _guess_primary_key(self, columns: List[Dict]) -> str:
        # Simple Heuristic: 'id', 'uuid', or first integer
        for col in columns:
            name = col["name"].lower()
            if name == "id" or "uuid" in name:
                return col["name"]
        return None

    def _check_pii(self, columns: List[Dict]) -> bool:
        pii_keywords = ["email", "ssn", "phone", "address", "card"]
        for col in columns:
            name = col["name"].lower()
            if any(k in name for k in pii_keywords):
                return True
        return False
