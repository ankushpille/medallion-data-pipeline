from datetime import datetime
from sqlalchemy.orm import Session
from core.adls_connector import ADLSConnector, generate_dataset_id
from models.metadata import IngestionMetadata, ConfigurationMetadata
from core.database import SessionLocal
from loguru import logger
from core.azure_storage import get_storage_client
from core.settings import settings
import pandas as pd
import io

class IngestionService:
    def __init__(self, db: Session, container: str = None):
        self.db = db
        self.container = container or settings.AZURE_CONTAINER_NAME or "datalake"
        self.storage = get_storage_client()
        adls_conn_str = settings.AZURE_STORAGE_CONNECTION_STRING or ""
        self.adls = ADLSConnector(adls_conn_str, "client-data", self.container)

    def run_ingestion(self, client_name: str, source_folder: str):
        batch_id = datetime.now().strftime("%b-%d-%H")
        logger.info(f"Starting Ingestion Batch: {batch_id} for {client_name}")

        files = self.adls.list_files(source_folder)
        failed_datasets = []

        for file_path in files:
            filename = file_path.split('/')[-1]
            
            # 1. Validate
            is_valid, failed_rules, stream = self.adls.validate_file(file_path)
            
            raw_path = None
            status = "FAIL"
            
            # 2. Promote to Raw (If Valid)
            if is_valid:
                status = "PASS"
                raw_path = self.adls.promote_to_raw(stream, client_name, batch_id, file_path)
                
                # 3. Update Master Config (Idempotent)
                self._update_master_config(client_name, source_folder, filename, file_path, batch_id)
            else:
                failed_datasets.append({"file": filename, "rules": failed_rules})

            # 4. Log to Ingestion Metadata
            self._log_ingestion(
                client_name, source_folder, filename, batch_id, 
                status, failed_rules, raw_path
            )

        # 5. Generate & Upload Master Excel
        self._generate_master_excel(client_name)
        
        return {
            "batch_id": batch_id,
            "total_files": len(files),
            "failed_files": len(failed_datasets),
            "failures": failed_datasets
        }

    def _log_ingestion(self, client, folder, filename, batch, status, rules, path):
        record = IngestionMetadata(
            client_name=client,
            source_adls_path=folder,
            source_object=filename,
            batch_id=batch,
            validation_status=status,
            failed_validation_rules=rules,
            raw_storage_path=path,
            job_status="COMPLETED"
        )
        self.db.add(record)
        self.db.commit()

    def _update_master_config(self, client, folder, filename, full_path, batch_id):
        ds_id = generate_dataset_id(client, folder, filename)
        
        existing = self.db.query(ConfigurationMetadata).filter_by(dataset_id=ds_id).first()
        
        if not existing:
            # INSERT NEW
            new_config = ConfigurationMetadata(
                dataset_id=ds_id,
                client_name=client,
                source_system=folder,
                source_object=filename,
                file_format=filename.split('.')[-1],
                target_layer_bronze=f"az://{self.container}/bronze/{client}/",
                target_layer_silver=f"az://{self.container}/silver/{client}/",
                latest_batch_id=batch_id,
                is_active=False # Default OFF
            )
            self.db.add(new_config)
            self.db.commit()
        else:
            # JUST UPDATE SYSTEM FIELDS
            existing.latest_batch_id = batch_id
            self.db.commit()

    def _generate_master_excel(self, client_name: str):
        """
        Dump ConfigurationMetadata table to Excel and upload to S3
        """
        key = f"config/{client_name}/master_config.xlsx"
        
        # Query all configs for this client
        configs = self.db.query(ConfigurationMetadata).filter_by(client_name=client_name).all()
        
        # Convert to DataFrame
        data = [c.__dict__ for c in configs]
        if not data:
            return

        # Alchemy adds an internal state object, remove it
        for d in data:
            d.pop('_sa_instance_state', None)

        df = pd.DataFrame(data)
        
        # Order columns nicely
        cols = ['dataset_id', 'source_object', 'is_active', 'load_type', 'upsert_key', 'watermark_column']
        # Add remaining columns
        cols += [c for c in df.columns if c not in cols]
        df = df[cols]

        # Write to Buffer
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
        output.seek(0)
        
        # Upload to S3
        output.seek(0)
        self.storage.upload_fileobj(output, Container=self.container, Key=key)
        logger.info(f"Master Config uploaded to s3://{self.s3_bucket}/{key}")
