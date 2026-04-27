import os
import io
from typing import List, Dict, Tuple
from azure.storage.blob import BlobServiceClient
from loguru import logger
from core.azure_storage import get_storage_client
from core.settings import settings
import pandas as pd
from datetime import datetime
import hashlib

class ADLSConnector:
    def __init__(self, connection_string: str, container_name: str, dest_container: str = None):
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string) if connection_string else None
        self.container_client = self.blob_service_client.get_container_client(container_name) if self.blob_service_client else None
        self.dest_storage = get_storage_client()
        self.dest_container = dest_container or settings.AZURE_CONTAINER_NAME or "datalake"

    def list_files(self, folder_path: str) -> List[str]:
        """List all files in ADLS folder"""
        blob_list = self.container_client.list_blobs(name_starts_with=folder_path)
        return [blob.name for blob in blob_list]

    def validate_file(self, blob_name: str) -> Tuple[bool, List[str], io.BytesIO]:
        """
        Validate file existence, size, format, and structure.
        Returns: (is_valid, failed_rules, file_content_stream)
        """
        failed_rules = []
        blob_client = self.container_client.get_blob_client(blob_name)
        
        # 1. Check existence and basic read
        try:
            stream = io.BytesIO()
            blob_client.download_blob().readinto(stream)
            stream.seek(0)
            file_size = stream.getbuffer().nbytes
        except Exception as e:
            return False, ["File not readable"], None

        # 2. Empty Check
        if file_size == 0:
            failed_rules.append("File is empty")
        
        # 3. Format Check
        ext = blob_name.split('.')[-1].lower()
        if ext not in ['csv', 'json', 'parquet']:
            failed_rules.append(f"Unsupported format: {ext}")
        
        # 4. Structural Validation (Header check for CSV)
        if ext == 'csv' and file_size > 0:
            try:
                # Read first few lines to check parseability
                pd.read_csv(stream, nrows=5)
                stream.seek(0) # Reset pointer
            except Exception:
                failed_rules.append("CSV Parsing Failed")

        is_valid = len(failed_rules) == 0
        return is_valid, failed_rules, stream

    def promote_to_raw(self, stream: io.BytesIO, client_name: str, batch_id: str, source_object: str):
        """
        Write PASS datasets to Azure Raw Layer (Immutable)
        Structure: Raw/<client>/<batch>/<source_folder>/<filename>
        """
        key = f"Raw/{client_name}/{batch_id}/{source_object}"
        data = stream.read()
        self.dest_storage.put_object(Container=self.dest_container, Key=key, Body=data)
        return f"az://{self.dest_container}/{key}"

def generate_dataset_id(client_name: str, source_path: str, filename: str) -> str:
    """Generate idempotent hash for dataset identity"""
    raw_str = f"{client_name}{source_path}{filename}"
    return hashlib.md5(raw_str.encode()).hexdigest()
