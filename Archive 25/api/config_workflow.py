from fastapi import APIRouter, File, UploadFile, HTTPException, Depends
from sqlalchemy.orm import Session
from core.database import get_db
from models.master_config import MasterConfig
from tools.config_exporter import export_master_config_to_storage
from loguru import logger
import pandas as pd
from io import BytesIO
from typing import List

router = APIRouter(prefix="/config", tags=["Configuration Workflow"])

EDITABLE_COLUMNS = [
    "load_type", 
    "upsert_key", 
    "watermark_column", 
    "partition_column", 
    "is_active", 
    "priority",
    "frequency"
]

from core.settings import settings
from core.azure_storage import get_storage_client

@router.get("/clients")
def list_clients(db: Session = Depends(get_db)):
    """
    Lists distinct clients from:
    1. Azure Blob Storage (Master_Configuration folder)
    2. DB (MasterConfigAuthoritative table)
    3. DB (APISourceConfig table)
    """
    try:
        from models.api_source_config import APISourceConfig
        from models.master_config_authoritative import MasterConfigAuthoritative
        
        clients = set()
        
        # 1. From Storage
        storage = get_storage_client()
        container = settings.AZURE_CONTAINER_NAME or "datalake"
        prefix = "Master_Configuration/"
        resp = storage.list_objects_v2(Prefix=prefix, Delimiter="/", Container=container)
        for p in resp.get("CommonPrefixes", []):
            folder = p["Prefix"].replace(prefix, "").strip("/")
            if folder: clients.add(folder)
            
        # 2. From DB - Metadata Registry
        db_clients = db.query(MasterConfigAuthoritative.client_name).distinct().all()
        for dc in db_clients:
            if dc[0]: clients.add(dc[0])
            
        # 3. From DB - API Sources
        api_clients = db.query(APISourceConfig.client_name).distinct().all()
        for ac in api_clients:
            if ac[0]: clients.add(ac[0])

        return {"clients": sorted(list(clients))}
        
    except Exception as e:
        logger.error(f"Failed to list clients: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/datasets")
def list_datasets_for_client(client_name: str, dataset_ids: str = None):
    """
    Lists dataset_id and dataset_name for a given client from the Master Configuration CSV in S3.
    Optionally filters by dataset_ids list.
    """
    try:
        storage = get_storage_client()
        container = settings.AZURE_CONTAINER_NAME or "datalake"
        clean_client = client_name.strip().replace(" ", "_")
        key = f"Master_Configuration/{clean_client}/master_config.csv"

        obj = storage.get_object(Key=key, Container=container)
        df = pd.read_csv(BytesIO(obj["Body"].read()))

        if "dataset_id" not in df.columns or "source_object" not in df.columns:
            raise HTTPException(status_code=400, detail="Missing required columns: dataset_id, source_object")

        # Optional Filter
        if dataset_ids:
            target_ids = [tid.strip().lower() for tid in dataset_ids.split(",") if tid.strip()]
            if not df.empty:
                # Check for matches in multiple columns to be robust
                mask = df["dataset_id"].astype(str).str.lower().isin(target_ids)
                if "source_object" in df.columns:
                    mask |= df["source_object"].astype(str).str.lower().isin(target_ids)
                    mask |= df["source_object"].astype(str).str.lower().apply(lambda x: x.rsplit(".", 1)[0] if "." in x else x).isin(target_ids)
                if "source_folder" in df.columns:
                    mask |= df["source_folder"].astype(str).str.lower().isin(target_ids)
                    mask |= df["source_folder"].astype(str).str.lower().apply(lambda x: any(t in [s.strip() for s in x.split(",")] for t in target_ids))
                df = df[mask]


        datasets = []
        for _, row in df.iterrows():
            dsid = str(row.get("dataset_id")).strip() if pd.notna(row.get("dataset_id")) else ""
            src = str(row.get("source_object")).strip() if pd.notna(row.get("source_object")) else ""
            name = src.rsplit(".", 1)[0] if src else ""
            if dsid:
                datasets.append({"dataset_id": dsid, "dataset_name": name})

        return {"client_name": client_name, "datasets": datasets}
    except Exception as e:
        logger.error(f"Failed to list datasets for {client_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/upload")
async def upload_master_config(
    client_name: str, # REQUIRED QUERY PARAM
    file: UploadFile = File(...)
    
    # db: Session = Depends(get_db) 
):
    """
    Human-in-the-Loop Endpoint (Direct S3 Overwrite Mode):
    Allows Data Engineers to upload a modified Master Config Excel/CSV for a SPECIFIC CLIENT.
     DIRECTLY overwrites the specific client's S3 config file (as CSV).
    Bypasses Postgres DB as per user request.
    """
    logger.info(f"Received Master Config upload for client {client_name}: {file.filename}")
    
    try:
        contents = await file.read()
        
        # Parse File
        if file.filename.endswith(".csv"):
            df = pd.read_csv(BytesIO(contents))
        elif file.filename.endswith((".xlsx", ".xls")):
            df = pd.read_excel(BytesIO(contents))
        else:
            raise HTTPException(status_code=400, detail="Invalid file format. Use CSV or Excel.")
            
        # Basic Validation
        if "dataset_id" not in df.columns:
            raise HTTPException(status_code=400, detail="Missing required column: dataset_id")
            
        # DIRECT SAVE TO Azure Blob Storage (CSV format for MasterConfigManager compatibility)
        storage = get_storage_client()
        container = settings.AZURE_CONTAINER_NAME or "datalake"

        clean_client = client_name.strip().replace(" ", "_")
        key = f"Master_Configuration/{clean_client}/master_config.csv"

        csv_bytes = df.to_csv(index=False, encoding="utf-8").encode("utf-8")
        storage.put_object(Container=container, Key=key, Body=csv_bytes, ContentType="text/csv")

        logger.info(f"Successfully overwrote Master Config at az://{container}/{key}")

        return {
            "status": "SUCCESS",
            "message": f"Updated Master Config for {client_name} directly in Azure Blob Storage.",
            "az_path": f"az://{container}/{key}"
        }
        
    except Exception as e:
        logger.error(f"Config Upload Failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/clients/{client_name}")
def delete_client(client_name: str, db: Session = Depends(get_db)):
    """
    Deletes a client and all associated data across ALL layers and registry tables.
    """
    try:
        from models.api_source_config import APISourceConfig
        from models.master_config_authoritative import MasterConfigAuthoritative
        from models.dq_schema_config import DQSchemaConfig
        
        # Consistent cleaning of client name to match storage keys
        clean_client = client_name.strip().replace(" ", "_")
        
        # 1. DB Cleanup - Registry & Rules
        # Match by both raw and cleaned name to be safe
        client_filter = [client_name, clean_client]
        
        db.query(APISourceConfig).filter(APISourceConfig.client_name.in_(client_filter)).delete(synchronize_session=False)
        
        mcs = db.query(MasterConfigAuthoritative).filter(MasterConfigAuthoritative.client_name.in_(client_filter)).all()
        ids = [m.dataset_id for m in mcs]
        if ids:
            db.query(DQSchemaConfig).filter(DQSchemaConfig.dataset_id.in_(ids)).delete(synchronize_session=False)
            db.query(MasterConfigAuthoritative).filter(MasterConfigAuthoritative.client_name.in_(client_filter)).delete(synchronize_session=False)
        
        db.commit()
        
        # 2. Azure Storage Cleanup (All layers)
        storage = get_storage_client()
        container = settings.AZURE_CONTAINER_NAME or "datalake"
        
        # Prefixes to scrub
        layers = ["Master_Configuration", "Raw", "Bronze", "Silver", "Reports"]
        
        for layer in layers:
            prefix = f"{layer}/{clean_client}"
            # Use recursive directory delete for ADLS Gen2 speed and reliability
            storage.delete_directory(Prefix=prefix, Container=container)
            
        logger.info(f"Scrubbed all configurations and storage for client: {client_name}")
        return {"status": "SUCCESS", "message": f"Client '{client_name}' completely scrubbed."}
        
    except Exception as e:
        logger.error(f"Failed to delete client {client_name}: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
