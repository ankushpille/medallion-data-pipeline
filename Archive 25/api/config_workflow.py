from fastapi import APIRouter, File, UploadFile, HTTPException, Depends
from sqlalchemy.orm import Session
from core.database import get_db
from models.master_config import MasterConfig
from models.master_config_authoritative import MasterConfigAuthoritative
from tools.config_exporter import export_master_config_to_storage
from loguru import logger
import pandas as pd
from io import BytesIO
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
from datetime import datetime
import uuid

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

class IntelligenceConfigSaveRequest(BaseModel):
    client_name: str
    intelligence_data: Dict[str, Any]
    source_type: Optional[str] = None
    source_path: Optional[str] = None


def _safe_ext_from_key(key: str) -> str:
    if not key or "." not in key:
        return "UNKNOWN"
    return key.rsplit(".", 1)[-1].upper()


def _source_object_from_path(path: str) -> str:
    clean = (path or "").rstrip("/")
    if not clean:
        return "detected_source"
    return clean.rsplit("/", 1)[-1] or "detected_source"


def _join_source_path(source_path: str, item: str) -> str:
    if str(item).startswith(("s3://", "az://")):
        return item
    if source_path.startswith("s3://"):
        rest = source_path.split("s3://", 1)[1]
        bucket = rest.split("/", 1)[0]
        return f"s3://{bucket}/{str(item).lstrip('/')}"
    if source_path.startswith("az://"):
        rest = source_path.split("az://", 1)[1]
        parts = rest.split("/", 2)
        root = "/".join(parts[:2]) if len(parts) >= 2 else rest
        return f"az://{root}/{str(item).lstrip('/')}"
    return f"{source_path.rstrip('/')}/{str(item).lstrip('/')}".strip("/")


def _parse_s3_path(path: str) -> tuple[str, str]:
    if not path or not path.startswith("s3://"):
        return "", ""
    rest = path.split("s3://", 1)[1]
    if "/" not in rest:
        return rest, ""
    bucket, prefix = rest.split("/", 1)
    return bucket, prefix


def _region_from_intelligence(intelligence: Dict[str, Any]) -> str:
    for asset in intelligence.get("discovered_assets") or []:
        config = asset.get("configuration") or {}
        if asset.get("type") == "s3" and config.get("Region"):
            return config["Region"]
    return "us-east-1"


def _rows_from_intelligence(client_name: str, intelligence: Dict[str, Any], source_type: Optional[str], source_path: Optional[str]) -> List[Dict[str, Any]]:
    from core.utils import generate_dataset_id

    reformatted = intelligence.get("reformatted_config") or {}
    details = intelligence.get("ingestion_details") or {}
    discovered_assets = intelligence.get("discovered_assets") or []
    file_types = intelligence.get("file_types") or reformatted.get("file_types") or ["UNKNOWN"]

    src_type = (source_type or details.get("source_type") or reformatted.get("source_type") or "S3").upper()
    src_path = source_path or details.get("source_path") or reformatted.get("source_path") or ""
    rows: List[Dict[str, Any]] = []

    sample_keys: List[str] = []
    for asset in discovered_assets:
        config = asset.get("configuration") or {}
        if isinstance(config.get("SampleObjectKeys"), list):
            sample_keys.extend([k for k in config["SampleObjectKeys"] if k])

    source_items = sample_keys[:25] or [src_path or f"{src_type.lower()}://detected_source"]
    for item in source_items:
        item_path = _join_source_path(src_path, str(item))
        source_object = _source_object_from_path(item)
        file_format = _safe_ext_from_key(source_object)
        if file_format == "UNKNOWN" and file_types:
            file_format = str(file_types[0]).upper()
        dataset_id = generate_dataset_id(client_name, src_type, item_path)
        rows.append({
            "dataset_id": dataset_id,
            "pipeline_id": str(uuid.uuid4()),
            "client_name": client_name,
            "source_type": src_type,
            "source_folder": src_path,
            "source_object": source_object,
            "file_format": file_format,
            "raw_layer_path": item_path,
            "target_layer_bronze": f"Bronze/{client_name}/{source_object.rsplit('.', 1)[0] if '.' in source_object else source_object}",
            "target_layer_silver": f"Silver/{client_name}/{source_object.rsplit('.', 1)[0] if '.' in source_object else source_object}",
            "is_active": True,
            "created_at": datetime.utcnow().isoformat(),
            "load_type": "full",
            "upsert_key": None,
            "watermark_column": None,
            "partition_column": None,
        })

    # De-dupe in case multiple sample keys collapse to the same dataset id.
    return list({row["dataset_id"]: row for row in rows}.values())


@router.post("/save")
def save_generated_config(request: IntelligenceConfigSaveRequest, db: Session = Depends(get_db)):
    """
    Persists Pipeline Intelligence output into the authoritative DB registry,
    legacy master_configuration table, and the master config CSV consumed by Step 4.
    """
    if not request.client_name:
        raise HTTPException(status_code=400, detail="client_name is required")
    if not request.intelligence_data:
        raise HTTPException(status_code=400, detail="intelligence_data is required")

    intelligence = request.intelligence_data
    logger.info(
        "Saving intelligence config. client={} scan_status={} auth_mode={} is_fallback={}",
        request.client_name,
        intelligence.get("scan_status"),
        intelligence.get("auth_mode"),
        intelligence.get("is_fallback"),
    )

    rows = _rows_from_intelligence(request.client_name, intelligence, request.source_type, request.source_path)
    if not rows:
        raise HTTPException(status_code=400, detail="No config rows could be generated from intelligence data")

    validation_payload = {
        "delimiter_config": intelligence.get("delimiter_config") or {},
        "dq_rules": intelligence.get("dq_rules") or {},
        "ingestion_type": intelligence.get("ingestion_types") or [],
        "pipeline_capabilities": intelligence.get("pipeline_capabilities") or {},
        "source_path": request.source_path or intelligence.get("ingestion_details", {}).get("source_path"),
        "scan_status": intelligence.get("scan_status"),
        "auth_mode": intelligence.get("auth_mode"),
        "is_fallback": bool(intelligence.get("is_fallback")),
    }

    try:
        for row in rows:
            authoritative = db.query(MasterConfigAuthoritative).filter(MasterConfigAuthoritative.dataset_id == row["dataset_id"]).first()
            if not authoritative:
                authoritative = MasterConfigAuthoritative(dataset_id=row["dataset_id"])
                db.add(authoritative)

            for field in [
                "pipeline_id", "client_name", "source_type", "source_folder", "source_object", "file_format",
                "raw_layer_path", "target_layer_bronze", "target_layer_silver", "load_type",
                "upsert_key", "watermark_column", "partition_column"
            ]:
                setattr(authoritative, field, row.get(field))
            authoritative.is_active = True
            authoritative.updated_at = datetime.utcnow()

            legacy = db.query(MasterConfig).filter(MasterConfig.dataset_id == row["dataset_id"]).first()
            if not legacy:
                legacy = MasterConfig(dataset_id=row["dataset_id"])
                db.add(legacy)
            legacy.pipeline_id = row["pipeline_id"]
            legacy.client_name = row["client_name"]
            legacy.source_system = row["source_type"]
            legacy.source_schema = row["source_folder"]
            legacy.source_object = row["source_object"]
            legacy.file_format = row["file_format"]
            legacy.load_type = row["load_type"]
            legacy.is_active = True
            legacy.validation_rules = validation_payload
            legacy.updated_at = datetime.utcnow()

        db.commit()

        # Persist non-secret S3 source registry metadata so S3Connector can resolve
        # client + bucket during orchestration. Credentials stay in memory only.
        if rows and rows[0].get("source_type") == "S3":
            from models.api_source_config import APISourceConfig
            bucket, prefix = _parse_s3_path(request.source_path or rows[0].get("source_folder") or "")
            if bucket:
                existing_source = db.query(APISourceConfig).filter(
                    APISourceConfig.client_name == request.client_name,
                    APISourceConfig.source_type == "S3",
                    APISourceConfig.aws_bucket_name == bucket,
                ).first()
                if not existing_source:
                    existing_source = APISourceConfig(
                        client_name=request.client_name,
                        source_name=f"{bucket}-framework-scan",
                        source_type="S3",
                        auth_type="transient",
                        aws_bucket_name=bucket,
                        aws_region=_region_from_intelligence(intelligence),
                        endpoints=prefix,
                        is_active=True,
                    )
                    db.add(existing_source)
                else:
                    existing_source.auth_type = "transient"
                    existing_source.aws_region = existing_source.aws_region or _region_from_intelligence(intelligence)
                    existing_source.endpoints = prefix or existing_source.endpoints
                    existing_source.is_active = True
                db.commit()
                logger.info("Saved S3 source registry metadata. client={} bucket={} prefix={}", request.client_name, bucket, prefix)

        from core.master_config_manager import MasterConfigManager, MASTER_CONFIG_COLUMNS
        mgr = MasterConfigManager()
        key = mgr._get_config_key(request.client_name)
        df = pd.DataFrame(rows)
        for col in MASTER_CONFIG_COLUMNS:
            if col not in df.columns:
                df[col] = None
        df = df[MASTER_CONFIG_COLUMNS]
        mgr._save_config(df, key)

        logger.info("Saved {} intelligence config rows for client={}", len(rows), request.client_name)
        return {
            "status": "SUCCESS",
            "client_name": request.client_name,
            "rows_inserted": len(rows),
            "dataset_ids": [row["dataset_id"] for row in rows],
            "location": f"az://{mgr.bucket_name}/{key}",
        }
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to save intelligence config for {request.client_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

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
