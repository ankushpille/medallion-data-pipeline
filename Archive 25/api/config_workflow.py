from fastapi import APIRouter, File, UploadFile, HTTPException, Depends, Form
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
import json
import re
import zipfile

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


def _flatten_fabric_activities(activities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    flat: List[Dict[str, Any]] = []
    for activity in activities or []:
        if not isinstance(activity, dict):
            continue
        flat.append(activity)
        type_props = activity.get("typeProperties") or {}
        flat.extend(_flatten_fabric_activities(type_props.get("activities") or []))
        flat.extend(_flatten_fabric_activities(type_props.get("ifTrueActivities") or []))
        flat.extend(_flatten_fabric_activities(type_props.get("ifFalseActivities") or []))
    return flat


def _fabric_expr_to_string(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        raw = value.get("value")
        if isinstance(raw, str):
            return raw.strip()
    return ""


def _safe_slug(value: str, default: str = "dataset") -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value or "").strip()).strip("._-")
    return cleaned or default


def _infer_fabric_source_type(activities: List[Dict[str, Any]]) -> str:
    activity_types = {str(a.get("type", "")).lower() for a in activities}
    if "webactivity" in activity_types:
        return "API"
    if "copy" in activity_types:
        for activity in activities:
            if str(activity.get("type", "")).lower() != "copy":
                continue
            source_type = str((((activity.get("typeProperties") or {}).get("source") or {}).get("type")) or "").lower()
            if "warehouse" in source_type or "sql" in source_type:
                return "DATABASE"
    return "LOCAL"


def _extract_fabric_pipeline_intelligence(client_name: str, payload: Dict[str, Any], artifact_name: str) -> Dict[str, Any]:
    resources = payload.get("resources") or []
    pipeline_resource = next(
        (
            resource
            for resource in resources
            if str(resource.get("type", "")).lower() == "pipelines"
        ),
        {},
    )
    pipeline_name = pipeline_resource.get("name") or artifact_name or "fabric_pipeline"
    properties = pipeline_resource.get("properties") or {}
    root_activities = properties.get("activities") or []
    flat_activities = _flatten_fabric_activities(root_activities)
    source_type = _infer_fabric_source_type(flat_activities)

    lookups = [a for a in flat_activities if str(a.get("type", "")).lower() == "lookup"]
    web_activities = [a for a in flat_activities if str(a.get("type", "")).lower() == "webactivity"]
    copy_activities = [a for a in flat_activities if str(a.get("type", "")).lower() == "copy"]
    notebook_activities = [a for a in flat_activities if str(a.get("type", "")).lower() == "tridentnotebook"]
    script_activities = [a for a in flat_activities if str(a.get("type", "")).lower() == "script"]
    email_activities = [a for a in flat_activities if "email" in str(a.get("type", "")).lower()]

    config_table = ""
    config_query = ""
    warehouse_name = ""
    for lookup in lookups:
        type_props = lookup.get("typeProperties") or {}
        source = type_props.get("source") or {}
        query = _fabric_expr_to_string(source.get("sqlReaderQuery"))
        dataset = type_props.get("datasetSettings") or {}
        table_props = dataset.get("typeProperties") or {}
        schema = table_props.get("schema")
        table = table_props.get("table")
        if schema and table and not config_table:
            config_table = f"{schema}.{table}"
        if query and not config_query:
            config_query = query
        linked_service = dataset.get("linkedService") or {}
        if linked_service.get("name") and not warehouse_name:
            warehouse_name = str(linked_service.get("name"))

    endpoint_expressions: List[str] = []
    generated_rows: List[Dict[str, Any]] = []
    for activity in web_activities:
        type_props = activity.get("typeProperties") or {}
        endpoint = _fabric_expr_to_string(type_props.get("url")) or _fabric_expr_to_string(type_props.get("relativeUrl"))
        if endpoint:
            endpoint_expressions.append(endpoint)
        activity_name = str(activity.get("name") or "web_activity")
        dataset_stub = _safe_slug(activity_name, default="api_activity")
        generated_rows.append({
            "client_name": client_name,
            "source_type": "API",
            "source_folder": endpoint or config_table or pipeline_name,
            "source_object": f"{dataset_stub}.json",
            "file_format": "JSON",
            "raw_layer_path": endpoint or f"fabric://{pipeline_name}/{activity_name}",
            "target_layer_bronze": f"Bronze/{client_name}/{dataset_stub}",
            "target_layer_silver": f"Silver/{client_name}/{dataset_stub}",
            "is_active": True,
            "created_at": datetime.utcnow().isoformat(),
            "load_type": "full",
            "upsert_key": None,
            "watermark_column": None,
            "partition_column": None,
        })

    raw_output_folders: List[str] = []
    output_name_expressions: List[str] = []
    for activity in copy_activities:
        sink = ((activity.get("typeProperties") or {}).get("sink") or {})
        dataset = sink.get("datasetSettings") or {}
        location = (dataset.get("typeProperties") or {}).get("location") or {}
        folder_path = _fabric_expr_to_string(location.get("folderPath"))
        file_name = _fabric_expr_to_string(location.get("fileName"))
        if folder_path:
            raw_output_folders.append(folder_path)
        if file_name:
            output_name_expressions.append(file_name)

    notebook_parameters: Dict[str, str] = {}
    for activity in notebook_activities:
        parameters = ((activity.get("typeProperties") or {}).get("parameters") or {})
        for key, value in parameters.items():
            notebook_parameters[str(key)] = _fabric_expr_to_string((value or {}).get("value"))

    file_types = ["JSON"] if web_activities or output_name_expressions else ["UNKNOWN"]
    source_path = endpoint_expressions[0] if endpoint_expressions else (config_table or pipeline_name)
    warnings: List[str] = []
    if source_type == "API" and endpoint_expressions and any(expr.startswith("@") for expr in endpoint_expressions):
        warnings.append(
            "Fabric export uses dynamic API endpoint expressions. Generated API config is a template until the backing configuration table values are provided."
        )
    if config_table:
        warnings.append(
            f"Dataset-level config is metadata-driven from {config_table}; concrete table names are not embedded in the ZIP export."
        )

    discovered_assets = [
        {
            "type": "fabric_pipeline",
            "name": pipeline_name,
            "configuration": {
                "artifact_name": artifact_name,
                "lookup_count": len(lookups),
                "web_activity_count": len(web_activities),
                "copy_count": len(copy_activities),
                "notebook_count": len(notebook_activities),
                "script_count": len(script_activities),
                "email_count": len(email_activities),
            },
        }
    ]
    if config_table:
        discovered_assets.append(
            {
                "type": "warehouse_table",
                "name": config_table,
                "configuration": {
                    "warehouse": warehouse_name or None,
                    "query": config_query or None,
                },
            }
        )
    for endpoint in endpoint_expressions:
        discovered_assets.append(
            {
                "type": "api",
                "name": endpoint,
                "configuration": {
                    "endpoint_expression": endpoint,
                    "method": "GET",
                },
            }
        )

    return {
        "framework": "Microsoft Fabric",
        "scan_status": "success",
        "auth_mode": "artifact_import",
        "is_fallback": False,
        "warnings": warnings,
        "errors": [],
        "source_systems": [
            {
                "type": source_type,
                "name": "Fabric Imported Pipeline",
                "configuration": {
                    "pipeline_name": pipeline_name,
                    "config_table": config_table or None,
                    "endpoint_expressions": endpoint_expressions or None,
                },
            }
        ],
        "discovered_assets": discovered_assets,
        "data_pipelines": [
            {
                "name": pipeline_name,
                "framework": "Microsoft Fabric",
                "activity_count": len(flat_activities),
                "artifact": artifact_name,
            }
        ],
        "ingestion_support": {
            "file_based": bool(copy_activities),
            "api": bool(web_activities),
            "database": bool(lookups or script_activities),
            "streaming": False,
            "batch": bool(flat_activities),
        },
        "ingestion_types": [
            label
            for label, present in [
                ("fabric_pipeline", True),
                ("metadata_driven", bool(config_table)),
                ("api_ingestion", bool(web_activities)),
                ("file_landing", bool(copy_activities)),
                ("notebook_transform", bool(notebook_activities)),
                ("warehouse_logging", bool(script_activities)),
            ]
            if present
        ],
        "file_types": file_types,
        "delimiter_config": {},
        "dq_rules": {},
        "pipeline_capabilities": {
            "metadata_driven": bool(config_table),
            "foreach_loop": any(str(a.get("type", "")).lower() == "foreach" for a in flat_activities),
            "conditional_branching": any(str(a.get("type", "")).lower() == "ifcondition" for a in flat_activities),
            "notebook_execution": bool(notebook_activities),
            "warehouse_queries": bool(lookups or script_activities),
            "notifications": bool(email_activities),
            "scan_mode": "artifact_import",
        },
        "ingestion_details": {
            "source_type": source_type,
            "source_path": source_path,
            "config_table": config_table or None,
            "pipeline_name": pipeline_name,
        },
        "original_config": payload,
        "reformatted_config": {
            "client_name": client_name,
            "source_type": source_type,
            "source_path": source_path,
            "framework": "Microsoft Fabric",
            "pipeline_name": pipeline_name,
            "config_table": config_table or None,
            "warehouse_name": warehouse_name or None,
            "config_query": config_query or None,
            "api_endpoint_expressions": endpoint_expressions or None,
            "raw_output_folders": raw_output_folders or None,
            "output_name_expressions": output_name_expressions or None,
            "notebook_parameters": notebook_parameters or None,
            "generated_rows": generated_rows,
        },
    }


def _read_fabric_zip(file_bytes: bytes, client_name: str) -> Dict[str, Any]:
    try:
        archive = zipfile.ZipFile(BytesIO(file_bytes))
    except zipfile.BadZipFile as exc:
        raise HTTPException(status_code=400, detail=f"Invalid ZIP file: {exc}") from exc

    manifest_payload: Dict[str, Any] = {}
    artifact_payload: Dict[str, Any] = {}
    artifact_name = ""

    with archive:
        for entry_name in archive.namelist():
            if entry_name.lower().endswith("manifest.json"):
                with archive.open(entry_name) as handle:
                    manifest_payload = json.load(handle)
            elif entry_name.lower().endswith(".json"):
                with archive.open(entry_name) as handle:
                    candidate = json.load(handle)
                if isinstance(candidate, dict) and isinstance(candidate.get("resources"), list):
                    artifact_payload = candidate
                    artifact_name = entry_name.rsplit("/", 1)[-1]
                    break

    if not artifact_payload:
        raise HTTPException(status_code=400, detail="ZIP does not contain a Fabric pipeline JSON artifact.")

    intelligence = _extract_fabric_pipeline_intelligence(client_name, artifact_payload, artifact_name)
    if manifest_payload:
        intelligence.setdefault("original_config", {})
        intelligence["original_config"] = {
            "manifest": manifest_payload,
            "artifact": artifact_payload,
        }
    return intelligence


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

    generated_rows = reformatted.get("generated_rows")
    if isinstance(generated_rows, list) and generated_rows:
        for row in generated_rows:
            if not isinstance(row, dict):
                continue
            normalized = dict(row)
            normalized["client_name"] = normalized.get("client_name") or client_name
            normalized["source_type"] = (normalized.get("source_type") or src_type).upper()
            normalized["source_folder"] = normalized.get("source_folder") or src_path
            normalized["source_object"] = normalized.get("source_object") or "detected_source"
            normalized["file_format"] = (normalized.get("file_format") or _safe_ext_from_key(normalized["source_object"])).upper()
            normalized["raw_layer_path"] = normalized.get("raw_layer_path") or normalized["source_folder"]
            normalized["target_layer_bronze"] = normalized.get("target_layer_bronze") or f"Bronze/{client_name}/{normalized['source_object'].rsplit('.', 1)[0]}"
            normalized["target_layer_silver"] = normalized.get("target_layer_silver") or f"Silver/{client_name}/{normalized['source_object'].rsplit('.', 1)[0]}"
            normalized["is_active"] = True if normalized.get("is_active") is None else bool(normalized.get("is_active"))
            normalized["created_at"] = normalized.get("created_at") or datetime.utcnow().isoformat()
            normalized["load_type"] = normalized.get("load_type") or "full"
            normalized.setdefault("upsert_key", None)
            normalized.setdefault("watermark_column", None)
            normalized.setdefault("partition_column", None)
            normalized["dataset_id"] = normalized.get("dataset_id") or generate_dataset_id(
                normalized["client_name"],
                normalized["source_type"],
                normalized["raw_layer_path"],
            )
            normalized["pipeline_id"] = normalized.get("pipeline_id") or str(uuid.uuid4())
            rows.append(normalized)
        return list({row["dataset_id"]: row for row in rows}.values())

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


def _persist_generated_rows(
    client_name: str,
    intelligence: Dict[str, Any],
    rows: List[Dict[str, Any]],
    db: Session,
    source_path: Optional[str] = None,
) -> Dict[str, Any]:
    validation_payload = {
        "delimiter_config": intelligence.get("delimiter_config") or {},
        "dq_rules": intelligence.get("dq_rules") or {},
        "ingestion_type": intelligence.get("ingestion_types") or [],
        "pipeline_capabilities": intelligence.get("pipeline_capabilities") or {},
        "source_path": source_path or intelligence.get("ingestion_details", {}).get("source_path"),
        "framework": intelligence.get("framework"),
        "scan_status": intelligence.get("scan_status"),
        "auth_mode": intelligence.get("auth_mode"),
        "is_fallback": bool(intelligence.get("is_fallback")),
    }

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
        bucket, prefix = _parse_s3_path(source_path or rows[0].get("source_folder") or "")
        if bucket:
            existing_source = db.query(APISourceConfig).filter(
                APISourceConfig.client_name == client_name,
                APISourceConfig.source_type == "S3",
                APISourceConfig.aws_bucket_name == bucket,
            ).first()
            if not existing_source:
                existing_source = APISourceConfig(
                    client_name=client_name,
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
            logger.info("Saved S3 source registry metadata. client={} bucket={} prefix={}", client_name, bucket, prefix)

    from core.master_config_manager import MasterConfigManager, MASTER_CONFIG_COLUMNS
    mgr = MasterConfigManager()
    key = mgr._get_config_key(client_name)
    df = pd.DataFrame(rows)
    for col in MASTER_CONFIG_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[MASTER_CONFIG_COLUMNS]
    mgr._save_config(df, key)

    logger.info("Saved {} intelligence config rows for client={}", len(rows), client_name)
    return {
        "status": "SUCCESS",
        "client_name": client_name,
        "rows_inserted": len(rows),
        "dataset_ids": [row["dataset_id"] for row in rows],
        "location": f"az://{mgr.bucket_name}/{key}",
    }


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

    try:
        return _persist_generated_rows(
            client_name=request.client_name,
            intelligence=intelligence,
            rows=rows,
            db=db,
            source_path=request.source_path,
        )
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to save intelligence config for {request.client_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/import-fabric")
async def import_fabric_config(
    client_name: str = Form(...),
    file: UploadFile = File(..., description="Microsoft Fabric exported pipeline ZIP"),
    persist: bool = Form(False),
    db: Session = Depends(get_db),
):
    if not client_name:
        raise HTTPException(status_code=400, detail="client_name is required")
    if not file.filename or not file.filename.lower().endswith(".zip"):
        raise HTTPException(status_code=400, detail="Upload a Microsoft Fabric .zip export file.")

    try:
        content = await file.read()
        intelligence = _read_fabric_zip(content, client_name)
        rows = _rows_from_intelligence(
            client_name=client_name,
            intelligence=intelligence,
            source_type=intelligence.get("reformatted_config", {}).get("source_type"),
            source_path=intelligence.get("reformatted_config", {}).get("source_path"),
        )
        response: Dict[str, Any] = {
            "status": "SUCCESS",
            "client_name": client_name,
            "artifact_name": file.filename,
            "framework": intelligence.get("framework"),
            "source_type": intelligence.get("reformatted_config", {}).get("source_type"),
            "generated_rows": rows,
            "row_count": len(rows),
            "intelligence_data": intelligence,
        }
        if persist:
            response["persist_result"] = _persist_generated_rows(
                client_name=client_name,
                intelligence=intelligence,
                rows=rows,
                db=db,
                source_path=intelligence.get("reformatted_config", {}).get("source_path"),
            )
        return response
    except HTTPException:
        raise
    except Exception as exc:
        db.rollback()
        logger.error(f"Failed to import Fabric config for {client_name}: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))

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
