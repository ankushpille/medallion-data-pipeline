from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Depends
from fastapi.responses import JSONResponse
from typing import List
from sqlalchemy.orm import Session
from core.database import get_db
from core.azure_storage import get_storage_client
from core.settings import settings
from models.master_config_authoritative import MasterConfigAuthoritative
from models.dq_schema_config import DQSchemaConfig, ExpectedDataType
from loguru import logger
from datetime import datetime
import hashlib, io, uuid
import pandas as pd

router = APIRouter(prefix="/upload", tags=["Local File Upload"])

ALLOWED = {".csv", ".json", ".parquet", ".xlsx", ".xls", ".tsv"}


def _infer_cols(content: bytes, filename: str) -> list:
    try:
        fn = filename.lower()
        if fn.endswith(".csv"):   df = pd.read_csv(io.BytesIO(content), nrows=50, on_bad_lines="skip")
        elif fn.endswith(".tsv"): df = pd.read_csv(io.BytesIO(content), nrows=50, sep="\t")
        elif fn.endswith(".json"):df = pd.read_json(io.BytesIO(content))
        elif fn.endswith((".xlsx",".xls")): df = pd.read_excel(io.BytesIO(content), nrows=50)
        elif fn.endswith(".parquet"):       df = pd.read_parquet(io.BytesIO(content))
        else: return []
        cols = []
        for c in df.columns:
            d = str(df[c].dtype)
            t = "INTEGER" if "int" in d else "FLOAT" if "float" in d else "BOOLEAN" if "bool" in d else "DATE" if "date" in d or "time" in d else "STRING"
            cols.append({"name": str(c), "inferred_type": t})
        return cols
    except Exception as e:
        logger.warning(f"Column inference failed for {filename}: {e}")
        return []


def _dsid(client: str, filename: str) -> str:
    return hashlib.sha256(f"{client}:{filename}".encode()).hexdigest()


def _write_master_config(client: str, records: list, storage, container: str):
    from core.master_config_manager import MASTER_CONFIG_COLUMNS
    key = f"Master_Configuration/{client.strip().replace(' ','_')}/master_config.csv"
    try:
        obj = storage.get_object(Key=key, Container=container)
        ex  = pd.read_csv(io.BytesIO(obj["Body"].read()))
    except Exception:
        ex = pd.DataFrame(columns=MASTER_CONFIG_COLUMNS)
    ndf = pd.DataFrame(records)
    for col in MASTER_CONFIG_COLUMNS:
        if col not in ndf.columns: ndf[col] = None
    ndf = ndf[[c for c in MASTER_CONFIG_COLUMNS if c in ndf.columns]]
    if not ex.empty and "dataset_id" in ex.columns:
        ex = ex[~ex["dataset_id"].isin(ndf["dataset_id"].tolist())]
        merged = pd.concat([ex, ndf], ignore_index=True)
    else:
        merged = ndf
    storage.put_object(Container=container, Key=key, Body=merged.to_csv(index=False).encode("utf-8"))
    logger.info(f"master_config.csv written → {key} ({len(merged)} rows)")


@router.post("/ingest")
async def upload_ingest(
    client_name: str              = Form(..., description="Client name e.g. AMGEN"),
    files:       List[UploadFile] = File(..., description="Select one or more files (CSV, JSON, Parquet, Excel)"),
    db:          Session          = Depends(get_db),
):
    """
    Upload one or multiple files from your local machine → Azure Raw layer.
    Supports CSV, JSON, Parquet, Excel, TSV.
    Select a single file or hold Ctrl/Cmd to select multiple — both work the same way.
    After upload run POST /orchestrate/run with source_type=LOCAL and client_name to process through Bronze and Silver.
    """
    if not files:
        raise HTTPException(status_code=400, detail="No files selected")

    storage   = get_storage_client()
    container = settings.AZURE_CONTAINER_NAME or "ag-de-agent"
    batch_id  = datetime.utcnow().strftime("UPLOAD_%Y%m%d_%H%M%S")
    mc_recs, results, errors = [], [], []

    for f in files:
        try:
            suffix = ("." + f.filename.rsplit(".", 1)[-1].lower()) if "." in f.filename else ""
            if suffix not in ALLOWED:
                errors.append({"file": f.filename, "error": f"Unsupported type {suffix}. Allowed: {', '.join(ALLOWED)}"}); continue
            content = await f.read()
            if not content:
                errors.append({"file": f.filename, "error": "File is empty"}); continue

            safe  = f.filename.replace(" ", "_")
            rkey  = f"Raw/{client_name}/{batch_id}/{safe}"
            dsid  = _dsid(client_name, safe)
            fmt   = suffix.lstrip(".").upper()
            cols  = _infer_cols(content, safe)

            storage.put_object(Container=container, Key=rkey, Body=content)

            # DB upsert
            mc = db.query(MasterConfigAuthoritative).filter(MasterConfigAuthoritative.dataset_id == dsid).first()
            if mc:
                mc.last_seen_batch = batch_id; mc.updated_at = datetime.utcnow(); mc.raw_layer_path = f"az://{container}/{rkey}"
            else:
                db.add(MasterConfigAuthoritative(
                    dataset_id=dsid, pipeline_id=str(uuid.uuid4()),
                    client_name=client_name, source_type="LOCAL",
                    source_folder=f"upload/{client_name}", source_object=safe,
                    file_format=fmt,
                    target_layer_bronze=f"Bronze/{client_name}/upload",
                    target_layer_silver=f"Silver/{client_name}/upload",
                    raw_layer_path=f"az://{container}/{rkey}",
                    is_active=True, last_seen_batch=batch_id, created_at=datetime.utcnow(),
                ))

            # Seed DQ placeholders
            existing = {r.column_name for r in db.query(DQSchemaConfig).filter(DQSchemaConfig.dataset_id == dsid).all()}
            for col in cols:
                if col["name"] not in existing:
                    exp = None
                    try: exp = ExpectedDataType[col["inferred_type"]]
                    except KeyError: pass
                    db.add(DQSchemaConfig(dataset_id=dsid, column_name=col["name"],
                        expected_data_type=exp, dq_rule=None, rule_value=None, severity=None, is_active=False))
            db.commit()

            mc_recs.append({
                "dataset_id": dsid, "pipeline_id": str(uuid.uuid4()),
                "client_name": client_name, "source_type": "LOCAL",
                "source_folder": f"upload/{client_name}", "source_object": safe,
                "file_format": fmt, "raw_layer_path": f"az://{container}/{rkey}",
                "target_layer_bronze": f"Bronze/{client_name}/upload",
                "target_layer_silver": f"Silver/{client_name}/upload",
                "is_active": True, "created_at": datetime.utcnow().isoformat(),
            })
            results.append({
                "file_name": safe, "dataset_id": dsid,
                "size_bytes": len(content), "columns": len(cols),
                "raw_path": f"az://{container}/{rkey}",
            })
            logger.info(f"Uploaded {safe} → {rkey}")
        except Exception as e:
            errors.append({"file": f.filename, "error": str(e)})
            logger.error(f"Upload failed {f.filename}: {e}")

    if mc_recs:
        try: _write_master_config(client_name, mc_recs, storage, container)
        except Exception as e: logger.warning(f"master_config write failed: {e}")

    return JSONResponse(content={
        "status":      "SUCCESS" if not errors else ("PARTIAL" if results else "FAILED"),
        "client_name": client_name,
        "batch_id":    batch_id,
        "uploaded":    len(results),
        "failed":      len(errors),
        "results":     results,
        "errors":      errors,
        "next_step":   f"POST /orchestrate/run with source_type=LOCAL client_name={client_name} folder_path=upload/{client_name}",
    })
