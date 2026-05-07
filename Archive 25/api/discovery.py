from fastapi import APIRouter, HTTPException, Request, UploadFile, File, Form, Depends
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
import logging
import json
import urllib.request
import urllib.error
import base64
from datetime import datetime
from sqlalchemy.orm import Session
from services.pipeline_intelligence_service import analyze_pipeline_live
from services.fabric_bundle_analysis_service import analyze_fabric_bundle
from services.fabric_runtime_intelligence_service import execute_and_capture_runtime_intelligence
from services.fabric.universal_preview_service import FabricUniversalPreviewService
from api.storage import preview_file
from core.database import SessionLocal
from core.database import get_db
from core.utils import generate_dataset_id
from models.api_source_config import APISourceConfig
from models.master_config_authoritative import MasterConfigAuthoritative
from models.master_config import MasterConfig

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/discovery", tags=["Pipeline Intelligence"])

class AnalyzeRequest(BaseModel):
    client_name: str
    # platform identifies the execution layer (FABRIC, DATABRICKS, AWS, AZURE)
    # it is NOT a source connector and must never be validated against registered sources
    platform: Optional[str] = None
    target: Optional[str] = None
    auth_mode: Optional[str] = None
    credentials: Optional[Dict[str, Any]] = None
    use_cloud_llm: bool = True
    llm_provider: str = "gpt"
    use_local_llm: bool = False
    scan_mode: str = "live"
    providers: Optional[str] = None
    source_type: Optional[str] = None
    payload: Optional[Dict[str, Any]] = None


class ApiScanRequest(BaseModel):
    client_name: str
    source_name: Optional[str] = None


class RuntimeIntelligenceRequest(BaseModel):
    client_name: str
    workspace_id: str
    pipeline_id: str
    existing_analysis: Optional[Dict[str, Any]] = None


class RuntimeSourcePreviewRequest(BaseModel):
    source_connection: Optional[Dict[str, Any]] = None
    schema_discovery: Optional[Dict[str, Any]] = None
    workspaceId: Optional[str] = None
    artifactId: Optional[str] = None
    rootFolder: Optional[str] = None
    folderPath: Optional[str] = None
    fileName: Optional[str] = None
    format: Optional[str] = None
    header: Optional[bool] = None
    delimiter: Optional[str] = None


class RuntimeSourceSaveRequest(BaseModel):
    client_name: str
    pipeline_id: Optional[str] = None
    runtime_source_discovery: Dict[str, Any]


def _runtime_source_path(source_connection: Dict[str, Any]) -> str:
    return (
        source_connection.get("preview_path")
        or source_connection.get("resolved_path")
        or source_connection.get("full_path")
        or "/".join(
            part.strip("/")
            for part in [str(source_connection.get("folder_path") or "").strip("/"), str(source_connection.get("file_name") or "").strip("/")]
            if part
        )
    )


def _runtime_dataset_name(source_connection: Dict[str, Any]) -> str:
    return (
        source_connection.get("file_name")
        or source_connection.get("folder_path")
        or source_connection.get("artifact_id")
        or "fabric_runtime_source"
    )


def _normalize_preview_source(request: RuntimeSourcePreviewRequest) -> Dict[str, Any]:
    source_connection = dict(request.source_connection or {})
    if request.workspaceId:
        source_connection.setdefault("workspace_id", request.workspaceId)
    if request.artifactId:
        source_connection.setdefault("artifact_id", request.artifactId)
    if request.rootFolder:
        source_connection.setdefault("root_folder", request.rootFolder)
    if request.folderPath:
        source_connection.setdefault("folder_path", request.folderPath)
    if request.fileName:
        source_connection.setdefault("file_name", request.fileName)
    if request.format:
        source_connection.setdefault("format", str(request.format).lower())
    if request.header is not None:
        source_connection.setdefault("header_enabled", request.header)
    if request.delimiter:
        source_connection.setdefault("delimiter", request.delimiter)
    if not source_connection.get("resolved_path"):
        root = str(source_connection.get("root_folder") or "Files").strip("/\\") or "Files"
        folder = str(source_connection.get("folder_path") or "").strip("/\\")
        file_name = str(source_connection.get("file_name") or "").strip("/\\")
        parts = [root]
        if folder:
            parts.append(folder)
        if file_name:
            parts.append(file_name)
        source_connection["resolved_path"] = "/".join(part for part in parts if part)
    return source_connection


def _structured_runtime_sample_preview(source_connection: Dict[str, Any], schema_discovery: Dict[str, Any], diagnostics: List[Dict[str, Any]]) -> Dict[str, Any]:
    sample_rows = schema_discovery.get("sample_rows") or []
    columns = schema_discovery.get("columns") or []
    column_names = [column.get("column_name") for column in columns if column.get("column_name")]
    schema = [
        {
            "column_name": column.get("column_name"),
            "data_type": column.get("data_type"),
            "nullable": column.get("nullable"),
            "ordinal_position": column.get("ordinal_position"),
        }
        for column in columns
    ]
    preview_rows = []
    for row in sample_rows[:25]:
        if isinstance(row, dict):
            preview_rows.append({column: row.get(column) for column in column_names})
    return {
        "type": "csv",
        "preview_rows": preview_rows,
        "rows": [[row.get(column) for column in column_names] for row in preview_rows],
        "schema": schema,
        "row_count_estimate": len(sample_rows),
        "total_rows_approx": "Runtime metadata sample",
        "columns": column_names,
        "datatypes": [column.get("data_type") for column in columns],
        "resolved_path": source_connection.get("resolved_path"),
        "preview_mode": "runtime_sample",
        "source_name": _runtime_dataset_name(source_connection),
        "diagnostics": diagnostics,
    }


@router.post("/fabric-bundle-analysis")
async def run_fabric_bundle_analysis(
    http_request: Request,
    client_name: str = Form(...),
    workspace_id: Optional[str] = Form(None),
    pipeline_id: Optional[str] = Form(None),
    use_cloud_llm: bool = Form(True),
    existing_analysis_json: Optional[str] = Form(None),
    file: UploadFile = File(...),
):
    if not (file.filename or "").lower().endswith(".zip"):
        raise HTTPException(status_code=400, detail="Only Microsoft Fabric exported ZIP bundles are supported.")

    try:
        existing_analysis = json.loads(existing_analysis_json) if existing_analysis_json else None
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"existing_analysis_json is not valid JSON: {exc}")

    authorization = http_request.headers.get("authorization")
    bearer_token = None
    if authorization and authorization.lower().startswith("bearer "):
        bearer_token = authorization.split(" ", 1)[1].strip()

    try:
        payload = await analyze_fabric_bundle(
            client_name=client_name,
            file_bytes=await file.read(),
            filename=file.filename or "fabric-export.zip",
            workspace_id=workspace_id,
            pipeline_id=pipeline_id,
            use_cloud_llm=use_cloud_llm,
            authorization_token=bearer_token,
            existing_analysis=existing_analysis,
        )
        return payload
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Fabric bundle analysis failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/fabric-runtime-intelligence")
async def run_fabric_runtime_intelligence(request: RuntimeIntelligenceRequest, http_request: Request):
    authorization = http_request.headers.get("authorization")
    bearer_token = None
    if authorization and authorization.lower().startswith("bearer "):
        bearer_token = authorization.split(" ", 1)[1].strip()
    if not bearer_token:
        raise HTTPException(status_code=401, detail="Fabric runtime capture requires a bearer token.")

    try:
        return await execute_and_capture_runtime_intelligence(
            client_name=request.client_name,
            workspace_id=request.workspace_id,
            pipeline_id=request.pipeline_id,
            access_token=bearer_token,
            existing_analysis=request.existing_analysis,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Fabric runtime intelligence failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/fabric-runtime-source-preview")
async def preview_runtime_source(request: RuntimeSourcePreviewRequest, http_request: Request, db: Session = Depends(get_db)):
    source_connection = _normalize_preview_source(request)
    schema_discovery = request.schema_discovery or {}
    source_path = _runtime_source_path(source_connection)
    diagnostics: List[Dict[str, Any]] = []
    authorization = http_request.headers.get("authorization")
    bearer_token = None
    if authorization and authorization.lower().startswith("bearer "):
        bearer_token = authorization.split(" ", 1)[1].strip()

    diagnostics.append({
        "mode": "metadata_resolution",
        "status": "success" if source_connection.get("workspace_id") or source_connection.get("artifact_id") else "partial",
        "workspace_id": source_connection.get("workspace_id"),
        "artifact_id": source_connection.get("artifact_id"),
        "root_folder": source_connection.get("root_folder"),
        "folder_path": source_connection.get("folder_path"),
        "file_name": source_connection.get("file_name"),
        "resolved_path": source_connection.get("resolved_path"),
    })

    if bearer_token and source_connection.get("workspace_id"):
        diagnostics.append({
            "mode": "spark_notebook",
            "status": "attempted",
            "workspace_id": source_connection.get("workspace_id"),
            "resolved_path": source_connection.get("resolved_path"),
            "preview_strategy": source_connection.get("preview_strategy"),
        })
        try:
            resolved_source = {
                "source_type": source_connection.get("storage_type") or source_connection.get("source_type"),
                "connector_type": source_connection.get("connector_type"),
                "format": source_connection.get("format"),
                "connection_metadata": source_connection.get("connection_metadata") or {},
                "runtime_metadata": source_connection.get("runtime_metadata") or {},
                "preview_strategy": source_connection.get("preview_strategy"),
                "read_options": {
                    "header": source_connection.get("header_enabled"),
                    "delimiter": source_connection.get("delimiter"),
                    "quote": source_connection.get("quote_char"),
                    "escape": source_connection.get("escape_char"),
                },
            }
            universal_request = {
                "resolved_source": resolved_source,
                "runtime_statistics": {},
            }
            preview_service = FabricUniversalPreviewService(bearer_token)
            notebook_preview = await preview_service.execute_preview(
                workspace_id=source_connection.get("workspace_id"),
                preview_request=universal_request,
            )
            for item in notebook_preview.get("notebook_diagnostics") or []:
                diagnostics.append({
                    "mode": "spark_notebook_lifecycle",
                    **item,
                })
            if notebook_preview.get("preview_error"):
                diagnostics.append({
                    "mode": "spark_notebook",
                    "status": "failed",
                    "error": notebook_preview.get("preview_error"),
                    "resolved_path": notebook_preview.get("resolved_path") or source_connection.get("resolved_path"),
                })
                provisioning_detail = notebook_preview.get("provisioning_detail")
                if isinstance(provisioning_detail, dict):
                    diagnostics.append({
                        "mode": "spark_notebook_provisioning",
                        **{k: v for k, v in provisioning_detail.items() if k != "notebook_diagnostics"},
                    })
            else:
                diagnostics.append({
                    "mode": "spark_notebook",
                    "status": "success",
                    "preview_strategy": notebook_preview.get("preview_strategy"),
                    "resolved_path": notebook_preview.get("resolved_path"),
                })
                notebook_preview["preview_mode"] = "spark_notebook"
                notebook_preview["diagnostics"] = diagnostics
                notebook_preview["source_name"] = _runtime_dataset_name(source_connection)
                notebook_preview["resolved_source"] = notebook_preview.get("resolved_source") or universal_request.get("resolved_source")
                return notebook_preview
        except Exception as exc:
            diagnostics.append({
                "mode": "spark_notebook",
                "status": "failed",
                "error": str(exc),
                "resolved_path": source_connection.get("resolved_path"),
            })
    else:
        diagnostics.append({
            "mode": "spark_notebook",
            "status": "skipped",
            "reason": "Fabric bearer token or workspace id was not provided for notebook execution.",
        })

    if source_path.startswith(("az://", "s3://", "https://")):
        diagnostics.append({
            "mode": "filesystem",
            "status": "attempted",
            "path": source_path,
        })
        try:
            payload = preview_file(path=source_path, db=db)
            rows = payload.get("rows") or []
            columns = payload.get("columns") or []
            return {
                **payload,
                "preview_rows": [
                    {column: row[index] if isinstance(row, list) and index < len(row) else None for index, column in enumerate(columns)}
                    for row in rows[:25]
                ] if columns and rows else [],
                "schema": schema_discovery.get("columns") or [],
                "row_count_estimate": len(rows),
                "resolved_path": source_connection.get("resolved_path") or source_path,
                "preview_mode": "filesystem",
                "source_name": _runtime_dataset_name(source_connection),
                "diagnostics": diagnostics,
            }
        except Exception as exc:
            diagnostics.append({
                "mode": "filesystem",
                "status": "failed",
                "path": source_path,
                "error": str(exc),
            })
    else:
        diagnostics.append({
            "mode": "filesystem",
            "status": "skipped",
            "reason": "Resolved path is not an absolute storage URI.",
            "path": source_path,
        })

    diagnostics.append({
        "mode": "spark",
        "status": "unavailable",
        "path": source_connection.get("resolved_path"),
        "reason": "Spark preview fallback outside notebook execution is not configured in this backend session.",
    })
    diagnostics.append({
        "mode": "lakehouse_sql",
        "status": "unavailable",
        "artifact_id": source_connection.get("artifact_id"),
        "reason": "Lakehouse SQL preview is not configured in this backend session.",
    })
    diagnostics.append({
        "mode": "onelake_filesystem",
        "status": "unavailable",
        "path": source_connection.get("resolved_path"),
        "reason": "OneLake filesystem preview is not configured in this backend session.",
    })

    sample_preview = _structured_runtime_sample_preview(source_connection, schema_discovery, diagnostics)
    if sample_preview.get("preview_rows"):
        return sample_preview

    raise HTTPException(status_code=400, detail={
        "message": "Runtime source preview could not access the physical Lakehouse file and no runtime sample rows were available.",
        "lakehouse_metadata_found": bool(source_connection.get("workspace_id") or source_connection.get("artifact_id")),
        "resolved_path": source_connection.get("resolved_path"),
        "preview_mode_attempted": list(dict.fromkeys(item.get("mode") for item in diagnostics if item.get("mode"))),
        "diagnostics": diagnostics,
    })


@router.post("/fabric-runtime-source-save")
def save_runtime_source(request: RuntimeSourceSaveRequest, db: Session = Depends(get_db)):
    runtime_source_discovery = request.runtime_source_discovery or {}
    source_connection = runtime_source_discovery.get("source_connection") or {}
    target_connection = runtime_source_discovery.get("target_connection") or {}
    schema_discovery = runtime_source_discovery.get("schema_discovery") or {}
    dq_recommendations = runtime_source_discovery.get("dq_recommendations") or []
    runtime_statistics = runtime_source_discovery.get("runtime_statistics") or {}

    source_path = _runtime_source_path(source_connection)
    if not source_path:
        source_path = f"fabric://{source_connection.get('workspace_id') or 'workspace'}/{source_connection.get('artifact_id') or _runtime_dataset_name(source_connection)}"

    source_type = str(source_connection.get("storage_type") or source_connection.get("source_type") or "FABRIC").upper()
    dataset_id = generate_dataset_id(request.client_name, source_type, source_path)
    source_object = _runtime_dataset_name(source_connection)
    file_format = source_connection.get("format") or target_connection.get("format") or "UNKNOWN"

    authoritative = db.query(MasterConfigAuthoritative).filter(MasterConfigAuthoritative.dataset_id == dataset_id).first()
    if not authoritative:
        authoritative = MasterConfigAuthoritative(dataset_id=dataset_id)
        db.add(authoritative)
    authoritative.pipeline_id = request.pipeline_id
    authoritative.client_name = request.client_name
    authoritative.source_type = source_type
    authoritative.source_folder = source_connection.get("folder_path") or source_path
    authoritative.source_object = source_object
    authoritative.file_format = file_format
    authoritative.raw_layer_path = source_path
    authoritative.target_layer_bronze = target_connection.get("folder_path") or target_connection.get("full_path")
    authoritative.is_active = True
    authoritative.updated_at = datetime.utcnow()

    legacy = db.query(MasterConfig).filter(MasterConfig.dataset_id == dataset_id).first()
    if not legacy:
        legacy = MasterConfig(dataset_id=dataset_id)
        db.add(legacy)
    legacy.client_name = request.client_name
    legacy.source_system = source_type
    legacy.source_object = source_object
    legacy.source_schema = source_connection.get("folder_path")
    legacy.file_format = file_format
    legacy.target_schema = target_connection.get("folder_path")
    legacy.target_table = target_connection.get("file_name")
    legacy.is_active = True
    legacy.validation_rules = {
        "schema": schema_discovery,
        "dq_rules": dq_recommendations,
        "runtime_metrics": runtime_statistics,
        "source_connection": source_connection,
        "target_connection": target_connection,
    }
    legacy.rows_read = runtime_statistics.get("rows_read") or 0
    legacy.rows_written = runtime_statistics.get("rows_written") or 0
    legacy.updated_at = datetime.utcnow()

    db.commit()
    return {
        "status": "SUCCESS",
        "dataset_id": dataset_id,
        "source_path": source_path,
        "source_object": source_object,
        "file_format": file_format,
    }


def _api_headers(config: APISourceConfig) -> Dict[str, str]:
    headers = {"Accept": "application/json", "User-Agent": "DEA-Agent/1.0"}
    auth_type = (config.auth_type or "none").lower()
    token = config.auth_token or ""
    if auth_type == "bearer" and token:
        headers["Authorization"] = f"Bearer {token}"
    elif auth_type in {"api_key", "apikey"} and token:
        headers[config.api_key_header or "X-Api-Key"] = token
    elif auth_type == "basic" and token:
        headers["Authorization"] = f"Basic {base64.b64encode(token.encode()).decode()}"
    return headers


def _extract_records(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        if len(payload) == 2 and isinstance(payload[1], list):
            payload = payload[1]
        records = payload
    elif isinstance(payload, dict):
        records = None
        for key in ("data", "results", "items", "records", "value", "articles", "sources", "hits", "entries", "content", "list"):
            if isinstance(payload.get(key), list):
                records = payload[key]
                break
        if records is None:
            records = [payload]
    else:
        records = [{"value": payload}]

    normalized = []
    for row in records[:100]:
        normalized.append(row if isinstance(row, dict) else {"value": row})
    return normalized


def _infer_schema(records: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    schema = []
    seen = set()
    for row in records:
        for key, value in row.items():
            if key in seen:
                continue
            seen.add(key)
            if isinstance(value, bool):
                dtype = "BOOLEAN"
            elif isinstance(value, int):
                dtype = "INTEGER"
            elif isinstance(value, float):
                dtype = "DOUBLE"
            elif isinstance(value, (dict, list)):
                dtype = "JSON"
            else:
                dtype = "STRING"
            schema.append({"column_name": str(key), "data_type": dtype})
    return schema


def _dq_rules_for_schema(schema: List[Dict[str, str]]) -> Dict[str, Any]:
    return {
        column["column_name"]: {
            "rules": ["NOT_NULL"] + (["VALID_JSON"] if column["data_type"] == "JSON" else []),
            "severity": "WARN",
        }
        for column in schema
    }


def _endpoint_url(base_url: str, endpoint: str) -> str:
    if endpoint.lower().startswith(("http://", "https://")):
        return endpoint
    return f"{base_url.rstrip('/')}/{endpoint.strip('/')}" if endpoint else base_url.rstrip("/")


# Canonical identifiers that represent EXECUTION PLATFORMS.
PLATFORM_IDENTIFIERS: set[str] = {"FABRIC", "DATABRICKS", "AWS", "AZURE"}

# Canonical identifiers that represent DATA-SOURCE CONNECTORS.
SOURCE_CONNECTOR_IDENTIFIERS: set[str] = {"REST_API", "S3", "ADLS", "LOCAL", "AWS", "AZURE"}


def _canonical_source_type(value: Optional[str]) -> Optional[str]:
    """Normalise a data-source connector name to its canonical upper-case form."""
    raw = (value or "").strip().upper()
    if raw in {"AWS", "S3"}:
        return "AWS"
    if raw in {"AZURE", "ADLS"}:
        return "AZURE"
    if raw in {"API", "REST", "REST_API"}:
        return "REST_API"
    if raw in {"FABRIC", "MICROSOFT_FABRIC"}:
        return "FABRIC"
    if raw == "LOCAL":
        return "LOCAL"
    return raw or None


def _canonical_platform(value: Optional[str]) -> Optional[str]:
    """Normalise a platform name to its canonical upper-case form."""
    raw = (value or "").strip().upper()
    mapping = {
        "FABRIC": "FABRIC",
        "MICROSOFT_FABRIC": "FABRIC",
        "MSFABRIC": "FABRIC",
        "DATABRICKS": "DATABRICKS",
        "AWS": "AWS",
        "AMAZON": "AWS",
        "AZURE": "AZURE",
        "MICROSOFT": "AZURE",
    }
    return mapping.get(raw)


def _is_platform(value: Optional[str]) -> bool:
    """Return True when the value is a platform identifier, not a source connector."""
    canonical = _canonical_source_type(value)
    return canonical in PLATFORM_IDENTIFIERS and canonical not in SOURCE_CONNECTOR_IDENTIFIERS



def _target_source_type(target: Optional[str]) -> Optional[str]:
    raw = (target or "").strip().lower()
    if raw in {"aws", "s3"}:
        return "AWS"
    if raw in {"azure", "adls"}:
        return "AZURE"
    if raw in {"fabric", "microsoft fabric", "msfabric"}:
        return "FABRIC"
    if raw in {"api", "rest", "rest_api"}:
        return "REST_API"
    if raw == "local":
        return "LOCAL"
    return None


def _configured_source_types(client_name: str) -> set[str]:
    db = SessionLocal()
    try:
        types = set()
        for cfg in db.query(APISourceConfig).filter(APISourceConfig.client_name == client_name, APISourceConfig.is_active == True).all():
            mapped = _canonical_source_type(cfg.source_type)
            if mapped:
                types.add(mapped)
        for row in db.query(MasterConfigAuthoritative.source_type).filter(MasterConfigAuthoritative.client_name == client_name, MasterConfigAuthoritative.is_active == True).distinct().all():
            mapped = _canonical_source_type(row[0])
            if mapped:
                types.add(mapped)
        for row in db.query(MasterConfig.source_system).filter(MasterConfig.client_name == client_name, MasterConfig.is_active == True).distinct().all():
            mapped = _canonical_source_type(row[0])
            if mapped:
                types.add(mapped)
        return types
    finally:
        db.close()

@router.post("/analyze")
async def run_discovery_analyze(request: AnalyzeRequest, http_request: Request):
    """
    Analyzes the live cloud environment or configs for a client.

    Architectural contract:
      - `platform`    = execution layer (FABRIC / DATABRICKS / AWS / AZURE)
                        Never registered as a source; never validated against configured_types.
      - `source_type` = data-source connector (REST_API / S3 / ADLS / LOCAL)
                        Registered by users; validated only when present and not a platform.

    Valid combinations (examples):
      FABRIC + REST_API  ✓
      FABRIC + LOCAL     ✓
      FABRIC + S3        ✓
      REST_API (legacy)  ✓
      S3 (legacy)        ✓
    """
    logger.info(f"Running live pipeline intelligence for {request.client_name}")
    try:
        # ── 1. Resolve platform and source-type separately ───────────────────
        requested_platform = _canonical_platform(request.platform)

        # source_type from request; also accept target/providers as a legacy fallback.
        requested_source_type = _canonical_source_type(request.source_type)
        
        # Legacy: derive source type from target/providers when no explicit source_type given
        if not requested_source_type:
            derived = _target_source_type(request.target or request.providers)
            if derived:
                requested_source_type = derived

        # ── 2. Fetch configured source types for this client ──────────────────
        configured_types = _configured_source_types(request.client_name)

        # ── 3. Debug logging ──────────────────────────────────────────────────
        logger.info(
            "Discovery validation | platform=%s source_type=%s configured_types=%s",
            requested_platform,
            requested_source_type,
            sorted(configured_types),
        )

        # ── 4. Validate ONLY the data-source connector ────────────────────────
        # We skip validation if:
        # a) No sources are configured yet for the client
        # b) The requested source type is FABRIC (which is a platform, not a connector)
        # c) The requested source type is None
        if (
            configured_types
            and requested_source_type
            and requested_source_type != "FABRIC"
            and requested_source_type not in configured_types
        ):
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Discovery target '{requested_source_type}' is not configured for client "
                    f"'{request.client_name}'. Configured source types: {sorted(configured_types)}"
                ),
            )

        # ── 5. Build the bearer token from the Authorization header ───────────
        authorization = http_request.headers.get("authorization")
        bearer_token = None
        if authorization and authorization.lower().startswith("bearer "):
            bearer_token = authorization.split(" ", 1)[1].strip()

        # ── 6. Determine providers/target for the scanner ─────────────────────
        # When a platform is given, use it as the scan target so the intelligence
        # service routes to the correct scanner (fabric / aws / azure).
        scan_target = request.target or (requested_platform.lower() if requested_platform else None)
        providers = request.providers or scan_target

        result = await analyze_pipeline_live(
            client_name=request.client_name,
            providers=providers,
            target=scan_target,
            auth_mode=request.auth_mode,
            credentials=request.credentials,
            use_cloud_llm=request.use_cloud_llm,
            llm_provider=request.llm_provider,
            use_local_llm=request.use_local_llm,
            scan_mode=request.scan_mode,
            authorization_token=bearer_token,
            payload=request.payload,
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error analyzing live pipeline: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api-scan")
def run_api_scan(request: ApiScanRequest):
    """
    Scans registered REST API endpoints for a client.
    REST API configs without base_url or endpoints are intentionally skippable and return 400 here;
    the UI should allow Continue without scan for that case.
    """
    db = SessionLocal()
    try:
        query = db.query(APISourceConfig).filter(
            APISourceConfig.client_name == request.client_name,
            APISourceConfig.is_active == True,
        )
        if request.source_name:
            query = query.filter(APISourceConfig.source_name == request.source_name)
        configs = [
            cfg for cfg in query.all()
            if _canonical_source_type(cfg.source_type) == "REST_API"
        ]

        scan_configs = [
            cfg for cfg in configs
            if cfg.base_url and [ep.strip() for ep in (cfg.endpoints or "").split(",") if ep.strip()]
        ]
        if not scan_configs:
            raise HTTPException(status_code=400, detail="Provide API details to enable scanning")

        datasets = []
        discovered_assets = []
        warnings = []
        errors = []
        all_rules = {}

        for cfg in scan_configs:
            endpoints = [ep.strip() for ep in (cfg.endpoints or "").split(",") if ep.strip()]
            for endpoint in endpoints:
                url = _endpoint_url(cfg.base_url, endpoint)
                try:
                    req = urllib.request.Request(url, headers=_api_headers(cfg))
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        raw_text = resp.read().decode("utf-8-sig").strip()
                    payload = json.loads(raw_text)
                    records = _extract_records(payload)
                    schema = _infer_schema(records)
                    dq_rules = _dq_rules_for_schema(schema)
                    all_rules[endpoint] = dq_rules

                    dataset_id = generate_dataset_id(request.client_name, "API", f"{endpoint}/{endpoint.replace('/', '_')}.csv")
                    dataset = {
                        "dataset_id": dataset_id,
                        "source_name": cfg.source_name,
                        "endpoint": endpoint,
                        "full_url": url,
                        "file_name": f"{endpoint.replace('/', '_')}.csv",
                        "file_path": f"{endpoint}/{endpoint.replace('/', '_')}.csv",
                        "file_format": "CSV",
                        "record_sample_size": len(records),
                        "schema": schema,
                        "dq_rules": dq_rules,
                    }
                    datasets.append(dataset)
                    discovered_assets.append({
                        "type": "REST_API_ENDPOINT",
                        "name": endpoint,
                        "source_name": cfg.source_name,
                        "url": url,
                        "columns": [column["column_name"] for column in schema],
                    })

                    existing = db.query(MasterConfigAuthoritative).filter(
                        MasterConfigAuthoritative.dataset_id == dataset_id
                    ).first()
                    if not existing:
                        existing = MasterConfigAuthoritative(dataset_id=dataset_id)
                        db.add(existing)
                    existing.client_name = request.client_name
                    existing.source_type = "API"
                    existing.source_folder = endpoint
                    existing.source_object = dataset["file_name"]
                    existing.file_format = "CSV"
                    existing.raw_layer_path = f"Raw/{request.client_name}/API/{dataset['file_name']}"
                    existing.target_layer_bronze = f"Bronze/{request.client_name}/API/{dataset['file_name']}"
                    existing.target_layer_silver = f"Silver/{request.client_name}/API/{dataset['file_name']}"
                    existing.is_active = True
                except urllib.error.HTTPError as exc:
                    errors.append(f"{endpoint}: HTTP {exc.code} {exc.reason}")
                except Exception as exc:
                    errors.append(f"{endpoint}: {exc}")

        if not datasets:
            raise HTTPException(status_code=400, detail="REST API scan did not produce any datasets. " + "; ".join(errors))

        db.commit()
        source_path = ",".join(dataset["endpoint"] for dataset in datasets)
        return {
            "framework": "REST API",
            "scan_status": "partial" if errors else "success",
            "auth_mode": "credentials" if any((cfg.auth_type or "none").lower() != "none" for cfg in scan_configs) else "none",
            "is_fallback": False,
            "source_systems": [{"type": "REST_API", "source_name": cfg.source_name, "base_url": cfg.base_url} for cfg in scan_configs],
            "discovered_assets": discovered_assets,
            "datasets": datasets,
            "data_pipelines": [{"name": "REST API ingestion", "source_type": "API", "endpoint_count": len(datasets)}],
            "ingestion_support": {"file_based": False, "api": True, "database": False, "streaming": False, "batch": True},
            "ingestion_details": {"source_type": "API", "source_path": source_path, "target": "api"},
            "reformatted_config": {"source_type": "API", "source_path": source_path, "datasets": datasets},
            "pipeline_capabilities": {"scan_mode": "live", "api": True, "batch": True},
            "dq_rules": all_rules,
            "file_types": ["CSV"],
            "warnings": warnings,
            "errors": errors,
            "interactive_flow": ["REST API", "Infer schema", "Generate datasets", "Suggest DQ rules"],
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"REST API scan failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()
