from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
import logging
import json
import urllib.request
import urllib.error
import base64
from services.pipeline_intelligence_service import analyze_pipeline_live
from core.database import SessionLocal
from core.utils import generate_dataset_id
from models.api_source_config import APISourceConfig
from models.master_config_authoritative import MasterConfigAuthoritative
from models.master_config import MasterConfig

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/discovery", tags=["Pipeline Intelligence"])

class AnalyzeRequest(BaseModel):
    client_name: str
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


def _canonical_source_type(value: Optional[str]) -> Optional[str]:
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
    Analyzes the live cloud environment or configs for a client,
    and infers the underlying pipeline capabilities, DQ rules, and flow.
    """
    logger.info(f"Running live pipeline intelligence for {request.client_name}")
    try:
        requested_source_type = _canonical_source_type(request.source_type) or _target_source_type(request.target or request.providers)
        configured_types = _configured_source_types(request.client_name)
        if configured_types and requested_source_type and requested_source_type not in configured_types:
            raise HTTPException(
                status_code=400,
                detail=f"Discovery target {requested_source_type} is not configured for client '{request.client_name}'. Configured source types: {sorted(configured_types)}"
            )

        authorization = http_request.headers.get("authorization")
        bearer_token = None
        if authorization and authorization.lower().startswith("bearer "):
            bearer_token = authorization.split(" ", 1)[1].strip()

        providers = request.providers or request.target
        result = await analyze_pipeline_live(
            client_name=request.client_name,
            providers=providers,
            target=request.target,
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
