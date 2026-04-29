from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import Optional
from core.database import get_db
from models.api_source_config import APISourceConfig
from loguru import logger
import boto3
import urllib.request
from azure.storage.filedatalake import DataLakeServiceClient
import json

router = APIRouter(prefix="/api-source", tags=["API Source"])

def _storage_source_type(value: Optional[str]) -> str:
    raw = (value or "API").upper().strip()
    if raw in {"REST", "REST_API"}:
        return "API"
    if raw == "AWS":
        return "S3"
    if raw == "AZURE":
        return "ADLS"
    return raw


class RegisterRequest(BaseModel):
    client_name:    str
    source_name:    str
    source_type:    Optional[str] = "API"
    base_url:       Optional[str] = None
    auth_type:      Optional[str] = "none"

    auth_token:     Optional[str] = None
    api_key_header: Optional[str] = "X-Api-Key"
    endpoints:      Optional[str] = None
    
    # AWS
    aws_access_key_id:      Optional[str] = None
    aws_secret_access_key:  Optional[str] = None
    region:                 Optional[str] = None
    bucket_name:            Optional[str] = None
    
    # Azure
    azure_account_name:     Optional[str] = None
    azure_account_key:      Optional[str] = None
    azure_container_name:   Optional[str] = None


class UpdateRequest(BaseModel):
    base_url:       Optional[str] = None
    auth_type:      Optional[str] = None
    auth_token:     Optional[str] = None
    api_key_header: Optional[str] = None
    endpoints:      Optional[str] = None
    is_active:      Optional[bool] = None


@router.post("/test-connection", summary="Test Connection to a Data Source")
def test_connection(request: RegisterRequest):
    """
    Attempts to connect to the source defined in request.
    Does NOT save anything to DB.
    """
    source_type = _storage_source_type(request.source_type)
    logger.info(f"Testing connection for {source_type} source: {request.source_name}")

    try:
        if source_type == "S3":
            if not request.bucket_name:
                raise ValueError("Bucket name is required for S3 connection test.")
            
            s3 = boto3.client(
                's3',
                aws_access_key_id=request.aws_access_key_id,
                aws_secret_access_key=request.aws_secret_access_key,
                region_name=request.region or "us-east-1"
            )
            # Try to list objects (max 1) to verify access
            s3.list_objects_v2(Bucket=request.bucket_name, MaxKeys=1)
            return {"status": "SUCCESS", "message": f"Successfully connected to S3 bucket '{request.bucket_name}'"}

        elif source_type == "ADLS":
            if not request.azure_account_name or not request.azure_container_name:
                raise ValueError("Account name and Container name are required for ADLS connection test.")
            
            # Try to list paths in the container
            account_url = f"https://{request.azure_account_name}.dfs.core.windows.net"
            service_client = DataLakeServiceClient(account_url=account_url, credential=request.azure_account_key)
            file_system_client = service_client.get_file_system_client(file_system=request.azure_container_name)
            # Try to list 1 file to trigger a request
            paths = file_system_client.get_paths()
            try:
                next(paths, None)
            except Exception as e:
                # If container not found, it often raises an exception here
                if "ContainerNotFound" in str(e):
                    raise ValueError(f"Container '{request.azure_container_name}' not found.")
                raise
            
            return {"status": "SUCCESS", "message": f"Successfully connected to ADLS container '{request.azure_container_name}'"}

        elif source_type == "API":
            if not request.base_url:
                raise ValueError("Base URL is required for API connection test.")
            
            url = request.base_url.rstrip("/")
            # If endpoints are provided, try the first one for a more realistic test
            if request.endpoints:
                first_ep = request.endpoints.split(",")[0].strip()
                if first_ep:
                    url = f"{url}/{first_ep}"
            
            headers = {"User-Agent": "DEA-Agent/1.0", "Accept": "application/json"}
            
            auth_type = (request.auth_type or "none").lower()
            if auth_type == "bearer" and request.auth_token:
                headers["Authorization"] = f"Bearer {request.auth_token}"
            elif auth_type == "api_key" and request.auth_token:
                headers[request.api_key_header or "X-Api-Key"] = request.auth_token
            elif auth_type == "basic" and request.auth_token:
                import base64
                encoded = base64.b64encode(request.auth_token.encode()).decode()
                headers["Authorization"] = f"Basic {encoded}"
            
            req = urllib.request.Request(url, headers=headers)
            try:
                with urllib.request.urlopen(req, timeout=10) as resp:
                    if 200 <= resp.status < 300:
                        return {"status": "SUCCESS", "message": f"Successfully connected to API at {url} (Status {resp.status})"}
                    else:
                        raise ValueError(f"API returned status {resp.status}")
            except urllib.error.HTTPError as e:
                # Some APIs might return 401/403 which confirms we hit it but failed auth
                # or 404 which confirms we hit it but path is wrong.
                return {"status": "ERROR", "message": f"HTTP Error {e.code}: {e.reason} at {url}"}
            except urllib.error.URLError as e:
                return {"status": "ERROR", "message": f"URL Error: {e.reason}"}

        else:
            return {"status": "ERROR", "message": f"Unsupported source type for testing: {source_type}"}

    except Exception as e:
        logger.error(f"Connection test failed for {source_type}: {e}")
        return {"status": "ERROR", "message": str(e)}


@router.post("/register", summary="Register a Data Source (API/S3/ADLS)")
def register(request: RegisterRequest, db: Session = Depends(get_db)):
    """
    Register a data source for a client.
    Supported types: API, S3, ADLS.
    """
    source_type = _storage_source_type(request.source_type)
    existing = db.query(APISourceConfig).filter(
        APISourceConfig.client_name == request.client_name,
        APISourceConfig.source_name == request.source_name
    ).first()

    if existing:
        raise HTTPException(
            status_code=409,
            detail=f"Source \'{request.source_name}\' already registered for \'{request.client_name}\'. "
                   f"Use PUT /api-source/{existing.id} to update."
        )

    config = APISourceConfig(
        client_name    = request.client_name,
        source_name    = request.source_name,
        source_type    = source_type,
        base_url       = request.base_url.rstrip("/") if request.base_url else None,
        auth_type      = request.auth_type or "none",
        auth_token     = request.auth_token,
        api_key_header = request.api_key_header or "X-Api-Key",
        endpoints      = request.endpoints,
        
        # AWS
        aws_access_key = request.aws_access_key_id,
        aws_secret_key = request.aws_secret_access_key,
        aws_region     = request.region,
        aws_bucket_name = request.bucket_name,
        
        # Azure
        azure_account_name = request.azure_account_name,
        azure_account_key  = request.azure_account_key,
        azure_container_name = request.azure_container_name,

        is_active      = True,
    )
    db.add(config)
    db.commit()
    db.refresh(config)
    logger.info(
        f"Registered {config.source_type} source for client={request.client_name}, "
        f"source_name={request.source_name}, bucket={request.bucket_name or ''}, "
        f"container={request.azure_container_name or ''}, region={request.region or ''}"
    )

    return {
        "status":    "registered",
        "config":    config.to_dict(),
        "next_step": f"Use source_type={config.source_type} in orchestration runs."
    }


@router.get("/list", summary="List registered API sources")
def list_sources(client_name: Optional[str] = None, db: Session = Depends(get_db)):
    query = db.query(APISourceConfig)
    if client_name:
        query = query.filter(APISourceConfig.client_name == client_name)
    configs = query.order_by(APISourceConfig.client_name).all()
    logger.info(f"Listing registered sources: client_name={client_name or '*'}, count={len(configs)}")
    results = []
    for c in configs:
        d = c.to_dict()
        d["auth_token"] = ("*" * max(0, len(c.auth_token) - 4) + c.auth_token[-4:]) if c.auth_token else None
        results.append(d)
    return {"total": len(results), "configs": results}


def _canonical_source_type(value: Optional[str]) -> Optional[str]:
    raw = (value or "").strip().upper()
    if raw in {"S3", "AWS"}:
        return "AWS"
    if raw in {"ADLS", "AZURE"}:
        return "AZURE"
    if raw in {"API", "REST", "REST_API"}:
        return "REST_API"
    if raw in {"FABRIC", "MICROSOFT_FABRIC"}:
        return "FABRIC"
    if raw == "LOCAL":
        return "LOCAL"
    return raw or None


@router.get("/client-source-types", summary="List source types configured for a client")
def client_source_types(client_name: str, db: Session = Depends(get_db)):
    """
    Returns canonical source types associated with the selected client.
    Used by the old DEA UI to avoid showing unrelated Intelligence providers.
    """
    if not client_name:
        raise HTTPException(status_code=400, detail="client_name is required")

    types = set()

    configs = db.query(APISourceConfig).filter(
        APISourceConfig.client_name == client_name,
        APISourceConfig.is_active == True,
    ).all()
    for cfg in configs:
        mapped = _canonical_source_type(cfg.source_type)
        if mapped:
            types.add(mapped)

    try:
        from models.master_config_authoritative import MasterConfigAuthoritative
        rows = db.query(MasterConfigAuthoritative.source_type).filter(
            MasterConfigAuthoritative.client_name == client_name,
            MasterConfigAuthoritative.is_active == True,
        ).distinct().all()
        for row in rows:
            mapped = _canonical_source_type(row[0])
            if mapped:
                types.add(mapped)
    except Exception as exc:
        logger.warning(f"Could not inspect authoritative master config source types for client={client_name}: {exc}")

    try:
        from models.master_config import MasterConfig
        rows = db.query(MasterConfig.source_system).filter(
            MasterConfig.client_name == client_name,
            MasterConfig.is_active == True,
        ).distinct().all()
        for row in rows:
            mapped = _canonical_source_type(row[0])
            if mapped:
                types.add(mapped)
    except Exception as exc:
        logger.warning(f"Could not inspect intelligence master config source types for client={client_name}: {exc}")

    logger.info(f"Client source types: client={client_name}, source_types={sorted(types)}")
    return {"client_name": client_name, "source_types": sorted(types)}


@router.get("/endpoints/{client_name}", summary="List available API endpoints for a client")
def list_endpoints(client_name: str, db: Session = Depends(get_db)):
    """Returns all endpoints registered for a client. Use these as folder_path in orchestrate/run."""
    configs = db.query(APISourceConfig).filter(
        APISourceConfig.client_name == client_name,
        APISourceConfig.is_active == True
    ).all()
    if not configs:
        raise HTTPException(status_code=404, detail=f"No active API configs for \'{client_name}\'.")
    result = []
    for c in configs:
        eps = [e.strip() for e in c.endpoints.split(",") if e.strip()] if c.endpoints else []
        for ep in eps:
            result.append({
                "source_name": c.source_name,
                "endpoint":    ep,
                "folder_path": ep,
                "full_url":    f"{c.base_url}/{ep}"
            })
    return {"client_name": client_name, "endpoints": result}


@router.put("/{config_id}", summary="Update an API source config")
def update(config_id: str, request: UpdateRequest, db: Session = Depends(get_db)):
    config = db.query(APISourceConfig).filter(APISourceConfig.id == config_id).first()
    if not config:
        raise HTTPException(status_code=404, detail=f"Config {config_id} not found.")
    if request.base_url       is not None: config.base_url       = request.base_url.rstrip("/")
    if request.auth_type      is not None: config.auth_type      = request.auth_type
    if request.auth_token     is not None: config.auth_token     = request.auth_token
    if request.api_key_header is not None: config.api_key_header = request.api_key_header
    if request.endpoints      is not None: config.endpoints      = request.endpoints
    if request.is_active      is not None: config.is_active      = request.is_active
    db.commit()
    db.refresh(config)
    return {"status": "updated", "config": config.to_dict()}


@router.delete("/{config_id}", summary="Delete an API source config")
def delete(config_id: str, db: Session = Depends(get_db)):
    config = db.query(APISourceConfig).filter(APISourceConfig.id == config_id).first()
    if not config:
        raise HTTPException(status_code=404, detail=f"Config {config_id} not found.")
    db.delete(config)
    db.commit()
    return {"status": "deleted", "config_id": config_id}
