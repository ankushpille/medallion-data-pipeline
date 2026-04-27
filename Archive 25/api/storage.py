from fastapi import APIRouter, HTTPException, Query, Depends
from core.azure_storage import get_storage_client
from core.utils import parse_s3_url
from core.settings import settings
from loguru import logger
import pandas as pd
from io import BytesIO
import json
from typing import Optional
from azure.core.exceptions import ResourceNotFoundError
from sqlalchemy.orm import Session
from core.database import get_db
from models.api_source_config import APISourceConfig

router = APIRouter(prefix="/storage", tags=["Storage Explorer"])

@router.get("/list")
def list_storage(path: str = Query("", description="Folder path to list"), db: Session = Depends(get_db)):
    """
    Lists blobs/directories under a path OR lists containers if at root.
    """
    path = path.strip().replace("az:// ", "az://").replace("s3:// ", "s3://")
    
    if path.startswith("s3://") or ("s3" in path and "amazonaws.com" in path):
        try:
            import boto3
            bucket, prefix = parse_s3_url(path)
            
            if prefix and not prefix.endswith("/"):
                prefix += "/"
                
            config = db.query(APISourceConfig).filter(APISourceConfig.aws_bucket_name == bucket).first()
            if not config:
                raise HTTPException(status_code=404, detail=f"S3 Bucket '{bucket}' not found in any registered API Source.")
                
            s3 = boto3.client(
                's3', 
                aws_access_key_id=config.aws_access_key, 
                aws_secret_access_key=config.aws_secret_key, 
                region_name=config.aws_region or "us-east-1"
            )
            
            logger.info(f"Listing S3 bucket='{bucket}' prefix='{prefix}'")
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
            
            folders = []
            for p in resp.get("CommonPrefixes", []):
                folder_path = p.get("Prefix")
                name = folder_path.strip("/").split("/")[-1]
                display_path = f"s3://{bucket}/{folder_path}"
                folders.append({"name": name, "path": display_path, "type": "folder"})
                
            files = []
            for c in resp.get("Contents", []):
                key = c.get("Key")
                if key == prefix: continue
                name = key.split("/")[-1]
                if not name: continue
                display_path = f"s3://{bucket}/{key}"
                files.append({
                    "name": name, 
                    "path": display_path, 
                    "type": "file", 
                    "size": c.get("Size", 0),
                    "extension": name.split(".")[-1].lower() if "." in name else ""
                })
                
            return {"path": path, "folders": folders, "files": files}
            
        except Exception as e:
            logger.error(f"Failed to list S3 storage at {path}: {e}")
            if isinstance(e, HTTPException): raise e
            raise HTTPException(status_code=500, detail=str(e))
            
    # --- Azure Logic Below ---
    try:
        storage = get_storage_client()
        acc = settings.AZURE_STORAGE_ACCOUNT or "account"
        
        # Check if we are at the ROOT level (listing containers)
        # matches: "", "/", "az://", "az://account", "az://account/"
        is_root = False
        if not path or path == "/" or path == "az://" or path == f"az://{acc}" or path == f"az://{acc}/" or path.startswith("s3://"):
            is_root = True

        if is_root:
            logger.info("Listing multi-cloud root level.")
            folders = []
            
            # 1. AWS S3 Buckets from registry
            s3_configs = db.query(APISourceConfig).filter(APISourceConfig.source_type == "S3").all()
            for c in s3_configs:
                if c.aws_bucket_name:
                    folders.append({"name": f"{c.aws_bucket_name} (S3)", "path": f"s3://{c.aws_bucket_name}/", "type": "folder"})
            
            # 2. Azure Containers
            try:
                container_names = storage.list_containers()
                for name in container_names:
                    folders.append({"name": f"{name} (Azure)", "path": f"az://{acc}/{name}/", "type": "folder"})
            except Exception as ae:
                logger.warning(f"Failed to list Azure containers: {ae}")

            return {"path": path or "Root", "folders": folders, "files": []}

        # Regular blob listing logic
        if path.startswith("az://") or ".blob.core.windows.net" in path:
            passed_container, clean_path = storage.parse_az_url(path)
            container = passed_container or settings.AZURE_CONTAINER_NAME or "datalake"
            logger.info(f"Listing container='{container}' prefix='{clean_path}'")
        else:
            clean_path = path
            container = settings.AZURE_CONTAINER_NAME or "datalake"
        
        # Ensure path ends with / if not empty for correct delimiter listing
        prefix = clean_path if not clean_path or clean_path.endswith("/") else f"{clean_path}/"
        
        # Determine if we should return full URIs to maintain context during navigation
        uri_base = None
        if path.startswith("az://"):
            # Normalize path for base formatting
            p = path if path.endswith("/") else f"{path}/"
            # Format: az://account/container/ or az://container/
            parts = p.split("/", 4) # az:, "", acct, cont, path...
            if len(parts) >= 4:
                # If 3rd part is account, base is az://account/container/
                if parts[2].lower() == (settings.AZURE_STORAGE_ACCOUNT or "").lower():
                    uri_base = f"az://{parts[2]}/{container}/"
                else:
                    # Legacy az://container/
                    uri_base = f"az://{container}/"
            else:
                 uri_base = f"az://{container}/"
        elif ".blob.core.windows.net" in path:
            # Format: https://account.blob.core.windows.net/container/
            acc = settings.AZURE_STORAGE_ACCOUNT
            uri_base = f"https://{acc}.blob.core.windows.net/{container}/"

        response = storage.list_objects_v2(Prefix=prefix, Delimiter="/", Container=container)

        folders = []
        for p in response.get("CommonPrefixes", []):
            folder_path = p["Prefix"]
            name = folder_path.strip("/").split("/")[-1]
            display_path = folder_path
            if uri_base:
                display_path = uri_base + folder_path.lstrip("/")
            folders.append({"name": name, "path": display_path, "type": "folder"})
            
        files = []
        for c in response.get("Contents", []):
            key = c["Key"]
            if key == prefix: continue # Skip the folder itself
            name = key.split("/")[-1]
            if not name: continue
            display_path = key
            if uri_base:
                display_path = uri_base + key.lstrip("/")
            files.append({
                "name": name, 
                "path": display_path, 
                "type": "file", 
                "size": c.get("Size", 0),
                "extension": name.split(".")[-1].lower() if "." in name else ""
            })
            
        return {"path": path, "folders": folders, "files": files}
    except ResourceNotFoundError as e:
        logger.error(f"Container not found when listing {path}: {e}")
        raise HTTPException(
            status_code=404,
            detail=f"Container not found: '{container}'. Verify it exists in your Azure Storage account '{settings.AZURE_STORAGE_ACCOUNT}'. Error: {e}"
        )
    except Exception as e:
        logger.error(f"Failed to list storage at {path}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/config")
def get_storage_config():
    """
    Returns the storage account and container configuration for the frontend explore.
    """
    return {
        "azure_account": settings.AZURE_STORAGE_ACCOUNT,
        "azure_container": "", # Leave empty to force root selection (az://account/)
        "adls_container": ""  # User will pick manually (e.g. ag-de-landing)
    }

@router.get("/preview")
def preview_file(path: str = Query(..., description="Full path to the file"), db: Session = Depends(get_db)):
    """
    Fetches the first few rows of a CSV or full content of JSON/Text for preview.
    """
    path = path.strip().replace("az:// ", "az://").replace("s3:// ", "s3://")
    
    if path.startswith("s3://") or ("s3" in path and "amazonaws.com" in path):
        try:
            import boto3
            bucket, key = parse_s3_url(path)
            
            config = db.query(APISourceConfig).filter(APISourceConfig.aws_bucket_name == bucket).first()
            if not config:
                raise HTTPException(status_code=404, detail=f"S3 Bucket '{bucket}' not found.")
                
            s3 = boto3.client(
                's3', 
                aws_access_key_id=config.aws_access_key, 
                aws_secret_access_key=config.aws_secret_key, 
                region_name=config.aws_region or "us-east-1"
            )
            
            logger.info(f"Previewing S3 bucket='{bucket}' key='{key}'")
            obj = s3.get_object(Bucket=bucket, Key=key)
            content = obj["Body"].read()
        except Exception as e:
            logger.error(f"Failed to preview S3 file at {path}: {e}")
            if isinstance(e, HTTPException): raise e
            raise HTTPException(status_code=500, detail=str(e))
    else:
        try:
            storage = get_storage_client()
            # Support full az:// URIs in the path parameter, otherwise treat as pure path
            if path.startswith("az://") or ".blob.core.windows.net" in path:
                passed_container, key = storage.parse_az_url(path)
                container = passed_container or settings.AZURE_CONTAINER_NAME or "datalake"
            else:
                key = path
                container = settings.AZURE_CONTAINER_NAME or "datalake"
            
            logger.info(f"Previewing Azure container='{container}' key='{key}'")
            obj = storage.get_object(Key=key, Container=container)
    
            content = obj["Body"].read()
        except Exception as e:
            logger.error(f"Failed to preview Azure file at {path}: {e}")
            raise HTTPException(status_code=500, detail=str(e))
            
    # Common preview handling layer
    try:
        # Derive extension from the decoded key (avoids %20 / special chars in raw URL)
        ext = key.split(".")[-1].lower() if "." in key else ""
        
        if ext == "csv":
            df = pd.read_csv(BytesIO(content), nrows=100) # Preview first 100 rows
            # Convert to string to safely handle NaN, Inf, and other non-JSON serializeable types
            df = df.fillna("").astype(str)
            return {
                "type": "csv",
                "columns": df.columns.tolist(),
                "rows": df.values.tolist(),
                "total_rows_approx": "First 100 lines shown"
            }
        elif ext == "parquet":
            try:
                df = pd.read_parquet(BytesIO(content))
                df = df.head(100).fillna("").astype(str)
                return {
                    "type": "csv", # Use csv type in frontend to reuse table rendering
                    "columns": df.columns.tolist(),
                    "rows": df.values.tolist(),
                    "total_rows_approx": "First 100 Parquet rows shown"
                }
            except Exception as pe:
                logger.error(f"Parquet decode failed: {pe}")
                return {"type": "text", "content": f"Error decoding Parquet: {str(pe)}"}
        elif ext == "json":
            try:
                data = json.loads(content.decode("utf-8"))
                return {"type": "json", "data": data}
            except:
                return {"type": "text", "content": content.decode("utf-8")[:5000]}
        else:
            return {"type": "text", "content": content.decode("utf-8", errors="ignore")[:5000]}
            
    except Exception as e:
        logger.error(f"Preview failed for {path}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
