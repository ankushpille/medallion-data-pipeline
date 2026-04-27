from mcp.server.fastmcp import FastMCP
import os
from azure.storage.filedatalake import DataLakeServiceClient
from azure.storage.blob import BlobServiceClient, ContainerClient
from typing import List, Dict, Any, Optional
from loguru import logger
from dataclasses import dataclass
from enum import Enum
import json
import io

# Initialize FastMCP Server
mcp = FastMCP("AgilDataEngineerAgent")

def _get_fs_client(container_name: str):
    """
    Returns a DataLakeFileSystemClient (ADLS Gen2 — hierarchical namespace).
    Uses connection string from AZURE_STORAGE_CONNECTION_STRING.
    """
    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    account_name = os.getenv("AZURE_STORAGE_ACCOUNT")

    if conn_str:
        svc = DataLakeServiceClient.from_connection_string(conn_str)
    elif account_name:
        from azure.identity import DefaultAzureCredential
        svc = DataLakeServiceClient(
            account_url=f"https://{account_name}.dfs.core.windows.net",
            credential=DefaultAzureCredential()
        )
    else:
        raise ValueError("Neither AZURE_STORAGE_CONNECTION_STRING nor AZURE_STORAGE_ACCOUNT is set.")

    return svc.get_file_system_client(file_system=container_name)

# ---------------------------------------------------------
# SHARED LOGIC (Migrated from MCPSourceConnector)
# ---------------------------------------------------------

class SourceType(str, Enum):
    ADLS = "ADLS"

def _build_search_path(client_name: str, folder_path: str) -> str:
    """
    Builds the full ADLS search path. 
    If folder_path starts with root/client or already seems 'absolute' 
    relative to the container, it preserves it.
    """
    root = os.getenv("ADLS_ROOT_FOLDER", "").strip("/")
    
    fp = folder_path.strip("/")
    
    # If folder_path already starts with root or client_name, don't double-prefix
    if root and fp.startswith(root + "/"):
        return fp
    
    if client_name and fp.startswith(client_name + "/"):
        # Still prefix with root if provided
        return "/".join([root, fp]) if root else fp
        
    parts = [p for p in [root, client_name, folder_path] if p and p.strip("/")]
    return "/".join(parts)

def _normalize_path(full_path: str, client_name: str, source_type: str) -> str:
    """
    Strips the root folder prefix and client prefix from a full blob path
    so the returned file_path is relative to client root.
    e.g.  landing/AMGEN/clinical/file.csv  →  clinical/file.csv
    """
    root = os.getenv("ADLS_ROOT_FOLDER", "").strip("/")
    # Strip root folder prefix
    if root:
        root_prefix = root + "/"
        if full_path.startswith(root_prefix):
            full_path = full_path[len(root_prefix):]
    # Strip client prefix
    client_prefix = f"{client_name}/"
    if full_path.startswith(client_prefix):
        full_path = full_path[len(client_prefix):]
    return full_path

# ---------------------------------------------------------
# TOOLS
# ---------------------------------------------------------

@mcp.tool()
def list_datasets(source_type: str, client_name: str, folder_path: str, container: Optional[str] = None) -> str:
    """
    Lists datasets from ADLS (Azure Data Lake Storage).
    Returns a JSON string of List[DatasetInfo].
    """
    logger.info(f"MCP Tool: list_datasets called for {source_type} - {client_name} - container: {container}")
    
    datasets = []
    
    if source_type == SourceType.ADLS.value:
        # ADLS LOGIC
        container_name = container or os.getenv("ADLS_CONTAINER_NAME", "ag-de-agent")
        try:
            fs_client = _get_fs_client(container_name)
            full_search = _build_search_path(client_name, folder_path)
            try:
                paths = list(fs_client.get_paths(path=full_search if full_search else None, recursive=True))
            except Exception as path_err:
                if "PathNotFound" in str(path_err) or "404" in str(path_err):
                    return json.dumps([])
                raise
            for path in paths:
                if path.is_directory:
                    continue
                file_name = os.path.basename(path.name)
                canonical_path = _normalize_path(path.name, client_name, source_type)
                datasets.append({
                    "file_name": file_name,
                    "file_path": canonical_path,
                    "file_format": file_name.split(".")[-1].upper() if "." in file_name else "UNKNOWN",
                    "file_size": path.content_length or 0,
                    "source_type": source_type,
                    "client_name": client_name
                })
        except Exception as e:
            return json.dumps({"error": str(e)})
            
    else:
        return json.dumps({"error": f"Unsupported source type: {source_type}"})

    return json.dumps(datasets)


@mcp.tool()
def list_children(source_type: str, client_name: str, folder_path: str, container: Optional[str] = None) -> str:
    """
    Lists immediate child folders and files at the given path.
    Returns a JSON string: {"folders": [str], "files": [DatasetInfo-like dicts]}
    """
    logger.info(f"MCP Tool: list_children called for {source_type} - {client_name} - container: {container} - folder: {folder_path}")

    result = {"folders": [], "files": []}

    if source_type == SourceType.ADLS.value:
        container_name = container or os.getenv("ADLS_CONTAINER_NAME", "ag-de-agent")
        try:
            fs_client = _get_fs_client(container_name)
            full_search = _build_search_path(client_name, folder_path)
            folders_set = set()
            try:
                all_paths = list(fs_client.get_paths(path=full_search if full_search else None, recursive=True))
            except Exception as path_err:
                if "PathNotFound" in str(path_err) or "404" in str(path_err):
                    return json.dumps({"folders": [], "files": []})
                raise
            for path in all_paths:
                rel = path.name[len(full_search):].lstrip("/") if full_search else path.name
                if not rel:
                    continue
                if path.is_directory:
                    child = rel.split("/")[0]
                    if child:
                        folders_set.add(child)
                else:
                    if "/" in rel:
                        continue
                    file_name = os.path.basename(path.name)
                    canonical_path = _normalize_path(path.name, client_name, source_type)
                    result["files"].append({
                        "file_name": file_name,
                        "file_path": canonical_path,
                        "file_format": file_name.split(".")[-1].upper() if "." in file_name else "UNKNOWN",
                        "file_size": path.content_length or 0,
                        "source_type": source_type,
                        "client_name": client_name
                    })
            result["folders"] = sorted(list(folders_set))
        except Exception as e:
            return json.dumps({"error": str(e)})
    else:
        return json.dumps({"error": f"Unsupported source type: {source_type}"})

    return json.dumps(result)


@mcp.tool()
def get_file_content_base64(source_type: str, client_name: str, file_path_canonical: str, container: Optional[str] = None) -> str:
    """
    Reads file content and returns it as base64 string.
    """
    import base64
    
    logger.info(f"MCP Tool: get_file_content called for {file_path_canonical} - container: {container}")

    try:
        content_bytes = b""
        
        if source_type == SourceType.ADLS.value:
             container_name = container or os.getenv("ADLS_CONTAINER_NAME", "ag-de-agent")
             fs_client = _get_fs_client(container_name)
             full_path = _build_search_path(client_name, file_path_canonical)
             file_client = fs_client.get_file_client(full_path)
             content_bytes = file_client.download_file().readall()
        
        else:
             return json.dumps({"error": "Unsupported Source"})

        # Return as Base64 String
        b64_str = base64.b64encode(content_bytes).decode('utf-8')
        return json.dumps({"content_base64": b64_str})

    except Exception as e:
        return json.dumps({"error": str(e)})




# ---------------------------------------------------------
# API SOURCE TOOLS  (dynamic — reads config from DB per client)
# ---------------------------------------------------------

def _get_api_config(client_name: str, folder_path: str) -> dict:
    """Reads API source config from DB for the given client."""
    import sys as _sys
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if project_root not in _sys.path:
        _sys.path.insert(0, project_root)

    from core.database import SessionLocal
    from models.api_source_config import APISourceConfig

    db = SessionLocal()
    try:
        configs = db.query(APISourceConfig).filter(
            APISourceConfig.client_name == client_name,
            APISourceConfig.is_active   == True
        ).all()

        if not configs:
            raise ValueError(
                f"No active API config for client '{client_name}'. "
                f"Register one via POST /api-source/register"
            )

        # Match config whose endpoints list contains folder_path
        ep = folder_path.strip("/").split("/")[0] if folder_path else ""
        matched = next(
            (c for c in configs
             if ep in [e.strip() for e in (c.endpoints or "").split(",")]),
            configs[0]
        )

        return {
            "base_url":       matched.base_url.rstrip("/"),
            "auth_type":      matched.auth_type or "none",
            "auth_token":     matched.auth_token or "",
            "api_key_header": matched.api_key_header or "X-Api-Key",
            "endpoints":      [e.strip() for e in (matched.endpoints or "").split(",") if e.strip()],
            "source_name":    matched.source_name,
        }
    finally:
        db.close()


def _build_url(endpoint_url: str, cfg: dict) -> str:
    """
    Appends extra query params if needed per API convention.
    """
    extra = cfg.get("extra_params", "")
    if extra:
        sep = "&" if "?" in endpoint_url else "?"
        return endpoint_url + sep + extra
    # World Bank API defaults to XML — force JSON
    if "worldbank.org" in endpoint_url and "format=" not in endpoint_url:
        sep = "&" if "?" in endpoint_url else "?"
        return endpoint_url + sep + "format=json&per_page=300"
    # NewsAPI — needs country or query param to return results
    if "newsapi.org" in endpoint_url:
        sep = "&" if "?" in endpoint_url else "?"
        if "top-headlines" in endpoint_url and "country=" not in endpoint_url and "q=" not in endpoint_url:
            return endpoint_url + sep + "country=us&pageSize=100"
        if "everything" in endpoint_url and "q=" not in endpoint_url:
            return endpoint_url + sep + "q=health&pageSize=100"
    # Alpha Vantage — needs function param
    if "alphavantage.co" in endpoint_url and "function=" not in endpoint_url:
        sep = "&" if "?" in endpoint_url else "?"
        endpoint_name = endpoint_url.split("/")[-1]
        return endpoint_url + sep + f"function={endpoint_name}&symbol=IBM"
    # Open Meteo — needs lat/lon
    if "open-meteo.com" in endpoint_url and "latitude=" not in endpoint_url:
        sep = "&" if "?" in endpoint_url else "?"
        return endpoint_url + sep + "latitude=52.52&longitude=13.41&current_weather=true"
    return endpoint_url


def _is_direct_file_url(url: str) -> bool:
    """Returns True if URL points directly to a file (CSV/JSON/parquet)."""
    path = url.split("?")[0].lower()
    return any(path.endswith(ext) for ext in (".csv", ".json", ".parquet", ".xlsx", ".tsv"))


def _fetch_direct_file(url: str, cfg: dict) -> bytes:
    """Downloads a direct file URL and returns raw bytes."""
    import urllib.request, base64 as _b64
    headers = {"Accept": "*/*", "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
    auth = cfg.get("auth_type", "none").lower()
    token = cfg.get("auth_token") or ""
    if auth == "bearer" and token:
        headers["Authorization"] = f"Bearer {token}"
    elif auth == "apikey" and token:
        headers[cfg.get("api_key_header", "X-Api-Key")] = token
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read()


def _call_api(endpoint_url: str, cfg: dict) -> list:
    """Calls the REST API and normalises response to a list of dicts."""
    import urllib.request, base64 as _b64

    url = _build_url(endpoint_url, cfg)
    headers = {"Accept": "application/json", "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
    auth    = cfg["auth_type"].lower()
    token   = cfg["auth_token"]

    if auth == "bearer" and token:
        headers["Authorization"] = f"Bearer {token}"
    elif auth == "basic" and token:
        headers["Authorization"] = f"Basic {_b64.b64encode(token.encode()).decode()}"
    elif auth == "apikey" and token:
        headers[cfg["api_key_header"]] = token

    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw_bytes = resp.read()

        if not raw_bytes or not raw_bytes.strip():
            raise ValueError(f"API returned empty response from {url}")

        # Strip UTF-8 BOM if present (World Bank API quirk)
        raw_text = raw_bytes.decode("utf-8-sig").strip()

        # If response looks like XML, raise a helpful error
        if raw_text.startswith("<"):
            raise ValueError(
                f"API returned XML instead of JSON. "
                f"Try adding ?format=json to the base_url. URL was: {url}"
            )

        data = json.loads(raw_text)

        # World Bank wraps response as [{{page_meta}}, [actual_records]]
        if isinstance(data, list) and len(data) == 2:
            if isinstance(data[0], dict) and isinstance(data[1], list):
                data = data[1]

    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        # Try common envelope keys used by different APIs
        for key in (
            "data", "results", "items", "records", "value",
            "articles",      # NewsAPI
            "sources",       # NewsAPI /sources
            "breeds",        # Dog API
            "countries",     # REST Countries style
            "hits",          # Algolia / some search APIs
            "entries",       # some feed APIs
            "response",      # some wrapped APIs
            "content",       # generic
            "list",          # generic list wrapper
        ):
            if key in data and isinstance(data[key], list):
                return data[key]
        # Last resort — find any list value in the top-level dict
        for key, val in data.items():
            if isinstance(val, list) and len(val) > 0:
                return val
        return [data]
    raise ValueError(f"Unexpected API response type: {type(data).__name__}")


def _records_to_csv_bytes(records: list) -> bytes:
    """Converts a list of dicts to CSV bytes. Also handles flat lists of strings/values."""
    import io, csv
    if not records: return b""
    
    # If it is a flat list of items (e.g. ["animal", "career"])
    if not isinstance(records[0], dict):
        records = [{"value": v} for v in records]

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(records[0].keys()), extrasaction="ignore")
    writer.writeheader()
    writer.writerows(records)
    return buf.getvalue().encode("utf-8")


@mcp.tool()
def list_api_datasets(source_type: str, client_name: str, folder_path: str) -> str:
    """
    Lists datasets from a REST API or direct file URL.
    Config is read from DB by client_name — register first via POST /api-source/register.
    folder_path = API endpoint name e.g. "users" OR full file path e.g. "agent-data/file.csv"
    """
    import io, csv
    try:
        cfg = _get_api_config(client_name, folder_path)
        ep  = folder_path.strip("/")
        endpoint_url = f"{cfg['base_url']}/{ep}" if ep else cfg["base_url"]

        # Detect direct file URL (S3, blob, etc.)
        if _is_direct_file_url(endpoint_url):
            raw_bytes = _fetch_direct_file(endpoint_url, cfg)
            file_name = endpoint_url.split("/")[-1].split("?")[0]
            ep_name   = ep.replace("/", "_")
            return json.dumps([{
                "file_name":   file_name,
                "file_path":   f"{ep}/{file_name}",
                "file_format": "CSV" if file_name.endswith(".csv") else "JSON",
                "file_size":   len(raw_bytes),
                "source_type": source_type,
                "client_name": client_name,
            }])

        records   = _call_api(endpoint_url, cfg)
        ep_name   = ep.replace("/", "_") or "api_data"
        file_name = f"{ep_name}.csv"
        size      = len(_records_to_csv_bytes(records)) if records else 0

        return json.dumps([{
            "file_name":   file_name,
            "file_path":   f"{ep}/{file_name}" if ep else file_name,
            "file_format": "CSV",
            "file_size":   size,
            "source_type": source_type,
            "client_name": client_name,
        }])
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_api_file_content(source_type: str, client_name: str, file_path_canonical: str) -> str:
    """
    Fetches data from the registered API and returns base64-encoded CSV.
    file_path_canonical: "<endpoint>/<filename>.csv"
    """
    import base64 as _b64
    try:
        cfg   = _get_api_config(client_name, file_path_canonical)
        # Handle absolute Azure URIs (prevents 404 in urllib)
        if file_path_canonical.startswith("az://"):
             from core.azure_storage import AzureStorageClient
             container_name, key = AzureStorageClient().parse_az_url(file_path_canonical)
             fs_client = _get_fs_client(container_name)
             file_client = fs_client.get_file_client(key)
             raw_bytes = file_client.download_file().readall()
             return json.dumps({"content_base64": _b64.b64encode(raw_bytes).decode("utf-8")})
             
        parts = file_path_canonical.strip("/").split("/")
        ep    = "/".join(parts[:-1]) if len(parts) > 1 else parts[0].replace(".csv", "")
        endpoint_url = f"{cfg['base_url']}/{ep}" if ep else cfg["base_url"]

        # Detect direct file URL (S3 public, Azure blob public, etc.)
        if _is_direct_file_url(endpoint_url):
            raw_bytes = _fetch_direct_file(endpoint_url, cfg)
            return json.dumps({"content_base64": _b64.b64encode(raw_bytes).decode("utf-8")})

        records = _call_api(endpoint_url, cfg)
        if not records:
            return json.dumps({"error": "API returned empty dataset"})

        csv_bytes = _records_to_csv_bytes(records)
        return json.dumps({"content_base64": _b64.b64encode(csv_bytes).decode("utf-8")})
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def list_api_children(source_type: str, client_name: str, folder_path: str) -> str:
    """Lists available API endpoints as virtual folders from DB config."""
    try:
        cfg       = _get_api_config(client_name, folder_path)
        endpoints = cfg.get("endpoints", [])

        if not folder_path.strip("/"):
            return json.dumps({"folders": endpoints, "files": []})

        ep      = folder_path.strip("/")
        ep_name = ep.replace("/", "_")
        return json.dumps({
            "folders": [],
            "files": [{
                "file_name":   f"{ep_name}.csv",
                "file_path":   f"{ep}/{ep_name}.csv",
                "file_format": "CSV",
                "file_size":   0,
                "source_type": source_type,
                "client_name": client_name,
            }]
        })
    except Exception as e:
        return json.dumps({"error": str(e)})


if __name__ == "__main__":
    # Load env vars manually if running standalone
    from dotenv import load_dotenv
    load_dotenv()
    mcp.run()
