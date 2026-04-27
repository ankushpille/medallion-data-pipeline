"""
azure_storage.py
================
Central Azure Blob Storage helper that replaces all boto3 / S3 usage in the project.

AWS → Azure mapping:
  S3 bucket              → Azure Blob Storage container
  s3.put_object()        → BlobClient.upload_blob()
  s3.get_object()        → BlobClient.download_blob()
  s3.list_objects_v2()   → ContainerClient.list_blobs()
  s3://bucket/key        → az://container/blob_path  (internal convention)
  presigned URL          → BlobClient.generate_sas_url()
"""

import io
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Tuple

from azure.storage.blob import (
    BlobServiceClient,
    BlobClient,
    ContainerClient,
    generate_blob_sas,
    BlobSasPermissions,
)
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from loguru import logger

from core.settings import settings


# ---------------------------------------------------------------------------
# Authentication helper
# ---------------------------------------------------------------------------

def _get_blob_service_client() -> BlobServiceClient:
    """
    Returns an authenticated BlobServiceClient.

    Priority:
    1. Connection string  (AZURE_STORAGE_CONNECTION_STRING)
    2. Service Principal  (AZURE_CLIENT_ID + AZURE_CLIENT_SECRET + AZURE_TENANT_ID)
    3. DefaultAzureCredential (Managed Identity, az login, env vars…)
    """
    conn_str = settings.AZURE_STORAGE_CONNECTION_STRING
    if conn_str:
        return BlobServiceClient.from_connection_string(conn_str)

    account_name = settings.AZURE_STORAGE_ACCOUNT
    if not account_name:
        raise ValueError(
            "Either AZURE_STORAGE_CONNECTION_STRING or AZURE_STORAGE_ACCOUNT must be set."
        )

    account_url = f"https://{account_name}.blob.core.windows.net"

    client_id = settings.AZURE_CLIENT_ID
    client_secret = settings.AZURE_CLIENT_SECRET
    tenant_id = settings.AZURE_TENANT_ID

    if client_id and client_secret and tenant_id:
        credential = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
        )
    else:
        credential = DefaultAzureCredential()

    return BlobServiceClient(account_url=account_url, credential=credential)


# ---------------------------------------------------------------------------
# AzureStorageClient  (drop-in replacement for boto3 s3 client)
# ---------------------------------------------------------------------------

class AzureStorageClient:
    """
    Thin wrapper around BlobServiceClient exposing S3-like methods so
    the rest of the codebase needs minimal changes.

    All methods accept a `container` parameter (equivalent to S3 'Bucket')
    and a `key` / `prefix` parameter (equivalent to S3 'Key' / 'Prefix').

    The default container is AZURE_CONTAINER_NAME from settings.
    """

    def __init__(self):
        self._client = _get_blob_service_client()
        self.default_container = settings.AZURE_CONTAINER_NAME or "datalake"

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def put_object(
        self,
        Key: str,
        Body: bytes,
        Container: Optional[str] = None,
        ContentType: str = "application/octet-stream",
    ) -> None:
        """Equivalent to s3.put_object(Bucket=..., Key=..., Body=...)"""
        container = Container or self.default_container
        blob_client: BlobClient = self._client.get_blob_client(
            container=container, blob=Key
        )
        blob_client.upload_blob(Body, overwrite=True, content_settings=None)
        logger.info(f"Uploaded blob: {container}/{Key} ({len(Body)} bytes)")

    def upload_fileobj(
        self,
        Fileobj: io.IOBase,
        Container: Optional[str] = None,
        Key: str = "",
    ) -> None:
        """Equivalent to s3.upload_fileobj(Fileobj, Bucket, Key)"""
        container = Container or self.default_container
        blob_client: BlobClient = self._client.get_blob_client(
            container=container, blob=Key
        )
        blob_client.upload_blob(Fileobj, overwrite=True)
        logger.info(f"Uploaded fileobj: {container}/{Key}")

    def delete_object(self, Container: Optional[str] = None, Key: str = "") -> None:
        """Equivalent to s3.delete_object(Bucket=..., Key=...)"""
        container = Container or self.default_container
        blob_client: BlobClient = self._client.get_blob_client(
            container=container, blob=Key
        )
        blob_client.delete_blob()
        logger.info(f"Deleted blob: {container}/{Key}")

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def get_object(self, Key: str, Container: Optional[str] = None) -> dict:
        """
        Equivalent to s3.get_object(Bucket=..., Key=...).
        Returns a dict with a 'Body' key whose .read() returns bytes.
        """
        container = Container or self.default_container
        blob_client: BlobClient = self._client.get_blob_client(
            container=container, blob=Key
        )
        downloader = blob_client.download_blob()
        data = downloader.readall()
        logger.info(f"Downloaded blob: {container}/{Key} ({len(data)} bytes)")

        # Wrap in a file-like object to mimic boto3 response shape
        class _Body:
            def __init__(self, b: bytes):
                self._buf = io.BytesIO(b)

            def read(self) -> bytes:
                return self._buf.read()

        return {"Body": _Body(data), "ContentLength": len(data)}

    # ------------------------------------------------------------------
    # List
    # ------------------------------------------------------------------

    def list_containers(self) -> List[str]:
        """
        Lists all containers in the storage account.
        """
        containers = self._client.list_containers()
        names = [c.name for c in containers]
        logger.info(f"Listed {len(names)} containers in account.")
        return names


    def list_objects_v2(
        self,
        Prefix: str = "",
        Delimiter: str = "",
        Container: Optional[str] = None,
    ) -> dict:
        """
        Equivalent to s3.list_objects_v2(Bucket=..., Prefix=..., Delimiter=...).
        Returns a dict compatible with boto3's response shape:
          { "Contents": [...], "CommonPrefixes": [...], "KeyCount": int }
        """
        container = Container or self.default_container
        container_client: ContainerClient = self._client.get_container_client(container)

        contents = []
        common_prefixes_set = set()

        if Delimiter:
            # Simulate delimiter behaviour (virtual directory listing)
            blobs = container_client.walk_blobs(name_starts_with=Prefix, delimiter=Delimiter)
            for item in blobs:
                if hasattr(item, "prefix"):          # BlobPrefix (virtual folder)
                    common_prefixes_set.add(item.prefix)
                else:                                # BlobProperties (file)
                    contents.append({"Key": item.name, "Size": item.size or 0})
        else:
            for blob in container_client.list_blobs(name_starts_with=Prefix):
                contents.append({"Key": blob.name, "Size": blob.size or 0})

        common_prefixes = [{"Prefix": p} for p in sorted(common_prefixes_set)]

        return {
            "Contents": contents,
            "CommonPrefixes": common_prefixes,
            "KeyCount": len(contents),
        }

    # ------------------------------------------------------------------
    # Presigned / SAS URL  (replaces s3.generate_presigned_url)
    # ------------------------------------------------------------------

    def generate_presigned_url(
        self,
        operation: str,
        Params: dict,
        ExpiresIn: int = 3600,
        Container: Optional[str] = None,
    ) -> str:
        """
        Equivalent to s3.generate_presigned_url("get_object", Params={...}, ExpiresIn=...).
        Returns a SAS URL for the blob.
        """
        container = Container or Params.get("Bucket") or self.default_container
        key = Params.get("Key", "")

        account_name = settings.AZURE_STORAGE_ACCOUNT
        account_key = None

        # Try to extract account key from connection string for SAS signing
        conn_str = settings.AZURE_STORAGE_CONNECTION_STRING or ""
        for part in conn_str.split(";"):
            if part.startswith("AccountKey="):
                account_key = part[len("AccountKey="):]

        if not account_key:
            # Fallback: return direct blob URL (works with public containers or Managed Identity)
            blob_client = self._client.get_blob_client(container=container, blob=key)
            logger.warning(
                "Cannot generate SAS URL without account key; returning direct blob URL."
            )
            return blob_client.url

        sas_token = generate_blob_sas(
            account_name=account_name,
            container_name=container,
            blob_name=key,
            account_key=account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.now(timezone.utc) + timedelta(seconds=ExpiresIn),
        )
        return (
            f"https://{account_name}.blob.core.windows.net/{container}/{key}?{sas_token}"
        )

    # ------------------------------------------------------------------
    # URL parser  (replaces internal _parse_s3)
    # ------------------------------------------------------------------

    @staticmethod
    def parse_az_url(url: str) -> Tuple[str, str]:
        """
        Parses Azure storage URIs in two formats:
          1. az://account/container/blob_path  (frontend format)
          2. az://container/blob_path           (legacy internal format)
          3. https://<account>.blob.core.windows.net/<container>/<blob>

        Returns (container, blob_path).
        """
        if url.startswith("az://"):
            rest = url[len("az://"):].lstrip()
        elif "blob.core.windows.net" in url:
            # https://<account>.blob.core.windows.net/<container>/<blob>
            rest = url.split(".blob.core.windows.net/", 1)[-1].lstrip()
        else:
            rest = url.lstrip()  # treat as container/blob
        
        # rest is now: account/container/blob  OR  container/blob
        # Detect if the first segment is the storage account name (not a container).
        # Container names must be <= 63 chars, lowercase alphanumeric/hyphen.
        # Storage account names follow the same rules but are also set in settings.
        from core.settings import settings
        account_name = (settings.AZURE_STORAGE_ACCOUNT or "").lower()
        parts = rest.split("/", 1)
        first_segment = parts[0].lower()

        if account_name and first_segment == account_name:
            # Strip the account prefix → remaining is container/blob_path
            rest = parts[1] if len(parts) > 1 else ""
            parts = rest.split("/", 1)

        container = parts[0]
        blob = parts[1] if len(parts) > 1 else ""
        # URL-decode the blob path so that spaces (%20), parens, etc. are handled correctly
        # by the Azure SDK (e.g. "landing/analytics_data%20(2).csv" → "landing/analytics_data (2).csv")
        from urllib.parse import unquote
        blob = unquote(blob)
        return container, blob

    def delete_directory(self, Container: Optional[str] = None, Prefix: str = "") -> None:
        """
        Deletes a hierarchical directory recursively in Azure Data Lake Gen2.
        Fallback to manual multi-blob deletion if DataLake Client is not available.
        """
        container = Container or self.default_container
        prefix = Prefix.strip("/")
        if not prefix: return
        
        try:
            from azure.storage.filedatalake import DataLakeServiceClient
            from azure.core.exceptions import ResourceNotFoundError
            
            # Authenticate similarly to Blob client
            account_name = settings.AZURE_STORAGE_ACCOUNT
            conn_str = settings.AZURE_STORAGE_CONNECTION_STRING
            
            if conn_str:
                dl_client = DataLakeServiceClient.from_connection_string(conn_str)
            else:
                from azure.identity import DefaultAzureCredential
                url = f"https://{account_name}.dfs.core.windows.net"
                dl_client = DataLakeServiceClient(account_url=url, credential=DefaultAzureCredential())
                
            fs_client = dl_client.get_file_system_client(file_system=container)
            dir_client = fs_client.get_directory_client(prefix)
            dir_client.delete_directory()
            logger.info(f"Deleted directory recursively: {container}/{prefix}")
            
        except ResourceNotFoundError:
            # Expected if a certain layer (e.g. Reports) doesn't exist yet for this client
            logger.debug(f"Directory {prefix} not found, skipping delete.")
            return
        except ImportError:
            logger.warning("DataLake SDK not found. Falling back to multi-blob deletion.")
            self._manual_delete_recursive(container, Prefix)
        except Exception as e:
            # If fail, try to delete all blobs anyway
            logger.warning(f"DataLake directory delete failed ({e}). Falling back to multi-blob deletion.")
            self._manual_delete_recursive(container, Prefix)

    def _manual_delete_recursive(self, container: str, prefix: str):
        """Helper to delete all blobs under a prefix, deepest first."""
        objs = self.list_objects_v2(Prefix=prefix, Container=container)
        contents = objs.get("Contents", [])
        if not contents:
            return
            
        # Sort by key length descending to delete leaves before branches
        contents.sort(key=lambda x: len(x["Key"]), reverse=True)
        
        for obj in contents:
            try:
                self.delete_object(Container=container, Key=obj["Key"])
            except Exception as e:
                # Still might hit DirectoryIsNotEmpty if there are true ADLS directories 
                # that weren't in the blob list, or race conditions.
                logger.debug(f"Manual delete failed for {obj['Key']}: {e}")


# ---------------------------------------------------------------------------
# Convenience factory (singleton-ish)
# ---------------------------------------------------------------------------

def get_storage_client() -> AzureStorageClient:
    """Returns a configured AzureStorageClient."""
    return AzureStorageClient()
