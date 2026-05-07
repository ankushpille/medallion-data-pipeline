import asyncio
import base64
import json
import time
from typing import Any, Dict, List, Optional

import httpx
from fastapi import HTTPException

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
UNIVERSAL_PREVIEW_NOTEBOOK_NAME = "RuntimeUniversalPreviewNotebook"


def _first_key(data: Any, candidates: List[str]) -> Any:
    wanted = {item.lower() for item in candidates}
    if isinstance(data, dict):
        for key, value in data.items():
            if str(key).lower() in wanted:
                return value
            found = _first_key(value, candidates)
            if found is not None:
                return found
    elif isinstance(data, list):
        for item in data:
            found = _first_key(item, candidates)
            if found is not None:
                return found
    return None


def _normalize_format(value: Optional[str]) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return "csv"
    aliases = {
        "text": "txt",
        "delimitedtext": "csv",
    }
    return aliases.get(text, text)


def build_universal_preview_notebook_source(preview_request: Dict[str, Any]) -> str:
    request_json = json.dumps(preview_request, ensure_ascii=True)
    return f"""import json
import re

try:
    import notebookutils
except Exception:  # pragma: no cover
    import mssparkutils as notebookutils

REQUEST = json.loads(r'''{request_json}''')
RESULT = {{}}


def compact(value):
    if isinstance(value, dict):
        return {{k: compact(v) for k, v in value.items() if v not in (None, "", [], {{}})}}
    if isinstance(value, list):
        return [compact(item) for item in value if item not in (None, "", [], {{}})]
    return value


def first_non_empty(*values):
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        if isinstance(value, (list, dict)) and not value:
            continue
        return value
    return None


def build_lakehouse_abfss(meta):
    workspace_id = meta.get("workspaceId") or meta.get("workspace_id")
    artifact_id = meta.get("artifactId") or meta.get("artifact_id")
    resolved_path = meta.get("resolvedPath") or meta.get("resolved_path")
    if workspace_id and artifact_id and resolved_path:
        item_type = meta.get("itemType") or "Lakehouse"
        return f"abfss://{{workspace_id}}@onelake.dfs.fabric.microsoft.com/{{artifact_id}}.{{item_type}}/{{resolved_path.lstrip('/')}}"
    return None


def build_storage_path(spec):
    meta = spec.get("connection_metadata") or {{}}
    source_type = str(spec.get("source_type") or "").lower()
    connector_type = str(spec.get("connector_type") or "").lower()
    explicit = first_non_empty(meta.get("previewPath"), meta.get("fullPath"), meta.get("path"))
    if explicit:
        return explicit
    if "lakehouse" in source_type or "lakehouse" in connector_type or "onelake" in source_type:
        return build_lakehouse_abfss(meta)
    if meta.get("abfssPath"):
        return meta["abfssPath"]
    if meta.get("s3Path"):
        return meta["s3Path"]
    if meta.get("blobPath"):
        return meta["blobPath"]
    return meta.get("resolvedPath") or meta.get("resolved_path")


def normalize_strategy(spec):
    strategy = str(spec.get("preview_strategy") or "").strip().lower()
    fmt = str(spec.get("format") or "").strip().lower()
    connector = str(spec.get("connector_type") or "").strip().lower()
    meta = spec.get("connection_metadata") or {{}}
    if strategy:
        return strategy
    if any(key in meta for key in ("endpoint", "url")) or connector in {{"rest", "restapi", "http", "graphql", "soap"}}:
        return "rest_api"
    if any(key in meta for key in ("jdbc_url", "table", "query")):
        return "spark_jdbc"
    if fmt in {{"delta"}}:
        return "spark_delta"
    if fmt in {{"csv", "txt", "tsv"}}:
        return "spark_delimited"
    if fmt:
        return f"spark_{{fmt}}"
    return "spark_file"


def schema_payload(df):
    return [
        {{
            "column_name": field.name,
            "data_type": field.dataType.simpleString(),
            "nullable": field.nullable,
            "ordinal_position": index + 1,
        }}
        for index, field in enumerate(df.schema.fields)
    ]


def datatypes(df):
    return [field.dataType.simpleString() for field in df.schema.fields]


def nullable_columns(df):
    return [field.name for field in df.schema.fields if field.nullable]


def preview_rows(df, limit_rows=25):
    return [row.asDict(recursive=True) for row in df.limit(limit_rows).collect()]


def build_reader(spec):
    fmt = str(spec.get("format") or "").strip().lower()
    strategy = normalize_strategy(spec)
    path = build_storage_path(spec)
    read_options = dict(spec.get("read_options") or {{}})
    read_options = {{k: v for k, v in read_options.items() if v is not None}}
    reader = spark.read.options(**read_options)
    meta = spec.get("connection_metadata") or {{}}

    if strategy == "spark_delimited":
        return reader.csv(path)
    if strategy == "spark_json":
        return reader.json(path)
    if strategy == "spark_parquet":
        return reader.parquet(path)
    if strategy == "spark_delta":
        return reader.format("delta").load(path)
    if strategy in {{"spark_orc", "spark_avro", "spark_xml", "spark_excel"}}:
        return reader.format(fmt).load(path)
    if strategy == "spark_jdbc":
        jdbc_options = {{}}
        jdbc_url = first_non_empty(meta.get("jdbc_url"), meta.get("connectionString"), meta.get("url"))
        if jdbc_url:
            jdbc_options["url"] = jdbc_url
        dbtable = first_non_empty(meta.get("table"), meta.get("dbtable"))
        query = meta.get("query")
        if query:
            jdbc_options["query"] = query
        elif dbtable:
            jdbc_options["dbtable"] = dbtable
        for key in ("user", "password", "driver"):
            if meta.get(key):
                jdbc_options[key] = meta.get(key)
        return spark.read.format("jdbc").options(**jdbc_options).load()
    if strategy == "rest_api":
        import requests

        endpoint = first_non_empty(meta.get("endpoint"), meta.get("url"))
        headers = meta.get("headers") or {{}}
        response = requests.get(endpoint, headers=headers, timeout=60)
        response.raise_for_status()
        content_type = (response.headers.get("content-type") or "").lower()
        if "json" in content_type:
            payload = response.json()
            if isinstance(payload, list):
                rows = payload
            elif isinstance(payload, dict):
                rows = payload.get("value") or payload.get("data") or payload.get("items") or [payload]
            else:
                rows = [{{"value": payload}}]
            if rows and not isinstance(rows[0], dict):
                rows = [{{"value": item}} for item in rows]
            return spark.createDataFrame(rows)
        return spark.createDataFrame([{{"value": response.text}}])
    if path:
        if fmt == "json":
            return reader.json(path)
        if fmt == "parquet":
            return reader.parquet(path)
        if fmt == "delta":
            return reader.format("delta").load(path)
        return reader.load(path)
    raise ValueError(f"Unable to resolve a preview reader for strategy={{strategy}} path={{path}}")


def run_preview():
    spec = REQUEST.get("resolved_source") or REQUEST
    strategy = normalize_strategy(spec)
    resolved_path = build_storage_path(spec)
    df = build_reader(spec)
    rows = preview_rows(df)
    schema = schema_payload(df)
    return compact({{
        "preview_rows": rows,
        "schema": schema,
        "columns": [field["column_name"] for field in schema],
        "datatypes": datatypes(df),
        "nullable_columns": nullable_columns(df),
        "row_count_estimate": len(rows),
        "resolved_source": spec,
        "resolved_path": resolved_path,
        "preview_strategy": strategy,
        "runtime_statistics": REQUEST.get("runtime_statistics") or {{}},
    }})


try:
    RESULT = run_preview()
except Exception as exc:
    RESULT = {{
        "preview_error": str(exc),
        "resolved_source": REQUEST.get("resolved_source") or REQUEST,
        "resolved_path": build_storage_path(REQUEST.get("resolved_source") or REQUEST),
        "preview_strategy": normalize_strategy(REQUEST.get("resolved_source") or REQUEST),
    }}

notebookutils.notebook.exit(json.dumps(RESULT, default=str))
"""


class FabricUniversalPreviewService:
    _NOTEBOOK_CACHE: Dict[str, Dict[str, Any]] = {}

    def __init__(self, access_token: str):
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

    async def _request(self, method: str, url: str, **kwargs) -> httpx.Response:
        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.request(method, url, headers=self.headers, **kwargs)
            return response

    @staticmethod
    def _json_object(response: httpx.Response) -> Dict[str, Any]:
        try:
            body = response.json()
        except Exception:
            return {}
        return body if isinstance(body, dict) else {}

    @classmethod
    def _cache_key(cls, workspace_id: str, display_name: str = UNIVERSAL_PREVIEW_NOTEBOOK_NAME) -> str:
        return f"{workspace_id}:{display_name}"

    @classmethod
    def _get_cached_notebook(cls, workspace_id: str, display_name: str = UNIVERSAL_PREVIEW_NOTEBOOK_NAME) -> Optional[Dict[str, Any]]:
        cached = cls._NOTEBOOK_CACHE.get(cls._cache_key(workspace_id, display_name))
        if not cached or not cached.get("resolved") or not cached.get("notebookId"):
            return None
        return cached

    @classmethod
    def _set_cached_notebook(cls, workspace_id: str, notebook: Dict[str, Any]) -> Dict[str, Any]:
        cached = {
            "workspaceId": workspace_id,
            "notebookId": notebook.get("id"),
            "displayName": notebook.get("displayName") or notebook.get("name") or UNIVERSAL_PREVIEW_NOTEBOOK_NAME,
            "resolved": bool(notebook.get("id")),
            "cachedAt": time.time(),
        }
        cls._NOTEBOOK_CACHE[cls._cache_key(workspace_id, cached["displayName"])] = cached
        return cached

    async def _wait_lro(self, response: httpx.Response) -> Dict[str, Any]:
        final_body: Dict[str, Any] = {}
        if response.status_code != 202:
            return self._json_object(response)
        location = response.headers.get("Location")
        retry_after = int(response.headers.get("Retry-After", "2") or "2")
        if not location:
            return final_body
        for _ in range(30):
            await asyncio.sleep(max(retry_after, 1))
            poll = await self._request("GET", location)
            poll_body = self._json_object(poll)
            if poll.status_code in (200, 201, 204):
                final_body = poll_body or final_body
                break
            if poll.status_code == 202:
                retry_after = int(poll.headers.get("Retry-After", "2") or "2")
                continue
            if poll.is_error:
                raise HTTPException(status_code=poll.status_code, detail=f"Fabric LRO failed: {poll.text}")
        result_url = f"{location.rstrip('/')}/result"
        result = await self._request("GET", result_url)
        if result.is_success:
            result_body = self._json_object(result)
            if result_body:
                final_body = result_body
        return final_body

    async def list_notebooks(self, workspace_id: str) -> List[Dict[str, Any]]:
        response = await self._request("GET", f"{FABRIC_API_BASE}/workspaces/{workspace_id}/notebooks")
        if not response.is_success:
            raise HTTPException(status_code=response.status_code, detail=f"List notebooks failed: {response.text}")
        try:
            body = response.json()
        except Exception:
            return []
        if isinstance(body, list):
            return [item for item in body if isinstance(item, dict)]
        if isinstance(body, dict):
            value = body.get("value")
            return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []
        return []

    async def list_workspace_items(self, workspace_id: str, item_type: Optional[str] = None) -> List[Dict[str, Any]]:
        url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items"
        if item_type:
            url = f"{url}?type={item_type}"
        response = await self._request("GET", url)
        if not response.is_success:
            return []
        try:
            body = response.json()
        except Exception:
            return []
        if isinstance(body, list):
            return [item for item in body if isinstance(item, dict)]
        if isinstance(body, dict):
            value = body.get("value")
            return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []
        return []

    async def get_notebook(self, workspace_id: str, notebook_id: str) -> Optional[Dict[str, Any]]:
        response = await self._request("GET", f"{FABRIC_API_BASE}/workspaces/{workspace_id}/notebooks/{notebook_id}")
        if response.status_code == 404:
            return None
        if not response.is_success:
            return None
        body = self._json_object(response)
        return body or None

    async def create_notebook(self, workspace_id: str, display_name: str, source_code: str) -> Dict[str, Any]:
        payload_b64 = base64.b64encode(source_code.encode("utf-8")).decode("utf-8")
        payload = {
            "displayName": display_name,
            "definition": {
                "parts": [
                    {
                        "path": "notebook-content.py",
                        "payload": payload_b64,
                        "payloadType": "InlineBase64",
                    }
                ]
            },
        }
        response = await self._request("POST", f"{FABRIC_API_BASE}/workspaces/{workspace_id}/notebooks", json=payload)
        if response.status_code not in (200, 201, 202):
            raise HTTPException(status_code=response.status_code, detail=f"Create notebook failed: {response.text}")
        lro_body = await self._wait_lro(response)
        body = lro_body or self._json_object(response)
        return {
            "body": body,
            "operationId": response.headers.get("x-ms-operation-id"),
            "requestId": response.headers.get("request-id") or response.headers.get("x-ms-request-id"),
            "location": response.headers.get("Location"),
            "notebookId": body.get("id") or _first_key(body, ["itemId", "artifactId", "notebookId"]),
            "displayName": body.get("displayName") or body.get("name") or display_name,
        }

    async def update_notebook_definition(self, workspace_id: str, notebook_id: str, source_code: str) -> None:
        payload_b64 = base64.b64encode(source_code.encode("utf-8")).decode("utf-8")
        payload = {
            "definition": {
                "parts": [
                    {
                        "path": "notebook-content.py",
                        "payload": payload_b64,
                        "payloadType": "InlineBase64",
                    }
                ]
            }
        }
        response = await self._request(
            "POST",
            f"{FABRIC_API_BASE}/workspaces/{workspace_id}/notebooks/{notebook_id}/updateDefinition?updateMetadata=false",
            json=payload,
        )
        if response.status_code not in (200, 202):
            raise HTTPException(status_code=response.status_code, detail=f"Update notebook definition failed: {response.text}")
        await self._wait_lro(response)

    async def _resolve_notebook_by_name(self, workspace_id: str, display_name: str) -> Optional[Dict[str, Any]]:
        notebooks = await self.list_notebooks(workspace_id)
        exact = next((item for item in notebooks if item.get("displayName") == display_name and str(item.get("type") or "Notebook").lower() == "notebook"), None)
        if exact and exact.get("id"):
            return exact
        items = await self.list_workspace_items(workspace_id, item_type="Notebook")
        exact = next((item for item in items if item.get("displayName") == display_name and str(item.get("type") or "Notebook").lower() == "notebook"), None)
        if exact and exact.get("id"):
            return exact
        recent = next((item for item in reversed(items) if item.get("displayName") == display_name), None)
        if recent and recent.get("id"):
            return recent
        return None

    async def _poll_for_notebook_resolution(
        self,
        workspace_id: str,
        display_name: str,
        notebook_id: Optional[str],
        diagnostics: List[Dict[str, Any]],
        max_retries: int = 20,
        base_delay_seconds: int = 3,
    ) -> Optional[Dict[str, Any]]:
        delay_seconds = base_delay_seconds
        for attempt in range(1, max_retries + 1):
            diagnostics.append({
                "state": "notebook_polling",
                "attempt": attempt,
                "workspaceId": workspace_id,
                "notebookId": notebook_id,
                "displayName": display_name,
                "delaySeconds": delay_seconds,
            })
            if notebook_id:
                by_id = await self.get_notebook(workspace_id, notebook_id)
                if by_id and by_id.get("id"):
                    diagnostics.append({
                        "state": "notebook_visible",
                        "resolution": "itemId",
                        "workspaceId": workspace_id,
                        "notebookId": by_id.get("id"),
                        "displayName": by_id.get("displayName"),
                    })
                    return by_id
            by_name = await self._resolve_notebook_by_name(workspace_id, display_name)
            if by_name and by_name.get("id"):
                diagnostics.append({
                    "state": "notebook_visible",
                    "resolution": "displayName",
                    "workspaceId": workspace_id,
                    "notebookId": by_name.get("id"),
                    "displayName": by_name.get("displayName"),
                })
                return by_name
            await asyncio.sleep(delay_seconds)
            delay_seconds = min(delay_seconds * 2, 12)
        return None

    async def ensure_preview_notebook(self, workspace_id: str, source_code: str, diagnostics: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        diagnostics = diagnostics if diagnostics is not None else []
        cached = self._get_cached_notebook(workspace_id)
        if cached:
            diagnostics.append({
                "state": "notebook_cached",
                "workspaceId": workspace_id,
                "notebookId": cached.get("notebookId"),
                "displayName": cached.get("displayName"),
            })
            existing = await self.get_notebook(workspace_id, cached["notebookId"])
            if existing and existing.get("id"):
                await self.update_notebook_definition(workspace_id, existing["id"], source_code)
                return existing

        existing = await self._resolve_notebook_by_name(workspace_id, UNIVERSAL_PREVIEW_NOTEBOOK_NAME)
        if existing and existing.get("id"):
            self._set_cached_notebook(workspace_id, existing)
            diagnostics.append({
                "state": "notebook_visible",
                "resolution": "precheck",
                "workspaceId": workspace_id,
                "notebookId": existing.get("id"),
                "displayName": existing.get("displayName"),
            })
            diagnostics.append({
                "state": "notebook_cached",
                "workspaceId": workspace_id,
                "notebookId": existing.get("id"),
                "displayName": existing.get("displayName"),
            })
            await self.update_notebook_definition(workspace_id, existing["id"], source_code)
            return existing

        diagnostics.append({
            "state": "notebook_create_started",
            "workspaceId": workspace_id,
            "displayName": UNIVERSAL_PREVIEW_NOTEBOOK_NAME,
        })
        created = await self.create_notebook(workspace_id, UNIVERSAL_PREVIEW_NOTEBOOK_NAME, source_code)
        diagnostics.append({
            "state": "notebook_create_completed",
            "workspaceId": workspace_id,
            "displayName": UNIVERSAL_PREVIEW_NOTEBOOK_NAME,
            "operationId": created.get("operationId"),
            "requestId": created.get("requestId"),
            "notebookId": created.get("notebookId"),
            "location": created.get("location"),
        })
        resolved = await self._poll_for_notebook_resolution(
            workspace_id=workspace_id,
            display_name=UNIVERSAL_PREVIEW_NOTEBOOK_NAME,
            notebook_id=created.get("notebookId"),
            diagnostics=diagnostics,
        )
        if not resolved or not resolved.get("id"):
            raise HTTPException(
                status_code=500,
                detail={
                    "message": f"Unable to resolve the runtime universal preview notebook after creation. workspaceId={workspace_id} operationId={created.get('operationId') or ''}",
                    "workspaceId": workspace_id,
                    "operationId": created.get("operationId"),
                    "requestId": created.get("requestId"),
                    "createdNotebookId": created.get("notebookId"),
                    "location": created.get("location"),
                    "lifecycle": diagnostics,
                },
            )
        self._set_cached_notebook(workspace_id, resolved)
        diagnostics.append({
            "state": "notebook_cached",
            "workspaceId": workspace_id,
            "notebookId": resolved.get("id"),
            "displayName": resolved.get("displayName"),
        })
        await self.update_notebook_definition(workspace_id, resolved["id"], source_code)
        return resolved

    async def run_notebook(self, workspace_id: str, notebook_id: str) -> str:
        payload = {
            "executionData": {
                "compute": "Spark"
            }
        }
        response = await self._request(
            "POST",
            f"{FABRIC_API_BASE}/workspaces/{workspace_id}/notebooks/{notebook_id}/jobs/execute/instances?beta=false",
            json=payload,
        )
        if response.status_code not in (200, 201, 202):
            raise HTTPException(status_code=response.status_code, detail=f"Run notebook failed: {response.text}")
        location = response.headers.get("Location", "")
        job_instance_id = location.rstrip("/").split("/")[-1] if location else None
        body = self._json_object(response)
        job_instance_id = job_instance_id or body.get("id")
        if not job_instance_id:
            raise HTTPException(status_code=500, detail="Notebook run did not return a job instance id.")
        return job_instance_id

    async def get_notebook_job_instance(self, workspace_id: str, notebook_id: str, job_instance_id: str) -> Dict[str, Any]:
        response = await self._request(
            "GET",
            f"{FABRIC_API_BASE}/workspaces/{workspace_id}/notebooks/{notebook_id}/jobs/execute/instances/{job_instance_id}?beta=true",
        )
        if not response.is_success:
            raise HTTPException(status_code=response.status_code, detail=f"Get notebook job instance failed: {response.text}")
        body = self._json_object(response)
        return body

    async def execute_preview(self, workspace_id: str, preview_request: Dict[str, Any], timeout_seconds: int = 600) -> Dict[str, Any]:
        source_code = build_universal_preview_notebook_source(preview_request)
        notebook_diagnostics: List[Dict[str, Any]] = []
        try:
            notebook = await self.ensure_preview_notebook(workspace_id, source_code, diagnostics=notebook_diagnostics)
        except HTTPException as exc:
            detail = exc.detail if isinstance(exc.detail, dict) else {"message": str(exc.detail)}
            detail.setdefault("notebook_diagnostics", notebook_diagnostics)
            return {
                "preview_error": detail.get("message") or "Notebook provisioning failed.",
                "notebook_diagnostics": detail.get("notebook_diagnostics") or notebook_diagnostics,
                "provisioning_detail": detail,
            }
        notebook_id = notebook.get("id")
        if not notebook_id:
            return {
                "preview_error": "Preview notebook id could not be resolved.",
                "notebook_diagnostics": notebook_diagnostics,
            }

        notebook_diagnostics.append({
            "state": "notebook_execution_started",
            "workspaceId": workspace_id,
            "notebookId": notebook_id,
            "displayName": notebook.get("displayName"),
        })
        job_instance_id = await self.run_notebook(workspace_id, notebook_id)
        deadline = asyncio.get_event_loop().time() + timeout_seconds
        while asyncio.get_event_loop().time() < deadline:
            instance = await self.get_notebook_job_instance(workspace_id, notebook_id, job_instance_id)
            if not isinstance(instance, dict):
                return {
                    "preview_error": "Notebook job instance API returned a non-object response.",
                    "job_instance": instance,
                    "notebook_diagnostics": notebook_diagnostics,
                }
            status = str(_first_key(instance, ["status", "state"]) or "")
            if status.lower() in {"completed", "succeeded", "success"}:
                notebook_diagnostics.append({
                    "state": "notebook_execution_completed",
                    "workspaceId": workspace_id,
                    "notebookId": notebook_id,
                    "jobInstanceId": job_instance_id,
                    "status": status,
                })
                exit_value = _first_key(instance, ["exitValue"])
                if not exit_value:
                    return {
                        "preview_error": "Notebook completed without an exitValue payload.",
                        "job_instance": instance,
                        "notebook_diagnostics": notebook_diagnostics,
                    }
                try:
                    payload = json.loads(exit_value)
                    if isinstance(payload, dict):
                        payload["notebook_diagnostics"] = notebook_diagnostics
                    return payload
                except Exception:
                    return {
                        "preview_error": "Notebook returned a non-JSON exit value.",
                        "exit_value": exit_value,
                        "job_instance": instance,
                        "notebook_diagnostics": notebook_diagnostics,
                    }
            if status.lower() in {"failed", "cancelled", "canceled"}:
                raise HTTPException(status_code=500, detail=f"Universal preview notebook failed with status {status}.")
            await asyncio.sleep(5)
        raise HTTPException(status_code=504, detail="Universal preview notebook timed out.")
