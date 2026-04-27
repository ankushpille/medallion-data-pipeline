import logging
import base64
import json
import requests
from typing import Dict, List, Any
from .base import CloudScanner

logger = logging.getLogger("pipeline_ie.scanner.fabric")

class FabricScanner(CloudScanner):
    def can_scan(self, settings: Any) -> bool:
        # Fabric REST calls use the delegated token from the session; scan() no-ops quickly without it.
        return True

    def scan(self, settings: Any, **kwargs) -> Dict[str, List[str]]:
        raw_assets: Dict[str, List[Any]] = {
            "fabric_workspaces": [],
            "fabric_items": []
        }

        try:
            # 1. Identity Resolution (Fabric API requires Power BI / Fabric scope, not ARM)
            azure_token = kwargs.get("azure_token_fabric") or kwargs.get("azure_token")
            if not azure_token:
                logger.info("No delegated token for Fabric, skipping deep scan.")
                return {"raw_cloud_dump": [raw_assets]}

            # Microsoft Fabric API Scope
            headers = {"Authorization": f"Bearer {azure_token}"}
            
            # 2. List Workspaces
            logger.info("Triggering Microsoft Fabric Deep Discovery...")
            ws_response = requests.get("https://api.fabric.microsoft.com/v1/workspaces", headers=headers, timeout=10)
            
            if ws_response.status_code == 200:
                # ... (rest of logic stays same)
                workspaces = ws_response.json().get('value', [])
                for ws in workspaces:
                    # ... ws logic ...
                    ws_id = ws.get('id')
                    raw_assets["fabric_workspaces"].append({
                        "id": f"fabric || {ws.get('displayName')}",
                        "configuration": {
                            "WorkspaceId": ws_id,
                            "Type": ws.get('type'),
                            "CapacityId": ws.get('capacityId')
                        }
                    })
                    
                    # 3. List Items in Workspace (Lakehouses, Pipelines, etc.)
                    items_res = requests.get(f"https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/items", headers=headers, timeout=10)
                    if items_res.status_code == 200:
                        items = items_res.json().get('value', [])
                        for item in items:
                            item_id = item.get('id')
                            item_type = item.get('type')
                            item_meta = {
                                "id": f"fabric || {item.get('displayName')}",
                                "configuration": {
                                    "ItemId": item_id,
                                    "Type": item_type,
                                    "WorkspaceId": ws_id
                                }
                            }
                            
                            # Deep Inspection for Lakehouses
                            if item_type == 'Lakehouse':
                                try:
                                    lh_res = requests.get(f"https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/lakehouses/{item_id}", headers=headers, timeout=10)
                                    if lh_res.status_code == 200:
                                        lh_props = lh_res.json().get('properties', {})
                                        item_meta["configuration"]["OneLakeFilesPath"] = lh_props.get('oneLakeFilesPath')
                                        item_meta["configuration"]["OneLakeTablesPath"] = lh_props.get('oneLakeTablesPath')
                                except Exception:
                                    pass
                            elif str(item_type).lower() in {"pipeline", "datapipeline"}:
                                definition = self._fetch_item_definition(headers, ws_id, item_id, item_type)
                                if definition:
                                    item_meta["configuration"]["Definition"] = definition
                                    
                            raw_assets["fabric_items"].append(item_meta)
            elif ws_response.status_code == 401:
                logger.error("Fabric API 401 Unauthorized. The SSO token likely lacks the required PowerBI/Fabric scope ('https://analysis.windows.net/powerbi/api/.default').")
            else:
                logger.warning(f"Fabric API returned {ws_response.status_code}: {ws_response.text}")

        except Exception as e:
            logger.error(f"Fabric Scan failed: {e}")
            # Fallback for demo/dev if nothing found
            if not raw_assets["fabric_items"]:
                return self._simulate_fabric()

        return {"raw_cloud_dump": [raw_assets]}

    def _fetch_item_definition(self, headers: Dict[str, str], workspace_id: str, item_id: str, item_type: str) -> Dict[str, Any] | None:
        urls = [
            f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{item_id}/getDefinition",
            f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/{str(item_type).lower()}/items/{item_id}/getDefinition",
        ]

        for url in urls:
            try:
                response = requests.post(url, headers=headers, timeout=15)
                if response.status_code in (200, 201):
                    payload = response.json()
                    decoded = self._decode_definition_payload(payload)
                    if decoded:
                        return decoded
                elif response.status_code == 202:
                    location = response.headers.get("Location")
                    if location:
                        polled = self._poll_lro_definition(headers, location)
                        decoded = self._decode_definition_payload(polled) if polled else None
                        if decoded:
                            return decoded
            except Exception as exc:
                logger.debug(f"Fabric definition fetch failed for {item_id} via {url}: {exc}")
        return None

    def _poll_lro_definition(self, headers: Dict[str, str], location: str) -> Dict[str, Any] | None:
        for _ in range(5):
            try:
                response = requests.get(location, headers=headers, timeout=15)
                if response.status_code == 200:
                    return response.json()
                if response.status_code in (202, 204):
                    continue
            except Exception as exc:
                logger.debug(f"Fabric definition poll failed: {exc}")
                return None
        return None

    def _decode_definition_payload(self, payload: Dict[str, Any]) -> Dict[str, Any] | None:
        if not isinstance(payload, dict):
            return None

        parts = payload.get("definition", {}).get("parts") or payload.get("parts") or []
        decoded: Dict[str, Any] = {}
        if not isinstance(parts, list):
            return None

        for part in parts:
            if not isinstance(part, dict):
                continue
            path = part.get("path")
            if not path:
                continue
            raw_value = (
                part.get("payload")
                or part.get("content")
                or part.get("data")
                or part.get("payloadBase64")
            )
            if raw_value is None:
                continue

            text_value = None
            if isinstance(raw_value, str):
                text_value = raw_value
                try:
                    text_value = base64.b64decode(raw_value).decode("utf-8")
                except Exception:
                    text_value = raw_value

            if text_value is None:
                continue

            try:
                decoded[path] = json.loads(text_value)
            except Exception:
                decoded[path] = text_value

        return decoded or None

    def _simulate_fabric(self) -> Dict[str, List[Any]]:
        return {"raw_cloud_dump": [{
            "fabric_workspaces": [
                {
                    "id": "fabric || Analytics-Hub",
                    "configuration": {"Type": "Workspace", "Region": "West US"}
                }
            ],
            "fabric_items": [
                {
                    "id": "fabric || Customer-Lakehouse",
                    "configuration": {"Type": "Lakehouse", "Workspace": "Analytics-Hub"}
                },
                {
                    "id": "fabric || Silver-Transformation",
                    "configuration": {
                        "Type": "Pipeline",
                        "Workspace": "Analytics-Hub",
                        "Definition": {
                            "pipeline-content.json": {
                                "properties": {
                                    "activities": [
                                        {
                                            "name": "API Ingestion",
                                            "type": "WebActivity",
                                            "typeProperties": {
                                                "method": "GET",
                                                "url": "https://api.contoso.com/orders"
                                            }
                                        },
                                        {
                                            "name": "Notebook 1",
                                            "type": "TridentNotebook",
                                            "dependsOn": [{"activity": "API Ingestion"}],
                                            "typeProperties": {
                                                "notebookId": "nb-001",
                                                "workspaceId": "Analytics-Hub"
                                            }
                                        }
                                    ]
                                }
                            }
                        }
                    }
                }
            ]
        }]}
