"""
DataHub Graph API Client
────────────────────────
Thin wrapper around DataHub's GMS REST API.
We call DataHub as an *external metadata source* — we do NOT rebuild
any DataHub functionality.

Endpoints used:
  GET  /entities/{urn}                        — fetch single entity
  POST /entities?action=search                — search across entity types
  POST /relationships                         — graph traversal
  GET  /aspects/{urn}?aspect=upstreamLineage  — lineage aspects

Reference: https://datahubproject.io/docs/api/restli/restli-overview
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import httpx

from config.settings import settings

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = 10.0


class DataHubClient:
    def __init__(
        self,
        gms_url: str | None = None,
        token: str | None = None,
    ):
        self.base_url = (gms_url or settings.datahub_gms_url).rstrip("/")
        self._headers: Dict[str, str] = {"Content-Type": "application/json"}
        if token or settings.datahub_token:
            self._headers["Authorization"] = f"Bearer {token or settings.datahub_token}"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def search_entities(
        self,
        query: str = "*",
        entity_types: Optional[List[str]] = None,
        count: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Full-text search across DataHub entities.

        entity_types examples: ["dataset", "dataJob", "dataFlow"]
        """
        entity_types = entity_types or ["dataset", "dataJob", "dataFlow"]
        results: List[Dict[str, Any]] = []

        for entity_type in entity_types:
            payload = {
                "input": query,
                "type": entity_type.upper(),
                "start": 0,
                "count": count,
            }
            try:
                resp = self._post(f"/entities?action=search", json=payload)
                hits = (
                    resp.get("value", {})
                    .get("entities", [])
                )
                for hit in hits:
                    entity = hit.get("entity", hit)
                    entity["_entityType"] = entity_type
                    results.append(entity)
            except Exception as exc:  # noqa: BLE001
                logger.warning("DataHub search failed for type=%s: %s", entity_type, exc)

        return results

    def get_entity(self, urn: str) -> Optional[Dict[str, Any]]:
        """Fetch a single entity by URN."""
        encoded = quote(urn, safe="")
        try:
            return self._get(f"/entities/{encoded}")
        except Exception as exc:  # noqa: BLE001
            logger.warning("DataHub get_entity failed for urn=%s: %s", urn, exc)
            return None

    def get_lineage(self, urn: str, direction: str = "UPSTREAM") -> List[Dict[str, Any]]:
        """
        Retrieve upstream or downstream lineage for a dataset/job URN.
        direction: "UPSTREAM" | "DOWNSTREAM"
        """
        encoded = quote(urn, safe="")
        try:
            resp = self._get(
                f"/relationships?urn={encoded}&direction={direction}&types=DownstreamOf"
            )
            return resp.get("entities", [])
        except Exception as exc:  # noqa: BLE001
            logger.warning("DataHub lineage failed for urn=%s: %s", urn, exc)
            return []

    def get_aspect(self, urn: str, aspect: str) -> Optional[Dict[str, Any]]:
        """Fetch a specific aspect from an entity."""
        encoded = quote(urn, safe="")
        try:
            return self._get(f"/aspects/{encoded}?aspect={aspect}")
        except Exception as exc:  # noqa: BLE001
            logger.warning("DataHub get_aspect failed urn=%s aspect=%s: %s", urn, aspect, exc)
            return None

    def get_node_config(self, urn: str) -> Dict[str, Any]:
        """
        Fetch all available aspects for a URN and merge into a single dict.

        Fetches: datasetProperties, dataFlowInfo, dataJobInfo, ownership, upstreamLineage.
        Omits any aspect that returns None.  Returns {} if all aspects are absent.
        """
        aspects = [
            "datasetProperties",
            "dataFlowInfo",
            "dataJobInfo",
            "ownership",
            "upstreamLineage",
        ]
        result: Dict[str, Any] = {}
        for aspect in aspects:
            try:
                value = self.get_aspect(urn, aspect)
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "DataHub API call failed for URN %s aspect %s: %s", urn, aspect, e
                )
                continue
            if value is not None:
                result[aspect] = value
        return result

    def get_data_jobs(self, flow_urn: str) -> List[Dict[str, Any]]:
        """List all dataJobs belonging to a dataFlow."""
        try:
            resp = self._get(
                f"/relationships?urn={quote(flow_urn, safe='')}&direction=OUTGOING&types=IsPartOf"
            )
            return resp.get("entities", [])
        except Exception as exc:  # noqa: BLE001
            logger.warning("DataHub get_data_jobs failed: %s", exc)
            return []

    def health_check(self) -> bool:
        """Returns True if DataHub GMS is reachable."""
        try:
            self._get("/config")
            return True
        except Exception:  # noqa: BLE001
            return False

    # ------------------------------------------------------------------
    # Private HTTP helpers
    # ------------------------------------------------------------------

    def _get(self, path: str) -> Dict[str, Any]:
        with httpx.Client(timeout=_DEFAULT_TIMEOUT) as client:
            resp = client.get(f"{self.base_url}{path}", headers=self._headers)
            resp.raise_for_status()
            return resp.json()

    def _post(self, path: str, json: Any) -> Dict[str, Any]:
        with httpx.Client(timeout=_DEFAULT_TIMEOUT) as client:
            resp = client.post(
                f"{self.base_url}{path}", headers=self._headers, json=json
            )
            resp.raise_for_status()
            return resp.json()


# Module-level singleton (override in tests)
datahub_client = DataHubClient()
