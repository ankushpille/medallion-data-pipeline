"""
Base classes for all detectors in the Pipeline Intelligence Engine.

Every detector is a self-contained plugin that receives a unified
AnalysisPayload and returns a DetectionResult.
"""
from __future__ import annotations

import abc
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class CloudEnvironment:
    """
    Represents a cloud environment grouping for pipeline nodes.

    provider is one of: "aws" | "azure" | "gcp" | "databricks" | "snowflake"
    """

    provider: str
    account_id: Optional[str] = None
    region: Optional[str] = None
    project_id: Optional[str] = None
    node_urns: List[str] = field(default_factory=list)


@dataclass
class DetectionResult:
    """Standardised output from every detector."""

    results: List[str] = field(default_factory=list)
    confidence: float = 1.0          # 0.0 – 1.0
    evidence: List[str] = field(default_factory=list)   # human-readable trail
    raw: Optional[Dict[str, Any]] = field(default=None)  # optional extra context

    def merge(self, other: "DetectionResult") -> "DetectionResult":
        return DetectionResult(
            results=list(dict.fromkeys(self.results + other.results)),
            confidence=min(self.confidence, other.confidence),
            evidence=self.evidence + other.evidence,
        )


@dataclass
class AnalysisPayload:
    """
    Unified input object passed to every detector.

    Fields map to the three optional input surfaces on POST /analyze:
      - metadata   : free-form dict (DataHub entity attrs, platform tags, etc.)
      - config     : pipeline config (connections, job defs, resource blocks)
      - raw_json   : any additional JSON (Terraform, YAML-parsed configs, etc.)
    """

    metadata: Dict[str, Any] = field(default_factory=dict)
    config: Dict[str, Any] = field(default_factory=dict)
    raw_json: Dict[str, Any] = field(default_factory=dict)

    # Populated after DataHub enrichment
    datahub_entities: List[Dict[str, Any]] = field(default_factory=list)

    # Optional cloud environment context (populated by CloudEnvironmentGrouper)
    cloud_environment: Optional[CloudEnvironment] = None

    def all_text(self) -> str:
        """Single lowercase string of all payload content — used for pattern matching."""
        parts = [
            str(self.metadata),
            str(self.config),
            str(self.raw_json),
            str(self.datahub_entities),
        ]
        return " ".join(parts).lower()


class BaseDetector(abc.ABC):
    """
    Every detector must:
      1. Set a unique class-level `name`
      2. Implement `detect(payload) -> DetectionResult`
    """

    name: str = "base"
    priority: int = 100  # Lower values run first; detectors with equal priority run in registration order

    @abc.abstractmethod
    def detect(self, payload: AnalysisPayload) -> DetectionResult:
        ...

    def __repr__(self) -> str:  # pragma: no cover
        return f"<Detector: {self.name}>"
