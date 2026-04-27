"""
Pipeline Graph module for the Pipeline Intelligence Engine.

Provides an in-memory directed graph of pipeline nodes (datasets, jobs, flows)
and their lineage edges, built from DataHub lineage data.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from engine.detectors.base import CloudEnvironment
from engine.urn_parser import parse_urn


@dataclass
class PipelineNode:
    """Represents a single node in the pipeline graph."""

    urn: str
    node_type: str          # "dataset" | "dataJob" | "dataFlow"
    platform: str
    environment: Optional[CloudEnvironment]
    aspects: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineEdge:
    """Represents a directed edge between two pipeline nodes."""

    source_urn: str
    target_urn: str
    edge_type: str          # "DownstreamOf" | "IsPartOf"


def _infer_node_type(urn: str) -> str:
    """Infer the node_type string from a DataHub URN prefix."""
    if urn.startswith("urn:li:dataJob:"):
        return "dataJob"
    if urn.startswith("urn:li:dataFlow:"):
        return "dataFlow"
    if urn.startswith("urn:li:dataset:"):
        return "dataset"
    # Fallback: try to extract from the URN structure
    return "dataset"


def _infer_platform(urn: str) -> str:
    """Infer the platform from a DataHub URN using parse_urn."""
    parsed = parse_urn(urn)
    if parsed is not None:
        return parsed.platform
    return "unknown"


class PipelineGraph:
    """
    In-memory directed graph of pipeline nodes and lineage edges.

    Nodes are deduplicated by URN. Edges are stored as a list and
    deduplicated by (source_urn, target_urn, edge_type).
    """

    def __init__(self) -> None:
        self._nodes: Dict[str, PipelineNode] = {}   # keyed by URN
        self._edges: List[PipelineEdge] = []
        self._edge_keys: set = set()                # (source, target, type) for dedup

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    def add_node(self, node: PipelineNode) -> None:
        """
        Add a node to the graph.

        If a node with the same URN already exists, the existing node is
        kept unchanged (first-write-wins deduplication).
        """
        if node.urn not in self._nodes:
            self._nodes[node.urn] = node

    def add_edge(self, edge: PipelineEdge) -> None:
        """
        Add a directed edge to the graph.

        Edges are deduplicated by (source_urn, target_urn, edge_type).
        """
        key = (edge.source_urn, edge.target_urn, edge.edge_type)
        if key not in self._edge_keys:
            self._edges.append(edge)
            self._edge_keys.add(key)

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def get_node(self, urn: str) -> Optional[PipelineNode]:
        """Return the node with the given URN, or None if not present."""
        return self._nodes.get(urn)

    def get_upstream(self, urn: str) -> List[PipelineNode]:
        """
        Return all nodes that have a directed edge pointing TO this URN.

        In DataHub lineage terms, these are the nodes that this node
        depends on (its upstream producers).
        """
        upstream_urns = [e.source_urn for e in self._edges if e.target_urn == urn]
        return [self._nodes[u] for u in upstream_urns if u in self._nodes]

    def get_downstream(self, urn: str) -> List[PipelineNode]:
        """
        Return all nodes that this URN has a directed edge pointing TO.

        In DataHub lineage terms, these are the nodes that consume this
        node's output (its downstream consumers).
        """
        downstream_urns = [e.target_urn for e in self._edges if e.source_urn == urn]
        return [self._nodes[u] for u in downstream_urns if u in self._nodes]

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    def nodes(self) -> List[PipelineNode]:
        """Return all nodes in the graph."""
        return list(self._nodes.values())

    def edges(self) -> List[PipelineEdge]:
        """Return all edges in the graph."""
        return list(self._edges)

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def to_dict(self) -> Dict:
        """
        Serialise the graph to a plain dict.

        Returns:
            {
                "nodes": [{"urn": ..., "node_type": ..., "platform": ...}, ...],
                "edges": [{"source": ..., "target": ..., "edge_type": ...}, ...],
            }
        """
        return {
            "nodes": [
                {
                    "urn": node.urn,
                    "node_type": node.node_type,
                    "platform": node.platform,
                }
                for node in self._nodes.values()
            ],
            "edges": [
                {
                    "source": edge.source_urn,
                    "target": edge.target_urn,
                    "edge_type": edge.edge_type,
                }
                for edge in self._edges
            ],
        }

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_lineage(cls, seed_urn: str, lineage: List[Dict]) -> "PipelineGraph":
        """
        Build a PipelineGraph from a DataHub lineage response.

        Args:
            seed_urn:  The URN of the entity whose lineage was queried.
            lineage:   List of lineage items from DataHub, each with the shape:
                       {"entity": {"urn": "..."}, "type": "DownstreamOf"}

        Returns:
            A PipelineGraph containing the seed node plus all lineage nodes
            and edges. If lineage is empty, returns a graph with only the
            seed node and no edges.
        """
        graph = cls()

        # Always add the seed node
        seed_node = PipelineNode(
            urn=seed_urn,
            node_type=_infer_node_type(seed_urn),
            platform=_infer_platform(seed_urn),
            environment=None,
            aspects={},
        )
        graph.add_node(seed_node)

        for item in lineage:
            entity = item.get("entity") or {}
            related_urn = entity.get("urn", "")
            edge_type = item.get("type", "DownstreamOf")

            if not related_urn:
                continue

            # Add the related node
            related_node = PipelineNode(
                urn=related_urn,
                node_type=_infer_node_type(related_urn),
                platform=_infer_platform(related_urn),
                environment=None,
                aspects={},
            )
            graph.add_node(related_node)

            # The lineage item represents: related_urn --[edge_type]--> seed_urn
            # (i.e. related_urn is upstream of seed_urn)
            graph.add_edge(PipelineEdge(
                source_urn=related_urn,
                target_urn=seed_urn,
                edge_type=edge_type,
            ))

        return graph
