"""Rule structures for DPO graph rewriting."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, MutableMapping, Optional, Sequence

import networkx as nx

# Shared type alias used across rule/matching/serialization modules.
RuleGraph = nx.MultiDiGraph


def add_node(
    graph: RuleGraph,
    node_id: str,
    *,
    label: Optional[str | Sequence[str]] = None,
    props: Optional[Mapping[str, Any]] = None,
) -> None:
    """Add a node with standard label/props attributes."""
    attributes: MutableMapping[str, Any] = {}
    # Keep a consistent attribute schema for serialization/matching.
    if label is not None:
        attributes["label"] = label
    if props:
        attributes["props"] = dict(props)
    graph.add_node(node_id, **attributes)


def add_edge(
    graph: RuleGraph,
    source: str,
    target: str,
    *,
    key: Optional[str] = None,
    rel_type: Optional[str] = None,
    props: Optional[Mapping[str, Any]] = None,
) -> None:
    """Add an edge with standard type/props attributes."""
    attributes: MutableMapping[str, Any] = {}
    # Relationship type and props are used by the Cypher serializer.
    if rel_type is not None:
        attributes["type"] = rel_type
    if props:
        attributes["props"] = dict(props)
    graph.add_edge(source, target, key=key, **attributes)


@dataclass(frozen=True)
class DpoRule:
    """DPO rule represented as (L, K, R) graphs.

    Preserved elements should keep the same node IDs across L/K/R.
    """

    left: RuleGraph
    interface: RuleGraph
    right: RuleGraph
    nacs: tuple[RuleGraph, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        object.__setattr__(self, "nacs", tuple(self.nacs))
        self._validate_graph("left", self.left)
        self._validate_graph("interface", self.interface)
        self._validate_graph("right", self.right)
        for index, nac in enumerate(self.nacs):
            self._validate_graph(f"nac[{index}]", nac)

    @staticmethod
    def _validate_graph(name: str, graph: RuleGraph) -> None:
        if not isinstance(graph, nx.MultiDiGraph):
            raise TypeError(f"{name} must be a networkx.MultiDiGraph")

    @classmethod
    def from_graphs(
        cls, left: RuleGraph, interface: RuleGraph, right: RuleGraph
    ) -> "DpoRule":
        return cls(left=left, interface=interface, right=right)

    @classmethod
    def from_json(
        cls, payload: Mapping[str, Any], *, validate: bool = False
    ) -> "DpoRule":
        """Load a DPO rule from a JSON-compatible mapping.

        Expected shape:
            {
              "left": {"nodes": [...], "edges": [...]},
              "interface": {"nodes": [...], "edges": [...]},
              "right": {"nodes": [...], "edges": [...]}
            }
        """
        if validate:
            from dpo_rewrite.schema import validate_dpo_rule_payload

            validate_dpo_rule_payload(payload)
        left = cls._graph_from_payload("left", payload.get("left"))
        interface = cls._graph_from_payload("interface", payload.get("interface"))
        right = cls._graph_from_payload("right", payload.get("right"))
        nacs = cls._graphs_from_payload("nac", payload.get("nac", []))
        return cls(left=left, interface=interface, right=right, nacs=tuple(nacs))

    def summary(self) -> dict[str, int]:
        """Return basic counts for inspection/logging."""
        return {
            "left_nodes": self.left.number_of_nodes(),
            "left_edges": self.left.number_of_edges(),
            "interface_nodes": self.interface.number_of_nodes(),
            "interface_edges": self.interface.number_of_edges(),
            "right_nodes": self.right.number_of_nodes(),
            "right_edges": self.right.number_of_edges(),
        }

    def to_json(self) -> dict[str, Any]:
        """Serialize the rule to a JSON-compatible mapping."""
        return {
            "left": _graph_to_payload(self.left),
            "interface": _graph_to_payload(self.interface),
            "right": _graph_to_payload(self.right),
            "nac": [_graph_to_payload(nac) for nac in self.nacs],
        }

    @staticmethod
    def _graphs_from_payload(name: str, payload: Any) -> list[RuleGraph]:
        if payload is None:
            return []
        if not isinstance(payload, Sequence) or isinstance(
            payload, (str, bytes, bytearray, Mapping)
        ):
            raise TypeError(f"{name} must be a list of graphs")
        graphs: list[RuleGraph] = []
        for index, entry in enumerate(payload):
            graphs.append(DpoRule._graph_from_payload(f"{name}[{index}]", entry))
        return graphs

    @staticmethod
    def _graph_from_payload(name: str, payload: Any) -> RuleGraph:
        if not isinstance(payload, Mapping):
            raise TypeError(f"{name} must be a mapping with 'nodes' and 'edges'")
        nodes = payload.get("nodes", [])
        edges = payload.get("edges", [])
        if not isinstance(nodes, Iterable):
            raise TypeError(f"{name}.nodes must be a list of node mappings")
        if not isinstance(edges, Iterable):
            raise TypeError(f"{name}.edges must be a list of edge mappings")
        graph = nx.MultiDiGraph()
        for node in nodes:
            if not isinstance(node, Mapping):
                raise TypeError(f"{name}.nodes entries must be mappings")
            node_id = node.get("id")
            if not isinstance(node_id, str) or not node_id:
                raise ValueError(f"{name}.nodes entries must include non-empty 'id'")
            add_node(
                graph,
                node_id,
                label=node.get("label"),
                props=node.get("props"),
            )
        for edge in edges:
            if not isinstance(edge, Mapping):
                raise TypeError(f"{name}.edges entries must be mappings")
            source = edge.get("source")
            target = edge.get("target")
            if not isinstance(source, str) or not isinstance(target, str):
                raise ValueError(
                    f"{name}.edges entries must include string 'source'/'target'"
                )
            add_edge(
                graph,
                source,
                target,
                key=edge.get("key"),
                rel_type=edge.get("type"),
                props=edge.get("props"),
            )
        return graph


def _graph_to_payload(graph: RuleGraph) -> dict[str, Any]:
    nodes_payload: list[dict[str, Any]] = []
    for node_id in _sorted_node_ids(graph):
        data = graph.nodes[node_id]
        entry: dict[str, Any] = {"id": node_id}
        if "label" in data:
            entry["label"] = data["label"]
        if "props" in data and data["props"]:
            entry["props"] = data["props"]
        nodes_payload.append(entry)

    edges_payload: list[dict[str, Any]] = []
    for source, target, key, data in _sorted_edges(graph):
        entry: dict[str, Any] = {"source": source, "target": target}
        if key is not None:
            entry["key"] = key
        if "type" in data and data["type"] is not None:
            entry["type"] = data["type"]
        if "props" in data and data["props"]:
            entry["props"] = data["props"]
        edges_payload.append(entry)

    return {"nodes": nodes_payload, "edges": edges_payload}


def _sorted_node_ids(graph: RuleGraph) -> list[Any]:
    return sorted(graph.nodes, key=lambda item: str(item))


def _sorted_edges(graph: RuleGraph) -> list[tuple[Any, Any, Any, dict[str, Any]]]:
    edges = [(u, v, k, data) for u, v, k, data in graph.edges(keys=True, data=True)]
    return sorted(edges, key=_edge_sort_key)


def _edge_sort_key(
    edge: tuple[Any, Any, Any, dict[str, Any]],
) -> tuple[str, str, str, str, str]:
    source, target, key, data = edge
    rel_type = data.get("type") or ""
    props = data.get("props") or {}
    props_key = repr(_freeze_value(props))
    return (str(source), str(target), str(key), str(rel_type), props_key)


def _freeze_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return tuple(sorted((key, _freeze_value(val)) for key, val in value.items()))
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_value(val) for val in value)
    if isinstance(value, set):
        return tuple(sorted(_freeze_value(val) for val in value))
    try:
        hash(value)
    except TypeError:
        return repr(value)
    return value
