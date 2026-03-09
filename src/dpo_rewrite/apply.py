"""In-memory DPO application on NetworkX graphs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Hashable, Mapping, Optional

import networkx as nx
from networkx.algorithms import isomorphism as iso

from .cypher import _build_plan, _EdgeRecord, _RulePlan, _sorted_edges
from .rules import DpoRule


@dataclass(frozen=True)
class Match:
    node_mapping: dict[Any, Hashable]


@dataclass(frozen=True)
class RewriteResult:
    applied: bool
    match: Optional[Match]
    graph: nx.MultiDiGraph


def apply_rule(rule: DpoRule, host: nx.DiGraph | nx.MultiDiGraph) -> RewriteResult:
    """Apply a DPO rule once to a host graph."""
    plan = _build_plan(rule)
    working = _as_multidigraph(host)

    match_data = _find_first_match(rule, plan.left_edges, working)
    if match_data is None:
        return RewriteResult(applied=False, match=None, graph=working)

    node_mapping, matched_edges = match_data
    if _blocked_by_nac(rule, working, node_mapping):
        return RewriteResult(applied=False, match=None, graph=working)

    deleted_host_nodes = {
        node_mapping[node_id]
        for node_id in set(rule.left.nodes) - set(rule.interface.nodes)
    }
    if _has_dangling_edges(working, deleted_host_nodes, matched_edges):
        return RewriteResult(applied=False, match=None, graph=working)

    _apply_rewrite(rule, plan, working, node_mapping, matched_edges)
    return RewriteResult(
        applied=True, match=Match(node_mapping=dict(node_mapping)), graph=working
    )


def _as_multidigraph(host: nx.DiGraph | nx.MultiDiGraph) -> nx.MultiDiGraph:
    if isinstance(host, nx.MultiDiGraph):
        return host.copy()
    if isinstance(host, nx.DiGraph):
        return nx.MultiDiGraph(host)
    raise TypeError("host must be a networkx.DiGraph or networkx.MultiDiGraph")


def _find_first_match(
    rule: DpoRule, left_edges: list[_EdgeRecord], host: nx.MultiDiGraph
) -> tuple[dict[Any, Hashable], list[tuple[Hashable, Hashable, Hashable]]] | None:
    matcher = iso.MultiDiGraphMatcher(host, rule.left)
    left_nodes = sorted(rule.left.nodes, key=str)

    candidates: list[
        tuple[dict[Any, Hashable], list[tuple[Hashable, Hashable, Hashable]]]
    ] = []
    for raw_mapping in matcher.subgraph_isomorphisms_iter():
        node_mapping = {left_id: host_id for host_id, left_id in raw_mapping.items()}
        if not _nodes_compatible(rule.left, node_mapping, host):
            continue
        matched_edges = _materialize_edge_match(left_edges, node_mapping, host)
        if matched_edges is None:
            continue
        candidates.append((node_mapping, matched_edges))

    if not candidates:
        return None

    candidates.sort(
        key=lambda item: tuple(str(item[0][node_id]) for node_id in left_nodes)
    )
    return candidates[0]


def _nodes_compatible(
    pattern: nx.MultiDiGraph,
    node_mapping: Mapping[Any, Hashable],
    host: nx.MultiDiGraph,
) -> bool:
    for pattern_id, host_id in node_mapping.items():
        if not _node_attrs_match(host.nodes[host_id], pattern.nodes[pattern_id]):
            return False
    return True


def _materialize_edge_match(
    left_edges: list[_EdgeRecord],
    node_mapping: Mapping[Any, Hashable],
    host: nx.MultiDiGraph,
) -> list[tuple[Hashable, Hashable, Hashable]] | None:
    used: set[tuple[Hashable, Hashable, Hashable]] = set()
    matched: list[tuple[Hashable, Hashable, Hashable]] = []

    for edge in left_edges:
        source = node_mapping[edge.source]
        target = node_mapping[edge.target]
        edge_id = _pick_host_edge(source, target, edge.data, host, used)
        if edge_id is None:
            return None
        used.add(edge_id)
        matched.append(edge_id)
    return matched


def _pick_host_edge(
    source: Hashable,
    target: Hashable,
    pattern_data: Mapping[str, Any],
    host: nx.MultiDiGraph,
    used: set[tuple[Hashable, Hashable, Hashable]],
) -> tuple[Hashable, Hashable, Hashable] | None:
    if source not in host or target not in host:
        return None
    candidates = []
    for key, data in host.get_edge_data(source, target, default={}).items():
        edge_id = (source, target, key)
        if edge_id in used:
            continue
        if _edge_attrs_match(data, pattern_data):
            candidates.append(edge_id)
    if not candidates:
        return None
    return sorted(candidates, key=lambda item: str(item[2]))[0]


def _blocked_by_nac(
    rule: DpoRule,
    host: nx.MultiDiGraph,
    left_mapping: Mapping[Any, Hashable],
) -> bool:
    for nac in rule.nacs:
        if nac.number_of_nodes() == 0 and nac.number_of_edges() == 0:
            continue
        matcher = iso.MultiDiGraphMatcher(host, nac)
        nac_edges = _sorted_edges(nac)
        for raw_mapping in matcher.subgraph_isomorphisms_iter():
            nac_mapping = {nac_id: host_id for host_id, nac_id in raw_mapping.items()}
            if not _nodes_compatible(nac, nac_mapping, host):
                continue
            if _materialize_edge_match(nac_edges, nac_mapping, host) is None:
                continue
            if _respects_left_overlap(nac_mapping, left_mapping):
                return True
    return False


def _respects_left_overlap(
    nac_mapping: Mapping[Any, Hashable], left_mapping: Mapping[Any, Hashable]
) -> bool:
    for node_id, host_id in nac_mapping.items():
        if node_id in left_mapping and left_mapping[node_id] != host_id:
            return False
    return True


def _has_dangling_edges(
    host: nx.MultiDiGraph,
    deleted_nodes: set[Hashable],
    matched_edges: list[tuple[Hashable, Hashable, Hashable]],
) -> bool:
    matched = set(matched_edges)
    for node_id in deleted_nodes:
        for source, _, key in host.in_edges(node_id, keys=True):
            if (source, node_id, key) not in matched:
                return True
        for _, target, key in host.out_edges(node_id, keys=True):
            if (node_id, target, key) not in matched:
                return True
    return False


def _apply_rewrite(
    rule: DpoRule,
    plan: _RulePlan,
    host: nx.MultiDiGraph,
    node_mapping: dict[Any, Hashable],
    matched_edges: list[tuple[Hashable, Hashable, Hashable]],
) -> None:
    for edge_index in plan.deleted_edge_indices:
        source, target, key = matched_edges[edge_index]
        if host.has_edge(source, target, key):
            host.remove_edge(source, target, key)

    for node_id in plan.deleted_nodes:
        host_node = node_mapping[node_id]
        if host_node in host:
            host.remove_node(host_node)

    for rule_node_id in plan.created_nodes:
        new_id = _fresh_node_id(rule_node_id, host)
        host.add_node(new_id, **_copy_node_data(rule.right.nodes[rule_node_id]))
        node_mapping[rule_node_id] = new_id

    for edge in plan.created_edges:
        source = node_mapping[edge.source]
        target = node_mapping[edge.target]
        host.add_edge(source, target, **_copy_edge_data(edge.data))


def _fresh_node_id(base: Any, host: nx.MultiDiGraph) -> Hashable:
    if base not in host:
        return base
    prefix = str(base) if str(base) else "n"
    index = 1
    while True:
        candidate = f"{prefix}_{index}"
        if candidate not in host:
            return candidate
        index += 1


def _node_attrs_match(
    host_data: Mapping[str, Any], pattern_data: Mapping[str, Any]
) -> bool:
    pattern_labels = _as_labels(pattern_data.get("label"))
    if pattern_labels:
        host_labels = _as_labels(host_data.get("label"))
        if not pattern_labels.issubset(host_labels):
            return False

    return _props_subset_match(host_data.get("props"), pattern_data.get("props"))


def _edge_attrs_match(
    host_data: Mapping[str, Any], pattern_data: Mapping[str, Any]
) -> bool:
    pattern_type = pattern_data.get("type")
    if pattern_type is not None and host_data.get("type") != pattern_type:
        return False
    return _props_subset_match(host_data.get("props"), pattern_data.get("props"))


def _props_subset_match(host_props: Any, pattern_props: Any) -> bool:
    if pattern_props is None:
        return True
    if not isinstance(pattern_props, Mapping):
        return False
    if host_props is None:
        return False
    if not isinstance(host_props, Mapping):
        return False
    for key, value in pattern_props.items():
        if key not in host_props or host_props[key] != value:
            return False
    return True


def _as_labels(value: Any) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, str):
        return {value}
    if isinstance(value, (list, tuple, set)):
        return {label for label in value if isinstance(label, str)}
    return set()


def _copy_node_data(data: Mapping[str, Any]) -> dict[str, Any]:
    copied = dict(data)
    if isinstance(copied.get("props"), Mapping):
        copied["props"] = dict(copied["props"])
    return copied


def _copy_edge_data(data: Mapping[str, Any]) -> dict[str, Any]:
    copied = dict(data)
    if isinstance(copied.get("props"), Mapping):
        copied["props"] = dict(copied["props"])
    return copied
