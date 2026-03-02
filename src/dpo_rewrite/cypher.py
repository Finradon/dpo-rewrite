"""Cypher serialization for DPO rules."""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass
from typing import Any, Mapping, Optional, Sequence

from .rules import DpoRule, RuleGraph

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class RuleSerializationError(ValueError):
    """Raised when a rule cannot be serialized under current constraints."""


@dataclass(frozen=True)
class CypherQuery:
    query: str
    params: dict[str, Any]


@dataclass(frozen=True)
class _EdgeRecord:
    source: Any
    target: Any
    key: Any
    data: Mapping[str, Any]


@dataclass(frozen=True)
class _RulePlan:
    left_edges: list[_EdgeRecord]
    created_nodes: list[Any]
    created_edges: list[_EdgeRecord]
    deleted_nodes: list[Any]
    deleted_edge_indices: list[int]


class _ParamBuilder:
    def __init__(self) -> None:
        self.params: dict[str, Any] = {}

    def add(self, scope: str, key: str, value: Any) -> str:
        # Generate stable, collision-free parameter names for Cypher.
        scope_clean = _sanitize_identifier(scope)
        key_clean = _sanitize_identifier(key)
        base = "_".join(part for part in (scope_clean, key_clean) if part)
        if not base:
            base = "p"
        name = base
        counter = 1
        while name in self.params:
            name = f"{base}_{counter}"
            counter += 1
        self.params[name] = value
        return f"${name}"


def rule_to_cypher(rule: DpoRule) -> CypherQuery:
    """Serialize a DPO rule into Cypher."""
    plan = _build_plan(rule)
    left_nodes = _sorted_nodes(rule.left)
    left_edges = plan.left_edges

    # Node variables must be consistent across MATCH and CREATE clauses.
    used_vars: set[str] = set()
    node_vars: dict[Any, str] = {}
    for node_id in left_nodes:
        node_vars[node_id] = _make_node_var(node_id, used_vars)
    for node_id in plan.created_nodes:
        node_vars[node_id] = _make_node_var(node_id, used_vars)

    params = _ParamBuilder()
    match_patterns = _build_match_patterns(rule.left, node_vars, params, left_edges)
    create_node_patterns = _build_create_nodes(
        rule.right, plan.created_nodes, node_vars, params
    )
    create_edge_patterns = _build_create_edges(plan.created_edges, node_vars, params)
    where_clauses = _build_where_clauses(
        left_nodes, len(left_edges), plan.deleted_nodes, node_vars
    )
    where_clauses.extend(
        _build_nac_clauses(rule, node_vars, params, used_vars=used_vars)
    )
    delete_targets = _build_delete_targets(plan, node_vars)

    clauses: list[str] = []
    if match_patterns:
        clauses.append("MATCH " + ", ".join(match_patterns))
    if match_patterns and where_clauses:
        clauses.append("WHERE " + " AND ".join(where_clauses))
    if delete_targets:
        clauses.append("DELETE " + ", ".join(delete_targets))
    if create_node_patterns:
        clauses.append("CREATE " + ", ".join(create_node_patterns))
    if create_edge_patterns:
        clauses.append("CREATE " + ", ".join(create_edge_patterns))

    return CypherQuery(query="\n".join(clauses), params=params.params)


def _build_plan(rule: DpoRule) -> _RulePlan:
    _validate_graphs(rule)

    left_nodes = set(rule.left.nodes)
    interface_nodes = set(rule.interface.nodes)
    right_nodes = set(rule.right.nodes)

    if not interface_nodes.issubset(left_nodes):
        missing = interface_nodes - left_nodes
        raise RuleSerializationError(
            "Interface must be a subgraph of left. "
            f"Missing from left: {sorted(missing, key=str)}"
        )

    if not interface_nodes.issubset(right_nodes):
        missing = interface_nodes - right_nodes
        raise RuleSerializationError(
            "Interface must be a subgraph of right. "
            f"Missing from right: {sorted(missing, key=str)}"
        )

    common_nodes = left_nodes & right_nodes
    if interface_nodes != common_nodes:
        missing = common_nodes - interface_nodes
        raise RuleSerializationError(
            "Nodes present in both left and right must be in interface. "
            f"Missing from interface: {sorted(missing, key=str)}"
        )

    _validate_preserved_nodes(rule, interface_nodes)

    left_edges_list = _sorted_edges(rule.left)
    left_edges = _edge_multiset(rule.left, context="left")
    interface_edges = _edge_multiset(rule.interface, context="interface")
    right_edges = _edge_multiset(rule.right, context="right")

    if not _multiset_is_subset(interface_edges, left_edges):
        raise RuleSerializationError("Interface edges must be a subset of left edges.")

    if not _multiset_is_subset(interface_edges, right_edges):
        raise RuleSerializationError("Interface edges must be a subset of right edges.")

    shared_edges = _multiset_intersection(left_edges, right_edges)
    if interface_edges != shared_edges:
        raise RuleSerializationError(
            "Edges present in both left and right must be in interface."
        )

    created_nodes = sorted(right_nodes - interface_nodes, key=str)
    deleted_nodes = sorted(left_nodes - interface_nodes, key=str)
    created_edges = _edge_difference(rule.right, interface_edges)
    deleted_edge_indices = _edge_deletion_indices(left_edges_list, interface_edges)

    return _RulePlan(
        left_edges=left_edges_list,
        created_nodes=created_nodes,
        created_edges=created_edges,
        deleted_nodes=deleted_nodes,
        deleted_edge_indices=deleted_edge_indices,
    )


def _validate_graphs(rule: DpoRule) -> None:
    for name, graph in (
        ("left", rule.left),
        ("interface", rule.interface),
        ("right", rule.right),
    ):
        if not isinstance(graph, RuleGraph):
            raise RuleSerializationError(f"{name} must be a networkx.MultiDiGraph")
    _validate_nacs(rule)


def _validate_nacs(rule: DpoRule) -> None:
    left_nodes = set(rule.left.nodes)
    left_attrs: dict[Any, tuple[tuple[str, ...], dict[str, Any]]] = {}
    for node_id in left_nodes:
        left_attrs[node_id] = _normalized_node_attrs(rule.left, node_id, "left")

    for index, nac in enumerate(rule.nacs):
        if not isinstance(nac, RuleGraph):
            raise RuleSerializationError(
                f"nac[{index}] must be a networkx.MultiDiGraph"
            )
        for node_id in nac.nodes:
            if node_id in left_nodes:
                nac_attrs = _normalized_node_attrs(
                    nac, node_id, f"nac[{index}] node {node_id}"
                )
                if nac_attrs != left_attrs[node_id]:
                    raise RuleSerializationError(
                        "NAC nodes shared with left must keep identical labels/props. "
                        f"Mismatch on node '{node_id}' in nac[{index}]."
                    )


def _build_match_patterns(
    graph: RuleGraph,
    node_vars: Mapping[Any, str],
    params: _ParamBuilder,
    edges: Optional[Sequence[_EdgeRecord]] = None,
) -> list[str]:
    patterns: list[str] = []

    # Match nodes first, then relationships to anchor the pattern.
    for node_id in _sorted_nodes(graph):
        data = graph.nodes[node_id]
        patterns.append(_node_pattern(node_id, data, node_vars, params, "left"))

    if edges is None:
        edges = _sorted_edges(graph)
    for index, edge in enumerate(edges):
        patterns.append(
            _match_edge_pattern(edge, node_vars, params, index, context="left")
        )

    return patterns


def _build_create_nodes(
    graph: RuleGraph,
    created_nodes: Sequence[Any],
    node_vars: Mapping[Any, str],
    params: _ParamBuilder,
) -> list[str]:
    patterns: list[str] = []
    for node_id in created_nodes:
        data = graph.nodes[node_id]
        patterns.append(_node_pattern(node_id, data, node_vars, params, "right"))
    return patterns


def _build_create_edges(
    created_edges: Sequence[_EdgeRecord],
    node_vars: Mapping[Any, str],
    params: _ParamBuilder,
) -> list[str]:
    patterns: list[str] = []
    for index, edge in enumerate(created_edges):
        patterns.append(
            _create_edge_pattern(edge, node_vars, params, index, context="right")
        )
    return patterns


def _build_where_clauses(
    left_node_ids: Sequence[Any],
    left_edge_count: int,
    deleted_nodes: Sequence[Any],
    node_vars: Mapping[Any, str],
) -> list[str]:
    clauses: list[str] = []
    clauses.extend(_distinct_node_conditions(left_node_ids, node_vars))
    clauses.extend(_distinct_edge_conditions(left_edge_count))
    clauses.extend(_dangling_conditions(deleted_nodes, left_edge_count, node_vars))
    return clauses


def _build_nac_clauses(
    rule: DpoRule,
    node_vars: Mapping[Any, str],
    params: _ParamBuilder,
    *,
    used_vars: set[str],
) -> list[str]:
    clauses: list[str] = []
    if not rule.nacs:
        return clauses

    for nac_index, nac in enumerate(rule.nacs):
        nac_nodes = _sorted_nodes(nac)
        if not nac_nodes and nac.number_of_edges() == 0:
            continue

        nac_node_vars: dict[Any, str] = {}
        local_used = set(used_vars)
        for node_id in nac_nodes:
            if node_id in node_vars:
                nac_node_vars[node_id] = node_vars[node_id]
            else:
                nac_node_vars[node_id] = _make_node_var(node_id, local_used)

        patterns: list[str] = []
        for node_id in nac_nodes:
            data = nac.nodes[node_id]
            patterns.append(
                _node_pattern(node_id, data, nac_node_vars, params, f"nac {nac_index}")
            )

        edges = _sorted_edges(nac)
        rel_prefix = f"nac{nac_index}_r"
        for edge_index, edge in enumerate(edges):
            patterns.append(
                _match_edge_pattern(
                    edge,
                    nac_node_vars,
                    params,
                    edge_index,
                    context=f"nac {nac_index}",
                    rel_prefix=rel_prefix,
                )
            )

        if not patterns:
            continue

        subquery = "MATCH " + ", ".join(patterns)
        nac_where: list[str] = []
        nac_where.extend(_distinct_node_conditions(nac_nodes, nac_node_vars))
        nac_where.extend(_distinct_edge_conditions(len(edges), rel_prefix=rel_prefix))
        if nac_where:
            subquery += " WHERE " + " AND ".join(nac_where)

        clauses.append(f"NOT EXISTS {{ {subquery} }}")

    return clauses


def _distinct_node_conditions(
    left_node_ids: Sequence[Any], node_vars: Mapping[Any, str]
) -> list[str]:
    conditions: list[str] = []
    for index, node_id in enumerate(left_node_ids):
        node_var = node_vars[node_id]
        for other_id in left_node_ids[index + 1 :]:
            conditions.append(
                f"elementId({node_var}) <> elementId({node_vars[other_id]})"
            )
    return conditions


def _distinct_edge_conditions(
    left_edge_count: int, *, rel_prefix: str = "r"
) -> list[str]:
    conditions: list[str] = []
    for index in range(left_edge_count):
        for other in range(index + 1, left_edge_count):
            conditions.append(
                f"elementId({rel_prefix}{index}) <> elementId({rel_prefix}{other})"
            )
    return conditions


def _dangling_conditions(
    deleted_nodes: Sequence[Any],
    left_edge_count: int,
    node_vars: Mapping[Any, str],
) -> list[str]:
    if not deleted_nodes:
        return []
    rel_vars = ", ".join(f"r{index}" for index in range(left_edge_count))
    rel_list = f"[{rel_vars}]"
    conditions: list[str] = []
    for node_id in deleted_nodes:
        node_var = node_vars[node_id]
        conditions.append(
            "NOT EXISTS { MATCH " f"({node_var})-[r]-() WHERE NOT r IN {rel_list} }}"
        )
    return conditions


def _build_delete_targets(plan: _RulePlan, node_vars: Mapping[Any, str]) -> list[str]:
    targets = [f"r{index}" for index in plan.deleted_edge_indices]
    targets.extend(node_vars[node_id] for node_id in plan.deleted_nodes)
    return targets


def _edge_deletion_indices(
    edges: Sequence[_EdgeRecord], preserved_edges: Counter
) -> list[int]:
    remaining = Counter(preserved_edges)
    deleted_indices: list[int] = []
    for index, edge in enumerate(edges):
        descriptor = _edge_descriptor(edge, context="left")
        if remaining[descriptor] > 0:
            remaining[descriptor] -= 1
        else:
            deleted_indices.append(index)
    return deleted_indices


def _multiset_intersection(left: Counter, right: Counter) -> Counter:
    result: Counter = Counter()
    for key in left.keys() & right.keys():
        result[key] = min(left[key], right[key])
    return result


def _validate_preserved_nodes(rule: DpoRule, preserved_nodes: set[Any]) -> None:
    for node_id in preserved_nodes:
        left_attrs = _normalized_node_attrs(rule.left, node_id, "left")
        interface_attrs = _normalized_node_attrs(rule.interface, node_id, "interface")
        right_attrs = _normalized_node_attrs(rule.right, node_id, "right")
        if left_attrs != interface_attrs or left_attrs != right_attrs:
            raise RuleSerializationError(
                "Preserved nodes must keep identical labels/props across left, "
                f"interface, and right. Mismatch on node '{node_id}'."
            )


def _normalized_node_attrs(
    graph: RuleGraph, node_id: Any, context: str
) -> tuple[tuple[str, ...], dict[str, Any]]:
    data = graph.nodes[node_id]
    labels = _normalize_labels(data.get("label"), context=f"{context} node {node_id}")
    props = _normalize_props(data.get("props"), context=f"{context} node {node_id}")
    return (tuple(sorted(labels)), props)


def _node_pattern(
    node_id: Any,
    data: Mapping[str, Any],
    node_vars: Mapping[Any, str],
    params: _ParamBuilder,
    context: str,
) -> str:
    var = node_vars[node_id]
    node_context = f"{context} node {node_id}"
    labels = _labels_fragment(data.get("label"), context=node_context)
    props = _props_fragment(data.get("props"), params, var, context=node_context)
    return f"({var}{labels}{props})"


def _match_edge_pattern(
    edge: _EdgeRecord,
    node_vars: Mapping[Any, str],
    params: _ParamBuilder,
    index: int,
    context: str,
    rel_prefix: str = "r",
) -> str:
    return _edge_pattern(
        edge,
        node_vars=node_vars,
        params=params,
        index=index,
        context=context,
        require_type=False,
        allow_anonymous=True,
        force_var=True,
        rel_prefix=rel_prefix,
    )


def _create_edge_pattern(
    edge: _EdgeRecord,
    node_vars: Mapping[Any, str],
    params: _ParamBuilder,
    index: int,
    context: str,
) -> str:
    return _edge_pattern(
        edge,
        node_vars=node_vars,
        params=params,
        index=index,
        context=context,
        require_type=True,
        allow_anonymous=False,
        force_var=False,
        rel_prefix="r",
    )


def _edge_pattern(
    edge: _EdgeRecord,
    *,
    node_vars: Mapping[Any, str],
    params: _ParamBuilder,
    index: int,
    context: str,
    require_type: bool,
    allow_anonymous: bool,
    force_var: bool,
    rel_prefix: str,
) -> str:
    source_var = node_vars[edge.source]
    target_var = node_vars[edge.target]
    edge_context = f"{context} edge {edge.source}->{edge.target}"
    # CREATE requires explicit relationship types; MATCH may omit to allow any.
    rel_type = _normalize_rel_type(
        edge.data.get("type"), required=require_type, context=edge_context
    )
    rel_props = _normalize_props(edge.data.get("props"), context=edge_context)
    rel_scope = f"{rel_prefix}{index}"
    rel_props_fragment = _props_fragment(
        rel_props, params, rel_scope, context=edge_context
    )

    rel_var = ""
    if force_var:
        rel_var = rel_scope
    elif not rel_type and rel_props and allow_anonymous:
        rel_var = rel_scope

    rel_type_fragment = f":{rel_type}" if rel_type else ""
    rel_body = f"{rel_var}{rel_type_fragment}{rel_props_fragment}"
    return f"({source_var})-[{rel_body}]->({target_var})"


def _labels_fragment(label_value: Any, *, context: str) -> str:
    labels = _normalize_labels(label_value, context=context)
    if not labels:
        return ""
    return ":" + ":".join(labels)


def _props_fragment(
    props: Mapping[str, Any],
    params: _ParamBuilder,
    scope: str,
    *,
    context: str,
) -> str:
    if not props:
        return ""
    entries = []
    for key in sorted(props.keys()):
        if not isinstance(key, str):
            raise RuleSerializationError(f"{context} property keys must be strings.")
        _validate_identifier("property key", key, context=context)
        param = params.add(scope, key, props[key])
        entries.append(f"{key}: {param}")
    return " {" + ", ".join(entries) + "}"


def _normalize_labels(value: Any, *, context: str) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        labels = [value]
    elif isinstance(value, (list, tuple, set)):
        labels = list(value)
    else:
        raise RuleSerializationError(f"{context} labels must be strings or sequences.")

    cleaned: list[str] = []
    for label in labels:
        if not isinstance(label, str):
            raise RuleSerializationError(f"{context} labels must be strings.")
        _validate_identifier("label", label, context=context)
        cleaned.append(label)
    return cleaned


def _normalize_rel_type(value: Any, *, required: bool, context: str) -> Optional[str]:
    if value is None:
        if required:
            raise RuleSerializationError(
                f"{context} relationships must define a type for creation."
            )
        return None
    if not isinstance(value, str):
        raise RuleSerializationError(f"{context} relationship types must be strings.")
    _validate_identifier("relationship type", value, context=context)
    return value


def _normalize_props(value: Any, *, context: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise RuleSerializationError(f"{context} props must be a mapping.")
    props: dict[str, Any] = {}
    for key, val in value.items():
        if not isinstance(key, str):
            raise RuleSerializationError(f"{context} prop keys must be strings.")
        _validate_identifier("property key", key, context=context)
        props[key] = val
    return props


def _validate_identifier(kind: str, value: str, *, context: str) -> None:
    if not _IDENTIFIER_RE.match(value):
        raise RuleSerializationError(
            f"{context} {kind} '{value}' is not a valid Cypher identifier."
        )


def _sanitize_identifier(value: str) -> str:
    # Normalize user-provided identifiers into safe Cypher tokens.
    cleaned = re.sub(r"[^A-Za-z0-9_]+", "_", value)
    return cleaned.strip("_").lower()


def _make_node_var(node_id: Any, used: set[str]) -> str:
    # Derive readable variable names from node IDs, with deduping.
    base = f"n_{_sanitize_identifier(str(node_id))}" or "n"
    if base == "n_":
        base = "n"
    name = base
    counter = 1
    while name in used or not name:
        name = f"{base}_{counter}"
        counter += 1
    used.add(name)
    return name


def _sorted_nodes(graph: RuleGraph) -> list[Any]:
    return sorted(graph.nodes, key=lambda item: str(item))


def _sorted_edges(graph: RuleGraph) -> list[_EdgeRecord]:
    edges = [
        _EdgeRecord(source=u, target=v, key=k, data=data)
        for u, v, k, data in graph.edges(keys=True, data=True)
    ]
    return sorted(edges, key=_edge_sort_key)


def _edge_sort_key(edge: _EdgeRecord) -> tuple[str, str, str, str, str]:
    rel_type = edge.data.get("type") or ""
    props = edge.data.get("props") or {}
    props_key = repr(_freeze_value(props))
    return (
        str(edge.source),
        str(edge.target),
        str(edge.key),
        str(rel_type),
        props_key,
    )


def _edge_descriptor(
    edge: _EdgeRecord, *, context: str
) -> tuple[Any, Any, Optional[str], tuple]:
    rel_type = _normalize_rel_type(
        edge.data.get("type"), required=False, context=context
    )
    props = _normalize_props(edge.data.get("props"), context=f"{context} edge")
    frozen_props = tuple(
        sorted((key, _freeze_value(val)) for key, val in props.items())
    )
    return (edge.source, edge.target, rel_type, frozen_props)


def _edge_multiset(graph: RuleGraph, *, context: str) -> Counter:
    counter: Counter = Counter()
    for edge in _sorted_edges(graph):
        counter[_edge_descriptor(edge, context=context)] += 1
    return counter


def _edge_difference(graph: RuleGraph, preserved_edges: Counter) -> list[_EdgeRecord]:
    remaining = Counter(preserved_edges)
    created: list[_EdgeRecord] = []
    for edge in _sorted_edges(graph):
        descriptor = _edge_descriptor(edge, context="right")
        if remaining[descriptor] > 0:
            remaining[descriptor] -= 1
        else:
            created.append(edge)
    return created


def _multiset_is_subset(left: Counter, right: Counter) -> bool:
    for key, count in left.items():
        if right.get(key, 0) < count:
            return False
    return True


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
