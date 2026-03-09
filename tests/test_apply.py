import networkx as nx

from dpo_rewrite import apply_rule
from dpo_rewrite.rules import DpoRule, add_edge, add_node


def test_apply_rule_creates_node_and_edge() -> None:
    left = nx.MultiDiGraph()
    interface = nx.MultiDiGraph()
    right = nx.MultiDiGraph()
    host = nx.MultiDiGraph()

    add_node(left, "req1", label="Requirement", props={"id": "REQ-1"})
    add_node(interface, "req1", label="Requirement", props={"id": "REQ-1"})
    add_node(right, "req1", label="Requirement", props={"id": "REQ-1"})
    add_node(right, "comp1", label="Component", props={"name": "Beam"})
    add_edge(right, "req1", "comp1", rel_type="SATISFIES")

    add_node(host, "h_req", label="Requirement", props={"id": "REQ-1"})

    rule = DpoRule(left=left, interface=interface, right=right)
    result = apply_rule(rule, host)

    assert result.applied is True
    assert "h_req" in result.graph
    assert "comp1" in result.graph
    assert result.graph.has_edge("h_req", "comp1")


def test_apply_rule_blocks_on_nac() -> None:
    left = nx.MultiDiGraph()
    interface = nx.MultiDiGraph()
    right = nx.MultiDiGraph()
    nac = nx.MultiDiGraph()
    host = nx.MultiDiGraph()

    add_node(left, "req1", label="Requirement", props={"id": "REQ-1"})
    add_node(interface, "req1", label="Requirement", props={"id": "REQ-1"})
    add_node(right, "req1", label="Requirement", props={"id": "REQ-1"})
    add_node(right, "comp1", label="Component", props={"name": "Beam"})
    add_edge(right, "req1", "comp1", rel_type="SATISFIES")

    add_node(nac, "req1", label="Requirement", props={"id": "REQ-1"})
    add_node(nac, "other")
    add_edge(nac, "req1", "other", rel_type="SATISFIES")

    add_node(host, "h_req", label="Requirement", props={"id": "REQ-1"})
    add_node(host, "existing")
    add_edge(host, "h_req", "existing", rel_type="SATISFIES")

    rule = DpoRule(left=left, interface=interface, right=right, nacs=(nac,))
    result = apply_rule(rule, host)

    assert result.applied is False
    assert "comp1" not in result.graph


def test_apply_rule_rejects_dangling_delete() -> None:
    left = nx.MultiDiGraph()
    interface = nx.MultiDiGraph()
    right = nx.MultiDiGraph()
    host = nx.MultiDiGraph()

    add_node(left, "a", label="Thing", props={"id": "A"})

    add_node(host, "a", label="Thing", props={"id": "A"})
    add_node(host, "x")
    add_edge(host, "a", "x", rel_type="LINKS")

    rule = DpoRule(left=left, interface=interface, right=right)
    result = apply_rule(rule, host)

    assert result.applied is False
    assert "a" in result.graph


def test_apply_rule_deletes_edge() -> None:
    left = nx.MultiDiGraph()
    interface = nx.MultiDiGraph()
    right = nx.MultiDiGraph()
    host = nx.MultiDiGraph()

    add_node(left, "a", label="Thing", props={"id": "A"})
    add_node(left, "b", label="Thing", props={"id": "B"})
    add_edge(left, "a", "b", rel_type="LINKS")

    add_node(interface, "a", label="Thing", props={"id": "A"})
    add_node(interface, "b", label="Thing", props={"id": "B"})

    add_node(right, "a", label="Thing", props={"id": "A"})
    add_node(right, "b", label="Thing", props={"id": "B"})

    add_node(host, "h_a", label="Thing", props={"id": "A"})
    add_node(host, "h_b", label="Thing", props={"id": "B"})
    add_edge(host, "h_a", "h_b", rel_type="LINKS")

    rule = DpoRule(left=left, interface=interface, right=right)
    result = apply_rule(rule, host)

    assert result.applied is True
    assert result.graph.number_of_edges() == 0


def test_apply_rule_accepts_digraph_host() -> None:
    left = nx.MultiDiGraph()
    interface = nx.MultiDiGraph()
    right = nx.MultiDiGraph()
    host = nx.DiGraph()

    add_node(left, "a", label="Thing", props={"id": "A"})
    add_node(interface, "a", label="Thing", props={"id": "A"})
    add_node(right, "a", label="Thing", props={"id": "A"})
    add_node(right, "b")
    add_edge(right, "a", "b", rel_type="LINKS")

    host.add_node("h_a", label="Thing", props={"id": "A"})

    rule = DpoRule(left=left, interface=interface, right=right)
    result = apply_rule(rule, host)

    assert result.applied is True
    assert isinstance(result.graph, nx.MultiDiGraph)
    assert result.graph.has_edge("h_a", "b")
