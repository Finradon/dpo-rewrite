import networkx as nx
import pytest

from dpo_rewrite.cypher import RuleSerializationError, rule_to_cypher
from dpo_rewrite.rules import DpoRule, add_edge, add_node


def test_rule_to_cypher_create_only() -> None:
    left = nx.MultiDiGraph()
    interface = nx.MultiDiGraph()
    right = nx.MultiDiGraph()

    add_node(left, "req1", label="Requirement", props={"id": "REQ-1"})
    add_node(interface, "req1", label="Requirement", props={"id": "REQ-1"})
    add_node(right, "req1", label="Requirement", props={"id": "REQ-1"})
    add_node(right, "comp1", label="Component", props={"name": "Beam"})

    add_edge(right, "req1", "comp1", rel_type="SATISFIES")

    rule = DpoRule(left=left, interface=interface, right=right)
    cypher = rule_to_cypher(rule)

    assert cypher.query == (
        "MATCH (n_req1:Requirement {id: $n_req1_id})\n"
        "CREATE (n_comp1:Component {name: $n_comp1_name})\n"
        "CREATE (n_req1)-[:SATISFIES]->(n_comp1)"
    )
    assert cypher.params == {
        "n_req1_id": "REQ-1",
        "n_comp1_name": "Beam",
    }


def test_rule_to_cypher_multilabel_create() -> None:
    left = nx.MultiDiGraph()
    interface = nx.MultiDiGraph()
    right = nx.MultiDiGraph()

    add_node(left, "req1", label=["Requirement", "Spec"], props={"id": "REQ-1"})
    add_node(interface, "req1", label=["Requirement", "Spec"], props={"id": "REQ-1"})
    add_node(right, "req1", label=["Requirement", "Spec"], props={"id": "REQ-1"})
    add_node(right, "comp1", label=["Component", "Steel"], props={"name": "Beam"})

    rule = DpoRule(left=left, interface=interface, right=right)
    cypher = rule_to_cypher(rule)

    assert cypher.query == (
        "MATCH (n_req1:Requirement:Spec {id: $n_req1_id})\n"
        "CREATE (n_comp1:Component:Steel {name: $n_comp1_name})"
    )
    assert cypher.params == {
        "n_req1_id": "REQ-1",
        "n_comp1_name": "Beam",
    }


def test_rule_to_cypher_multilabel_from_json() -> None:
    payload = {
        "left": {
            "nodes": [
                {
                    "id": "req1",
                    "label": ["Requirement", "Spec"],
                    "props": {"id": "REQ-1"},
                }
            ],
            "edges": [],
        },
        "interface": {
            "nodes": [
                {
                    "id": "req1",
                    "label": ["Requirement", "Spec"],
                    "props": {"id": "REQ-1"},
                }
            ],
            "edges": [],
        },
        "right": {
            "nodes": [
                {
                    "id": "req1",
                    "label": ["Requirement", "Spec"],
                    "props": {"id": "REQ-1"},
                },
                {
                    "id": "comp1",
                    "label": ["Component", "Steel"],
                    "props": {"name": "Beam"},
                },
            ],
            "edges": [],
        },
    }

    rule = DpoRule.from_json(payload, validate=True)
    cypher = rule_to_cypher(rule)

    assert cypher.query == (
        "MATCH (n_req1:Requirement:Spec {id: $n_req1_id})\n"
        "CREATE (n_comp1:Component:Steel {name: $n_comp1_name})"
    )
    assert cypher.params == {
        "n_req1_id": "REQ-1",
        "n_comp1_name": "Beam",
    }


def test_rule_to_cypher_deletes_edge() -> None:
    left = nx.MultiDiGraph()
    interface = nx.MultiDiGraph()
    right = nx.MultiDiGraph()

    add_node(left, "a", label="Thing", props={"id": "A"})
    add_node(interface, "a", label="Thing", props={"id": "A"})
    add_node(right, "a", label="Thing", props={"id": "A"})
    add_node(left, "b", label="Thing", props={"id": "B"})
    add_node(interface, "b", label="Thing", props={"id": "B"})
    add_node(right, "b", label="Thing", props={"id": "B"})

    add_edge(left, "a", "b", rel_type="LINKS")

    rule = DpoRule(left=left, interface=interface, right=right)
    cypher = rule_to_cypher(rule)

    assert cypher.query == (
        "MATCH (n_a:Thing {id: $n_a_id}), (n_b:Thing {id: $n_b_id}), "
        "(n_a)-[r0:LINKS]->(n_b)\n"
        "WHERE elementId(n_a) <> elementId(n_b)\n"
        "DELETE r0"
    )
    assert cypher.params == {
        "n_a_id": "A",
        "n_b_id": "B",
    }


def test_rule_to_cypher_deletes_node() -> None:
    left = nx.MultiDiGraph()
    interface = nx.MultiDiGraph()
    right = nx.MultiDiGraph()

    add_node(left, "a", label="Thing", props={"id": "A"})

    rule = DpoRule(left=left, interface=interface, right=right)
    cypher = rule_to_cypher(rule)

    assert cypher.query == (
        "MATCH (n_a:Thing {id: $n_a_id})\n"
        "WHERE NOT EXISTS { MATCH (n_a)-[r]-() WHERE NOT r IN [] }\n"
        "DELETE n_a"
    )
    assert cypher.params == {
        "n_a_id": "A",
    }


def test_rule_to_cypher_rejects_missing_interface_nodes() -> None:
    left = nx.MultiDiGraph()
    interface = nx.MultiDiGraph()
    right = nx.MultiDiGraph()

    add_node(left, "req1", label="Requirement", props={"id": "REQ-1"})
    add_node(right, "req1", label="Requirement", props={"id": "REQ-1"})

    rule = DpoRule(left=left, interface=interface, right=right)

    with pytest.raises(
        RuleSerializationError,
        match="Nodes present in both left and right must be in interface",
    ):
        rule_to_cypher(rule)


def test_rule_to_cypher_with_nac_guard() -> None:
    left = nx.MultiDiGraph()
    interface = nx.MultiDiGraph()
    right = nx.MultiDiGraph()
    nac = nx.MultiDiGraph()

    add_node(left, "req1", label="Requirement", props={"id": "REQ-1"})
    add_node(interface, "req1", label="Requirement", props={"id": "REQ-1"})
    add_node(right, "req1", label="Requirement", props={"id": "REQ-1"})
    add_node(right, "comp1", label="Component", props={"name": "Beam"})
    add_edge(right, "req1", "comp1", rel_type="SATISFIES")

    add_node(nac, "req1", label="Requirement", props={"id": "REQ-1"})
    add_node(nac, "existing_target")
    add_edge(nac, "req1", "existing_target", rel_type="SATISFIES")

    rule = DpoRule(left=left, interface=interface, right=right, nacs=(nac,))
    cypher = rule_to_cypher(rule)

    assert cypher.query == (
        "MATCH (n_req1:Requirement {id: $n_req1_id})\n"
        "WHERE NOT EXISTS { MATCH (n_existing_target), "
        "(n_req1:Requirement {id: $n_req1_id_1}), "
        "(n_req1)-[nac0_r0:SATISFIES]->(n_existing_target) "
        "WHERE elementId(n_existing_target) <> elementId(n_req1) }\n"
        "CREATE (n_comp1:Component {name: $n_comp1_name})\n"
        "CREATE (n_req1)-[:SATISFIES]->(n_comp1)"
    )
    assert cypher.params == {
        "n_req1_id": "REQ-1",
        "n_req1_id_1": "REQ-1",
        "n_comp1_name": "Beam",
    }
