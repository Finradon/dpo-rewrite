import networkx as nx

from dpo_rewrite import apply_rule
from dpo_rewrite.rules import DpoRule, add_edge, add_node


def build_rule() -> DpoRule:
    left = nx.MultiDiGraph()
    interface = nx.MultiDiGraph()
    right = nx.MultiDiGraph()

    add_node(left, "a", label="Node", props={"id": "A"})
    add_node(interface, "a", label="Node", props={"id": "A"})
    add_node(right, "a", label="Node", props={"id": "A"})
    add_node(right, "b", label="Node", props={"id": "B"})
    add_edge(right, "a", "b", rel_type="LINKS_TO")

    return DpoRule(left=left, interface=interface, right=right)


def build_host() -> nx.DiGraph:
    host = nx.DiGraph()
    host.add_node("n1", label="Node", props={"id": "A"})
    return host


if __name__ == "__main__":
    rule = build_rule()
    host = build_host()
    result = apply_rule(rule, host)

    print("Applied:", result.applied)
    print("Matched mapping:", result.match.node_mapping if result.match else None)
    print("Nodes:", list(result.graph.nodes(data=True)))
    print("Edges:", list(result.graph.edges(data=True, keys=True)))
