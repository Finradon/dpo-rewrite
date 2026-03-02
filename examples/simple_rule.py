from dpo_rewrite import load_rule, to_cypher

payload = {
    "left": {
        "nodes": [{"id": "req1", "label": "Requirement", "props": {"id": "REQ-1"}}],
        "edges": [],
    },
    "interface": {
        "nodes": [{"id": "req1", "label": "Requirement", "props": {"id": "REQ-1"}}],
        "edges": [],
    },
    "right": {
        "nodes": [
            {"id": "req1", "label": "Requirement", "props": {"id": "REQ-1"}},
            {"id": "comp1", "label": "Component", "props": {"name": "Beam"}},
        ],
        "edges": [{"source": "req1", "target": "comp1", "type": "SATISFIES"}],
    },
}

rule = load_rule(payload)
cypher = to_cypher(rule)

print(cypher.query)
print(cypher.params)
