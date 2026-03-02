# dpo-rewrite

Minimal DPO graph rewriting core for graph databases.

## Scope

- DPO rule representation (`left`, `interface`, `right`, optional `nac`)
- JSON import and schema validation
- Cypher export for graph database execution

## Install

```bash
pip install dpo-rewrite
```

## Usage

```python
from dpo_rewrite import load_rule, to_cypher, validate

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

validate(payload)
rule = load_rule(payload)
cypher = to_cypher(rule)
print(cypher.query)
print(cypher.params)
```

## Sample Script

See `examples/simple_rule.py` for a minimal end-to-end example.
