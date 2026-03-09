# dpo-rewrite

Minimal DPO graph rewriting core for graph workflows and graph databases.

## Scope

- DPO rule representation (`left`, `interface`, `right`, optional `nac`)
- JSON import and schema validation
- Application to nx.MultiDiGraph
- Cypher export for graph database execution

## Install

Available on [PyPI](https://pypi.org/project/dpo-rewrite):
```bash
pip install dpo-rewrite
```

## Usage

```python
from dpo_rewrite import load_rule, to_cypher, validate

payload = {
    "left": {
        "nodes": [{"id": "a", "label": "Node", "props": {"id": "A"}}],
        "edges": [],
    },
    "interface": {
        "nodes": [{"id": "a", "label": "Node", "props": {"id": "A"}}],
        "edges": [],
    },
    "right": {
        "nodes": [
            {"id": "a", "label": "Node", "props": {"id": "A"}},
            {"id": "b", "label": "Node", "props": {"id": "B"}},
        ],
        "edges": [{"source": "a", "target": "b", "type": "LINKS_TO"}],
    },
}

validate(payload)
rule = load_rule(payload)
cypher = to_cypher(rule)
print(cypher.query)
print(cypher.params)
```

## Sample Script

See `examples/` for a minimal end-to-end examples.
