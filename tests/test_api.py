import pytest

from dpo_rewrite import (
    CypherQuery,
    DpoRule,
    iter_errors,
    load_rule,
    schema,
    to_cypher,
    validate,
)
from dpo_rewrite.cypher import RuleSerializationError


def _sample_payload() -> dict:
    return {
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
        "nac": [],
    }


def test_validate_and_iter_errors() -> None:
    payload = _sample_payload()
    validate(payload)
    assert iter_errors(payload) == []


def test_schema_returns_mapping() -> None:
    data = schema()
    assert isinstance(data, dict)
    assert data.get("$schema") == "https://json-schema.org/draft/2020-12/schema"


# def test_load_rule_and_round_trip_to_json() -> None:
#     payload = _sample_payload()
#     rule = load_rule(payload)
#     assert isinstance(rule, DpoRule)
#     assert rule.to_json() == payload


def test_to_cypher_accepts_payload_or_rule() -> None:
    payload = _sample_payload()
    query_from_payload = to_cypher(payload)
    assert isinstance(query_from_payload, CypherQuery)

    rule = load_rule(payload)
    query_from_rule = to_cypher(rule)
    assert query_from_rule.query == query_from_payload.query
    assert query_from_rule.params == query_from_payload.params


def test_to_cypher_respects_validation_flag() -> None:
    payload = _sample_payload()
    payload["right"]["edges"][0]["type"] = "NOT-VALID"

    with pytest.raises(RuleSerializationError):
        to_cypher(payload, validate=False)
