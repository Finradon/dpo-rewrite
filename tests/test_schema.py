import pytest

from dpo_rewrite.rules import DpoRule
from dpo_rewrite.schema import iter_dpo_rule_errors, validate_dpo_rule_payload

VALID_PAYLOAD = {
    "schema_version": "1.0",
    "rule_id": "dpo-rewrite:test:rename",
    "name": "Rename requirement",
    "left": {
        "nodes": [{"id": "r1", "label": "Requirement", "props": {"name": "Old"}}],
        "edges": [],
    },
    "interface": {
        "nodes": [{"id": "r1", "label": "Requirement"}],
        "edges": [],
    },
    "right": {
        "nodes": [{"id": "r1", "label": "Requirement", "props": {"name": "New"}}],
        "edges": [],
    },
}


def test_validate_dpo_rule_payload_ok() -> None:
    validate_dpo_rule_payload(VALID_PAYLOAD)
    assert iter_dpo_rule_errors(VALID_PAYLOAD) == []


def test_validate_dpo_rule_payload_missing_id() -> None:
    bad_payload = {
        "left": {"nodes": [{"label": "Requirement"}], "edges": []},
        "interface": {"nodes": [], "edges": []},
        "right": {"nodes": [], "edges": []},
    }
    errors = iter_dpo_rule_errors(bad_payload)
    assert errors
    with pytest.raises(ValueError):
        validate_dpo_rule_payload(bad_payload)


def test_from_json_with_validation() -> None:
    rule = DpoRule.from_json(VALID_PAYLOAD, validate=True)
    assert rule.left.number_of_nodes() == 1
    assert rule.right.number_of_nodes() == 1
