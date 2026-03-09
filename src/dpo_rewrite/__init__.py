"""Public API for DPO rewrite."""

from __future__ import annotations

from typing import Any, Mapping

from .apply import Match, RewriteResult, apply_rule
from .cypher import CypherQuery, RuleSerializationError, rule_to_cypher
from .rules import DpoRule
from .schema import (
    iter_dpo_rule_errors,
    load_dpo_rule_schema,
    validate_dpo_rule_payload,
)

__all__ = [
    "CypherQuery",
    "DpoRule",
    "Match",
    "RuleSerializationError",
    "RewriteResult",
    "apply_rule",
    "iter_errors",
    "load_rule",
    "rule_to_cypher",
    "schema",
    "to_cypher",
    "validate",
]


def load_rule(payload: Mapping[str, Any], *, validate: bool = True) -> DpoRule:
    """Validate (optional) and parse a JSON payload into a DpoRule."""
    return DpoRule.from_json(payload, validate=validate)


def to_cypher(
    payload_or_rule: Mapping[str, Any] | DpoRule, *, validate: bool = True
) -> CypherQuery:
    """Validate (optional), parse, and serialize a rule to Cypher."""
    if isinstance(payload_or_rule, DpoRule):
        rule = payload_or_rule
    else:
        rule = load_rule(payload_or_rule, validate=validate)
    return rule_to_cypher(rule)


def validate(payload: Mapping[str, Any]) -> None:
    """Validate a payload against the DPO rule JSON schema."""
    validate_dpo_rule_payload(payload)


def iter_errors(payload: Mapping[str, Any]) -> list[str]:
    """Return a list of schema validation errors."""
    return iter_dpo_rule_errors(payload)


def schema() -> dict[str, Any]:
    """Return the DPO rule JSON schema."""
    return load_dpo_rule_schema()
