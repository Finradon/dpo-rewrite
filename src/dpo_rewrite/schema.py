"""JSON schema helpers for DPO rule catalogs."""

from __future__ import annotations

import json
from functools import lru_cache
from importlib import resources
from typing import Any, Mapping

from jsonschema import Draft202012Validator

_DPO_RULE_SCHEMA_PATH = "schemas/dpo_rule.schema.json"


def load_dpo_rule_schema() -> dict[str, Any]:
    """Load the DPO rule JSON schema from package data."""
    with resources.files("dpo_rewrite").joinpath(_DPO_RULE_SCHEMA_PATH).open(
        "r", encoding="utf-8"
    ) as handle:
        return json.load(handle)


@lru_cache
def _dpo_rule_validator() -> Draft202012Validator:
    return Draft202012Validator(load_dpo_rule_schema())


def iter_dpo_rule_errors(payload: Mapping[str, Any]) -> list[str]:
    """Return a list of human-readable schema errors."""
    validator = _dpo_rule_validator()
    errors = sorted(validator.iter_errors(payload), key=lambda err: list(err.path))
    return [_format_error(error) for error in errors]


def validate_dpo_rule_payload(payload: Mapping[str, Any]) -> None:
    """Validate a DPO rule payload against the JSON schema.

    Raises:
        ValueError: if the payload does not match the schema.
    """
    errors = iter_dpo_rule_errors(payload)
    if errors:
        raise ValueError("Invalid DPO rule payload: " + "; ".join(errors))


def _format_error(error: Exception) -> str:
    path = ""
    if hasattr(error, "path"):
        for entry in error.path:
            if isinstance(entry, int):
                path += f"[{entry}]"
            else:
                path += f".{entry}"
    if not path:
        path = "<root>"
    elif path.startswith("."):
        path = path[1:]
    message = getattr(error, "message", str(error))
    return f"{path}: {message}"
