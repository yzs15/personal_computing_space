"""Deterministic validation for Loom's supported JSON Schema subset.

The ``jsonschema`` package is the standards implementation.  This module is
the Loom policy boundary: it rejects keywords outside ``io.v1`` and converts
third-party errors into a stable, payload-free representation shared by the
Observer and Slaves.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import Any

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError as JsonSchemaError
from pydantic import BaseModel, ConfigDict


SUPPORTED_KEYWORDS = frozenset(
    {
        "type",
        "required",
        "properties",
        "items",
        "enum",
        "const",
        "minimum",
        "maximum",
    }
)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ValidationError(BaseModel):
    """Stable, non-sensitive validation evidence for one schema failure."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    path: str
    keyword: str
    message: str
    expected: Any | None = None
    observed: str | None = None


class SchemaPolicyError(ValueError):
    """Raised when a schema is outside the supported ``io.v1`` policy."""


def _reject_unsupported_keywords(schema: Any, *, path: str = "$") -> None:
    """Reject unsupported keywords before the standards validator sees them."""

    if isinstance(schema, bool):
        return
    if not isinstance(schema, Mapping):
        # Draft202012Validator.check_schema will provide the complete schema
        # error below.  Keeping this walk side-effect free makes the failure
        # deterministic for malformed input too.
        return

    for keyword in schema:
        if keyword not in SUPPORTED_KEYWORDS:
            raise SchemaPolicyError(f"schema_unsupported_keyword:{keyword}")

    properties = schema.get("properties")
    if isinstance(properties, Mapping):
        for property_name, child in properties.items():
            _reject_unsupported_keywords(child, path=f"{path}.properties[{property_name!r}]")

    items = schema.get("items")
    if isinstance(items, Mapping) or isinstance(items, bool):
        _reject_unsupported_keywords(items, path=f"{path}.items")


def validate_schema(schema: Any) -> None:
    """Validate a schema against Draft 2020-12 and Loom's strict subset."""

    _reject_unsupported_keywords(schema)
    try:
        Draft202012Validator.check_schema(schema)
    except JsonSchemaError as exc:
        raise SchemaPolicyError("schema_invalid") from exc


def _json_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def _path(parts: tuple[Any, ...] | list[Any]) -> str:
    result = "$"
    for part in parts:
        if isinstance(part, int):
            result += f"[{part}]"
        elif isinstance(part, str) and _IDENTIFIER.fullmatch(part):
            result += f".{part}"
        else:
            # Property names come from the schema/path, never from a value.
            result += f"[{json.dumps(str(part), ensure_ascii=False)}]"
    return result


def _expected(error: Any) -> Any:
    validator = str(error.validator)
    if validator == "required":
        return "required property"
    if validator == "type":
        return error.validator_value
    if validator == "enum":
        return "one of allowed values"
    if validator == "const":
        return "constant"
    if validator in {"minimum", "maximum"}:
        return error.validator_value
    if validator == "items":
        return "item schema"
    if validator == "properties":
        return "property schemas"
    return None


def _message(keyword: str) -> str:
    return {
        "required": "required property is missing",
        "type": "value has an invalid type",
        "properties": "object properties are invalid",
        "items": "array items are invalid",
        "enum": "value is not an allowed choice",
        "const": "value does not equal the required constant",
        "minimum": "number is below the minimum",
        "maximum": "number is above the maximum",
    }.get(keyword, "value failed schema validation")


def validate(schema: Any, value: Any) -> list[ValidationError]:
    """Return deterministic errors for ``value`` against a supported schema.

    The function never includes the observed payload value in an error.  It
    returns an empty list for a valid value and raises ``ValueError`` for an
    invalid or unsupported schema.
    """

    validate_schema(schema)
    validator = Draft202012Validator(schema)
    errors: list[ValidationError] = []
    for error in validator.iter_errors(value):
        keyword = str(error.validator)
        errors.append(
            ValidationError(
                path=_path(tuple(error.absolute_path)),
                keyword=keyword,
                message=_message(keyword),
                expected=_expected(error),
                observed=_json_type(error.instance),
            )
        )
    return sorted(errors, key=lambda item: (item.path, item.keyword, item.message))


__all__ = ["SUPPORTED_KEYWORDS", "SchemaPolicyError", "ValidationError", "validate", "validate_schema"]
