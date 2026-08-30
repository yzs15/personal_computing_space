import json

import pytest

from loom_v2.contracts.io_schema import validate


SCORE_SCHEMA = {
    "type": "object",
    "required": ["scores"],
    "properties": {
        "scores": {
            "type": "array",
            "items": {"type": "number", "minimum": 0, "maximum": 100},
        }
    },
}


def test_valid_value_has_no_errors() -> None:
    value = json.loads('{"scores":[80, 90]}')

    assert validate(SCORE_SCHEMA, value) == []


def test_extra_payload_wrapper_reports_required_scores() -> None:
    errors = validate(SCORE_SCHEMA, {"payload": {"scores": [80]}})

    assert [(error.path, error.keyword) for error in errors] == [("$", "required")]
    assert "payload" not in errors[0].message


def test_type_items_enum_and_range_errors_are_structured() -> None:
    schema = {
        "type": "array",
        "items": {
            "type": "number",
            "enum": [1, 2],
            "minimum": 1,
            "maximum": 2,
        },
    }

    errors = validate(schema, ["bad", 3])

    assert all(error.keyword in {"type", "enum", "minimum", "maximum"} for error in errors)
    assert all(hasattr(error, "path") and hasattr(error, "expected") for error in errors)


@pytest.mark.parametrize(
    "schema",
    [
        {"type": "object", "additionalProperties": False},
        {"$ref": "#/definitions/x"},
        {"type": "string", "format": "email"},
    ],
)
def test_unsupported_keywords_are_rejected_instead_of_ignored(schema: dict) -> None:
    with pytest.raises(ValueError, match="schema_unsupported_keyword"):
        validate(schema, {})


def test_invalid_schema_is_rejected_by_draft_2020_12_meta_validation() -> None:
    with pytest.raises(ValueError, match="schema_invalid"):
        validate({"type": "not-a-json-schema-type"}, {})


def test_error_order_is_stable_and_payload_is_not_in_error_text() -> None:
    schema = {
        "type": "object",
        "required": ["a", "b"],
        "properties": {"a": {"type": "string"}, "b": {"type": "number"}},
    }

    first = validate(schema, {"secret": "do-not-leak"})
    second = validate(schema, {"secret": "do-not-leak"})

    assert [error.model_dump() for error in first] == [error.model_dump() for error in second]
    assert "do-not-leak" not in repr(first)
