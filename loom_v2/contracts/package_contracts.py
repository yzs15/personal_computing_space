from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError, ValidationError


PackageContractKey = tuple[str, str, str]
DRIVER_ORCHESTRATOR_KEY: PackageContractKey = (
    "function",
    "container:python_orchestrator",
    "1",
)


_LOWER_SHA256 = r"[0-9a-f]{64}"
_CONTENT_RESOURCE_ID_PATTERN = rf"^content://sha256/{_LOWER_SHA256}$"
_ORIGIN_PATH_PATTERN = r"^/(?!/)[^?#\\\u0000-\u0020\u007f]*$"
_OCI_DIGEST_REF_PATTERN = (
    rf"^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?(?::[0-9]+)?/"
    rf"(?:[a-z0-9]+(?:[._-][a-z0-9]+)*/)*"
    rf"[a-z0-9]+(?:[._-][a-z0-9]+)*@sha256:{_LOWER_SHA256}$"
)


_RESOURCE_REF_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["resource_id"],
    "properties": {
        "resource_id": {"type": "string", "minLength": 1},
        "version_or_digest": {"type": ["string", "null"]},
        "access_binding": {"type": "object"},
        "identity_criterion": {"type": ["string", "null"]},
        "provenance": {"type": "array", "items": {"type": "object"}},
    },
    "additionalProperties": False,
}


_CONTENT_REF_SCHEMA: dict[str, Any] = {
    **_RESOURCE_REF_SCHEMA,
    "required": ["resource_id", "identity_criterion"],
    "properties": {
        **_RESOURCE_REF_SCHEMA["properties"],
        "resource_id": {"type": "string", "pattern": _CONTENT_RESOURCE_ID_PATTERN},
        "version_or_digest": {"type": "null"},
        "identity_criterion": {"const": "content_digest"},
    },
}


_DESCRIPTOR_REF_SCHEMA: dict[str, Any] = {
    **_RESOURCE_REF_SCHEMA,
    "required": ["resource_id", "version_or_digest", "identity_criterion"],
    "properties": {
        **_RESOURCE_REF_SCHEMA["properties"],
        "version_or_digest": {"type": "string", "pattern": rf"^{_LOWER_SHA256}$"},
        "identity_criterion": {"const": "descriptor_digest"},
    },
}


def _contract_schema(
    package_type: str,
    execution_kind: str,
    execution_version: str,
    body: dict[str, Any],
    runtime_binding: dict[str, Any],
    *,
    max_exports: int | None = None,
) -> dict[str, Any]:
    capability_exports: dict[str, Any] = {
        "type": "array",
        "minItems": 1,
        "items": {
            "type": "object",
            "required": [
                "capability_descriptor_ref",
                "io_contract_ref",
                "effect_class",
                "permissions",
                "replay_safety",
                "runtime_binding",
            ],
            "properties": {
                "capability_descriptor_ref": {"$ref": "#/$defs/descriptor_ref"},
                "io_contract_ref": {"$ref": "#/$defs/content_ref"},
                "effect_class": {"type": "string"},
                "permissions": {"type": "array", "items": {"type": "string"}},
                "replay_safety": {"type": "string"},
                "runtime_binding": runtime_binding,
            },
            "additionalProperties": False,
        },
    }
    if max_exports is not None:
        capability_exports["maxItems"] = max_exports
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "required": ["package_type", "execution", "capability_exports", "body"],
        "properties": {
            "package_type": {"const": package_type},
            "execution": {
                "type": "object",
                "required": ["kind", "version"],
                "properties": {
                    "kind": {"const": execution_kind},
                    "version": {"const": execution_version},
                },
                "additionalProperties": False,
            },
            "capability_exports": capability_exports,
            "body": body,
        },
        "additionalProperties": False,
        "$defs": {
            "resource_ref": _RESOURCE_REF_SCHEMA,
            "content_ref": _CONTENT_REF_SCHEMA,
            "descriptor_ref": _DESCRIPTOR_REF_SCHEMA,
        },
    }


PROCESS_JSON_STDIO_V1_SCHEMA = _contract_schema(
    "function",
    "process:json_stdio",
    "1",
    {
        "type": "object",
        "required": ["program_content_ref"],
        "properties": {
            "program_content_ref": {"$ref": "#/$defs/content_ref"},
            "effective_constraint_refs": {"type": "array", "items": {"type": "object"}},
            "provider_fillable_hole_refs": {"type": "array", "items": {"type": "string"}},
            "semantic_closed": {"type": "boolean"},
        },
        "additionalProperties": False,
    },
    {"type": "object", "maxProperties": 0},
    max_exports=1,
)


PYTHON_ORCHESTRATOR_V1_SCHEMA = _contract_schema(
    "function",
    "container:python_orchestrator",
    "1",
    {
        "type": "object",
        "required": ["program_content_ref", "allowed_node_package_refs", "max_nodes", "max_live_nodes"],
        "properties": {
            "program_content_ref": {"$ref": "#/$defs/content_ref"},
            "captures_run_state": {"type": "boolean"},
            "captured_secret_refs": {"type": "array", "items": {"type": "string"}},
            "captured_path_refs": {"type": "array", "items": {"type": "string"}},
            "semantic_closed": {"type": "boolean"},
            "allowed_node_package_refs": {
                "type": "array",
                "minItems": 1,
                "items": {"$ref": "#/$defs/resource_ref"},
            },
            "max_nodes": {"type": "integer", "minimum": 1},
            "max_live_nodes": {"type": "integer", "minimum": 1},
        },
        "additionalProperties": False,
    },
    {"type": "object", "maxProperties": 0},
    max_exports=1,
)


CONTAINER_HTTP_V1_SCHEMA = _contract_schema(
    "service",
    "container:http",
    "1",
    {
        "type": "object",
        "required": ["image_ref", "container_port", "health_path"],
        "properties": {
            "image_ref": {
                "type": "string",
                "pattern": _OCI_DIGEST_REF_PATTERN,
            },
            "container_port": {"type": "integer", "minimum": 1, "maximum": 65535},
            "health_path": {
                "type": "string",
                "pattern": _ORIGIN_PATH_PATTERN,
            },
        },
        "additionalProperties": False,
    },
    {
        "type": "object",
        "required": ["path"],
        "properties": {
            "path": {
                "type": "string",
                "pattern": _ORIGIN_PATH_PATTERN,
            }
        },
        "additionalProperties": False,
    },
)


@dataclass
class PackageContractRegistry:
    """Operator-owned, append-only mapping from execution keys to JSON Schema."""

    _schemas: dict[PackageContractKey, dict[str, Any]] = field(default_factory=dict)

    def register(self, key: PackageContractKey, schema: dict[str, Any]) -> None:
        if not isinstance(schema, dict):
            raise ValueError("package_contract_schema_invalid")
        normalized_key: PackageContractKey = (
            str(key[0]),
            str(key[1]),
            str(key[2]),
        )
        if not all(normalized_key):
            raise ValueError("package_contract_key_invalid")
        try:
            Draft202012Validator.check_schema(schema)
        except SchemaError as exc:
            raise ValueError("package_contract_schema_invalid") from exc
        properties = schema.get("properties")
        execution_schema = (
            properties.get("execution") if isinstance(properties, dict) else None
        )
        execution_properties = (
            execution_schema.get("properties")
            if isinstance(execution_schema, dict)
            else None
        )
        package_type_schema = (
            properties.get("package_type") if isinstance(properties, dict) else None
        )
        execution_kind_schema = (
            execution_properties.get("kind")
            if isinstance(execution_properties, dict)
            else None
        )
        execution_version_schema = (
            execution_properties.get("version")
            if isinstance(execution_properties, dict)
            else None
        )
        required = schema.get("required")
        if (
            not isinstance(properties, dict)
            or not isinstance(execution_schema, dict)
            or not isinstance(execution_properties, dict)
            or not isinstance(required, list)
            or not {"package_type", "execution", "capability_exports", "body"}
            <= set(required)
            or schema.get("additionalProperties") is not False
            or not isinstance(package_type_schema, dict)
            or package_type_schema.get("const") != normalized_key[0]
            or not isinstance(execution_kind_schema, dict)
            or execution_kind_schema.get("const") != normalized_key[1]
            or not isinstance(execution_version_schema, dict)
            or execution_version_schema.get("const") != normalized_key[2]
            or "capability_exports" not in properties
            or "body" not in properties
        ):
            raise ValueError("package_contract_schema_invalid")
        candidate = deepcopy(schema)
        existing = self._schemas.get(normalized_key)
        if existing is not None:
            if existing != candidate:
                raise ValueError("package_contract_conflict")
            return
        self._schemas[normalized_key] = candidate

    def schema_for(self, key: PackageContractKey) -> dict[str, Any]:
        normalized_key: PackageContractKey = (
            str(key[0]),
            str(key[1]),
            str(key[2]),
        )
        try:
            return deepcopy(self._schemas[normalized_key])
        except KeyError as exc:
            raise ValueError("package_contract_not_found") from exc

    def validate(self, key: PackageContractKey, manifest: dict[str, Any]) -> None:
        validator = Draft202012Validator(self.schema_for(key))
        try:
            validator.validate(manifest)
        except ValidationError as exc:
            raise ValueError("package_contract_invalid") from exc

    def load_directory(self, directory: str | Path) -> list[PackageContractKey]:
        """Load operator-owned declarative contracts in deterministic order."""
        root = Path(directory)
        if not root.exists():
            return []
        if not root.is_dir():
            raise ValueError("package_contract_directory_invalid")
        loaded: list[PackageContractKey] = []
        for path in sorted(root.iterdir(), key=lambda item: item.name):
            if path.suffix != ".json" or not path.is_file():
                continue
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise ValueError("package_contract_file_invalid") from exc
            if not isinstance(payload, dict) or set(payload) != {
                "package_type",
                "execution",
                "schema",
            }:
                raise ValueError("package_contract_file_invalid")
            execution = payload.get("execution")
            schema = payload.get("schema")
            if (
                not isinstance(execution, dict)
                or set(execution) != {"kind", "version"}
                or not isinstance(schema, dict)
            ):
                raise ValueError("package_contract_file_invalid")
            key = (
                str(payload.get("package_type") or ""),
                str(execution.get("kind") or ""),
                str(execution.get("version") or ""),
            )
            self.register(key, schema)
            loaded.append(key)
        return loaded


package_contract_registry = PackageContractRegistry()
package_contract_registry.register(("function", "process:json_stdio", "1"), PROCESS_JSON_STDIO_V1_SCHEMA)
package_contract_registry.register(DRIVER_ORCHESTRATOR_KEY, PYTHON_ORCHESTRATOR_V1_SCHEMA)
package_contract_registry.register(("service", "container:http", "1"), CONTAINER_HTTP_V1_SCHEMA)


def load_operator_package_contracts(directory: str | Path) -> list[PackageContractKey]:
    """Load operator-owned schemas from an explicit composition-root setting."""
    return package_contract_registry.load_directory(directory)


__all__ = [
    "PackageContractKey",
    "DRIVER_ORCHESTRATOR_KEY",
    "PackageContractRegistry",
    "PROCESS_JSON_STDIO_V1_SCHEMA",
    "PYTHON_ORCHESTRATOR_V1_SCHEMA",
    "CONTAINER_HTTP_V1_SCHEMA",
    "package_contract_registry",
    "load_operator_package_contracts",
]
