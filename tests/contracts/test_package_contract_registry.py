from copy import deepcopy
import json
from pathlib import Path

import pytest

from loom_v2.contracts.package_contracts import (
    PROCESS_JSON_STDIO_V1_SCHEMA,
    PackageContractRegistry,
    package_contract_registry,
)
from loom_v2.contracts.types import CapabilityPackageVersion, ResourceRef


def _ref(resource_id: str, digest: str, criterion: str) -> dict[str, object]:
    return ResourceRef(
        resource_id=resource_id,
        version_or_digest=digest if criterion == "descriptor_digest" else None,
        identity_criterion=criterion,
    ).model_dump(mode="json")


def _export(*, descriptor_id: str = "loom://test/echo", digest: str = "a" * 64) -> dict[str, object]:
    return {
        "capability_descriptor_ref": _ref(
            descriptor_id, digest, "descriptor_digest"
        ),
        "io_contract_ref": _ref(
            "content://sha256/" + "b" * 64, "b" * 64, "content_digest"
        ),
        "effect_class": "Pure",
        "permissions": [],
        "replay_safety": "Idempotent",
        "runtime_binding": {},
    }


def _custom_schema(package_type: str, execution_kind: str) -> dict[str, object]:
    schema = deepcopy(PROCESS_JSON_STDIO_V1_SCHEMA)
    schema["properties"]["package_type"] = {"const": package_type}
    schema["properties"]["execution"]["properties"]["kind"] = {
        "const": execution_kind
    }
    schema["properties"]["body"] = {
        "type": "object",
        "required": ["message"],
        "properties": {"message": {"type": "string"}},
        "additionalProperties": False,
    }
    return schema


def _package_payload(
    *, package_type: str, execution_kind: str, body: dict[str, object]
) -> dict[str, object]:
    return {
        "package_type": package_type,
        "package_id": "pkg-custom",
        "package_version": "v1",
        "package_closure_version_ref": "closure-1",
        "source_run_ref": "run-1",
        "source_closure_version_ref": "version-1",
        "execution": {"kind": execution_kind, "version": "1"},
        "capability_exports": [_export()],
        "body": body,
    }


def test_unregistered_package_contract_is_rejected() -> None:
    with pytest.raises(ValueError, match="package_contract_not_found"):
        CapabilityPackageVersion.model_validate(
            _package_payload(
                package_type="test-unregistered",
                execution_kind="test:unregistered",
                body={"message": "hello"},
            )
        )


def test_package_contract_registry_is_append_only() -> None:
    registry = PackageContractRegistry()
    key = ("test-conflict", "test:echo", "1")
    schema = _custom_schema(key[0], key[1])

    registry.register(key, schema)
    registry.register(key, deepcopy(schema))

    changed = deepcopy(schema)
    changed["properties"]["body"]["required"] = []
    with pytest.raises(ValueError, match="package_contract_conflict"):
        registry.register(key, changed)


def test_package_contract_schema_must_be_bound_to_its_registry_key() -> None:
    registry = PackageContractRegistry()
    schema = _custom_schema("different-type", "test:different")

    with pytest.raises(ValueError, match="package_contract_schema_invalid"):
        registry.register(("test-expected", "test:expected", "1"), schema)


def test_custom_registered_contract_uses_generic_package_model() -> None:
    key = ("test-generic", "test:echo", "1")
    package_contract_registry.register(key, _custom_schema(key[0], key[1]))

    package = CapabilityPackageVersion.model_validate(
        _package_payload(
            package_type=key[0], execution_kind=key[1], body={"message": "hello"}
        )
    )

    assert package.body == {"message": "hello"}
    assert package.package_digest


def test_custom_registered_contract_keeps_body_strict() -> None:
    key = ("test-generic-strict", "test:strict", "1")
    package_contract_registry.register(key, _custom_schema(key[0], key[1]))

    with pytest.raises(ValueError, match="package_contract_invalid"):
        CapabilityPackageVersion.model_validate(
            _package_payload(
                package_type=key[0],
                execution_kind=key[1],
                body={"message": "hello", "unknown": True},
            )
        )


def test_custom_body_resource_id_property_is_not_mistaken_for_resource_ref() -> None:
    key = ("test-resource-record", "test:resource-record", "1")
    schema = _custom_schema(key[0], key[1])
    schema["properties"]["body"] = {
        "type": "object",
        "required": ["record"],
        "properties": {
            "record": {
                "type": "object",
                "required": ["resource_id", "policy"],
                "properties": {
                    "resource_id": {"type": "string"},
                    "policy": {"type": "string"},
                },
                "additionalProperties": False,
            }
        },
        "additionalProperties": False,
    }
    package_contract_registry.register(key, schema)

    left = CapabilityPackageVersion.model_validate(
        _package_payload(
            package_type=key[0],
            execution_kind=key[1],
            body={"record": {"resource_id": "resource-1", "policy": "left"}},
        )
    )
    right = CapabilityPackageVersion.model_validate(
        _package_payload(
            package_type=key[0],
            execution_kind=key[1],
            body={"record": {"resource_id": "resource-1", "policy": "right"}},
        )
    )

    assert left.package_digest != right.package_digest


def test_operator_contract_directory_loads_declarative_schema(tmp_path: Path) -> None:
    key = ("test-file-contract", "test:file", "1")
    contract_file = tmp_path / "test-file-contract.json"
    contract_file.write_text(
        json.dumps(
            {
                "package_type": key[0],
                "execution": {"kind": key[1], "version": key[2]},
                "schema": _custom_schema(key[0], key[1]),
            }
        ),
        encoding="utf-8",
    )
    registry = PackageContractRegistry()

    assert registry.load_directory(tmp_path) == [key]
    registry.validate(
        key,
        {
            "package_type": key[0],
            "execution": {"kind": key[1], "version": key[2]},
            "capability_exports": [_export()],
            "body": {"message": "loaded"},
        },
    )
