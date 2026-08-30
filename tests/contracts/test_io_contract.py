import pytest
from pydantic import ValidationError

from loom_v2.contracts.types import (
    CapabilityPackageVersion,
    IoContract,
    NodeInputBinding,
    ResourceRef,
)


def test_io_contract_round_trips_nullable_schema_refs() -> None:
    contract = IoContract(
        input_schema_ref=ResourceRef(
            resource_id="content://sha256/" + "1" * 64,
            version_or_digest="1" * 64,
        ),
        output_schema_ref=None,
        success_semantics={"criterion": "non_empty"},
        success_validator_ref=None,
    )

    dumped = contract.model_dump(mode="json")

    assert dumped["input_schema_ref"]["version_or_digest"] == "1" * 64
    assert dumped["output_schema_ref"] is None
    assert dumped["success_semantics"] == {"criterion": "non_empty"}


def test_io_contract_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        IoContract(unknown_field=True)  # type: ignore[call-arg]


def test_package_and_node_binding_carry_semantic_refs() -> None:
    contract_ref = ResourceRef(
        resource_id="content://sha256/" + "2" * 64,
        version_or_digest="2" * 64,
    )
    input_ref = ResourceRef(
        resource_id="content://sha256/" + "3" * 64,
        version_or_digest="3" * 64,
    )

    package_fields = CapabilityPackageVersion.model_fields
    assert "io_contract_ref" in package_fields

    binding = NodeInputBinding(node_id="node-1", input_ref=input_ref)
    assert binding.model_dump(mode="json") == {
        "node_id": "node-1",
        "input_ref": input_ref.model_dump(mode="json"),
        "provenance": [],
    }
    assert contract_ref.resource_id.startswith("content://sha256/")
