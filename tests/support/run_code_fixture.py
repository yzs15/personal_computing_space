from __future__ import annotations

import hashlib
from dataclasses import dataclass

from loom_v2.content_store import canonical_json_bytes
from loom_v2.contracts.types import (
    CapabilityPackageVersion,
    CapabilityProvisionCommand,
    ComputeBinding,
    ComputeSpec,
    IoContract,
    NodeInputBinding,
    ResourceRef,
    TaskClosure,
    TypedHole,
)
from loom_v2.slave.service import SlaveService
from loom_v2.slave.executor import default_registry


DEFAULT_PROGRAM = b"""import json\nimport sys\n\npayload = json.load(sys.stdin)\nvalue = payload.get(\"value\", 0)\nprint(json.dumps({\"value\": value * 2}))\n"""


@dataclass(frozen=True)
class RunCodeFixture:
    package: CapabilityPackageVersion
    binding: ComputeBinding
    operation: str
    program_ref: ResourceRef

    @property
    def operation_ref(self) -> str:
        return f"loom://{self.operation}"

    def closure(
        self,
        *,
        io_contract_ref: ResourceRef | None = None,
        input_refs: list[ResourceRef] | None = None,
        closure_id: str = "closure-run-code",
    ) -> TaskClosure:
        contract_ref = io_contract_ref or self.package.io_contract_ref
        assert contract_ref is not None
        hole = TypedHole(
            hole_id=self.binding.hole_id,
            status="bound",
            binding_ref=self.binding.binding_id,
        )
        return TaskClosure(
            closure_id=closure_id,
            program={
                "operation_ref": self.operation_ref,
                "io_contract_ref": contract_ref.model_dump(mode="json"),
            },
            compute=ComputeSpec(operation_ref=self.operation_ref, typed_holes=[hole]),
            compute_bindings=[self.binding],
            node_input_bindings=[
                NodeInputBinding(node_id=self.operation, input_ref=ref)
                for ref in (input_refs or [])
            ],
        )


async def make_run_code_fixture(
    service: SlaveService,
    *,
    operation: str = "test_double",
    package_id: str | None = None,
    program: bytes = DEFAULT_PROGRAM,
    io_contract_ref: ResourceRef | None = None,
) -> RunCodeFixture:
    """Create a real content-addressed subprocess package for tests."""

    program_ref = await service.content_store.put(program, media_type="text/x-python")
    if io_contract_ref is None:
        contract_ref = await service.content_store.put(
            canonical_json_bytes(IoContract().model_dump(mode="json")),
            media_type="application/vnd.loom.io-contract+json",
        )
    else:
        contract_ref = io_contract_ref
    operation_ref = ResourceRef(
        resource_id=f"loom://{operation}",
        identity_criterion="descriptor_digest",
    )
    package = CapabilityPackageVersion(
        package_id=package_id or f"package-{operation}",
        package_version="v1",
        package_closure_version_ref=f"package-closure-{operation}",
        source_run_ref="test-run",
        source_closure_version_ref="test-closure-v1",
        operation_descriptor_ref=operation_ref,
        operation_descriptor_digest=hashlib.sha256(operation_ref.resource_id.encode()).hexdigest(),
        program_content_ref=program_ref,
        program_digest=program_ref.version_or_digest or "",
        io_contract_ref=contract_ref,
        executor_kind="subprocess_json_v1",
        executor_operation="run_code",
    )
    descriptor_digest = default_registry.get("subprocess_json_v1").descriptor.digest
    binding = ComputeBinding(
        binding_id=f"binding-{operation}",
        hole_id=f"hole-{operation}",
        capability_descriptor_ref=ResourceRef(
            resource_id="executor://subprocess_json_v1/1",
        ),
        capability_package_ref=ResourceRef(
            resource_id=package.version_ref,
            version_or_digest=package.package_digest,
        ),
        target_resource_ref=ResourceRef(resource_id=service.slave_id),
        realization_digest=package.program_digest,
        executor_descriptor_digest=descriptor_digest,
    )
    return RunCodeFixture(
        package=package,
        binding=binding,
        operation=operation,
        program_ref=program_ref,
    )


async def provision_run_code_fixture(
    service: SlaveService,
    fixture: RunCodeFixture,
) -> None:
    await service.provision(
        CapabilityProvisionCommand(
            command_id=f"provision-{fixture.package.package_id}",
            package_version_ref=fixture.package.version_ref,
            package_digest=fixture.package.package_digest,
            target_slave=service.slave_id,
            program_content_ref=fixture.program_ref,
        ),
        fixture.package,
    )
