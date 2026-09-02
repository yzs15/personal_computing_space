import os

import pytest

from loom_v2.contracts.types import CapabilityPackageVersion, CapabilityProvisionCommand, ComputeBinding, ResourceRef
from loom_v2.content_store import ContentStore, canonical_json_bytes
from loom_v2.slave.service import SlaveService
from loom_v2.driver.worker import WorkerSession
from loom_v2.slave.app import create_app
import httpx


def _store() -> ContentStore:
    return ContentStore(
        endpoint_url=os.environ["LOOM_S3_ENDPOINT_URL"],
        bucket=os.environ["LOOM_S3_BUCKET"],
        access_key=os.environ["LOOM_S3_ACCESS_KEY"],
        secret_key=os.environ["LOOM_S3_SECRET_KEY"],
    )


@pytest.mark.asyncio
async def test_subprocess_package_is_provisioned_and_executed():
    store = _store()
    code = b'import sys,json\nd=json.load(sys.stdin)\nprint(json.dumps({"value": d["x"] * 2}))'
    program_ref = await store.put(code, media_type="text/x-python")
    contract_ref = await store.put(
        canonical_json_bytes({"schema_version": "io.v1", "input_schema_ref": None, "output_schema_ref": None, "success_semantics": None, "success_validator_ref": None}),
        media_type="application/vnd.loom.io-contract+json",
    )
    package = CapabilityPackageVersion(
        package_id="pkg",
        package_version="v1",
        package_closure_version_ref="closure-pkg-v1",
        source_run_ref="run-1",
        source_closure_version_ref="committed-1",
        operation_descriptor_ref=ResourceRef(resource_id="loom://double"),
        operation_descriptor_digest="descriptor",
        program_content_ref=program_ref,
        program_digest=program_ref.version_or_digest,
        io_contract_ref=contract_ref,
    )
    service = SlaveService("slave-a", content_store=store)
    binding = ComputeBinding(
        binding_id="binding",
        hole_id="h",
        capability_descriptor_ref=ResourceRef(resource_id="executor://subprocess_json_v1/1"),
        capability_package_ref=ResourceRef(resource_id="capability-package://pkg/v1"),
        target_resource_ref=ResourceRef(resource_id="slave-a"),
        realization_digest=package.program_digest,
    )
    report = await service.provision(CapabilityProvisionCommand(command_id="cmd", package_version_ref="pkg:v1", package_digest=package.package_digest, target_slave="slave-a"), package)
    assert report.activation_state == "ready"
    result = await service.run("attempt", "double", {"x": 3}, binding=binding)
    assert result.value == {"value": 6}


@pytest.mark.asyncio
async def test_same_package_version_keeps_distinct_digests_and_runs_requested_package():
    store = _store()
    contract_ref = await store.put(
        canonical_json_bytes({"schema_version": "io.v1", "input_schema_ref": None, "output_schema_ref": None, "success_semantics": None, "success_validator_ref": None}),
        media_type="application/vnd.loom.io-contract+json",
    )
    first_program = await store.put(b'import sys,json; d=json.load(sys.stdin); print(json.dumps({"value": d["x"] * 2}))', media_type="text/x-python")
    second_program = await store.put(b'import sys,json; d=json.load(sys.stdin); print(json.dumps({"value": d["x"] * 3}))', media_type="text/x-python")

    def package(source_run_ref: str, program_ref):
        return CapabilityPackageVersion(
            package_id="same-package",
            package_version="v1",
            package_closure_version_ref="closure",
            source_run_ref=source_run_ref,
            source_closure_version_ref="version",
            operation_descriptor_ref=ResourceRef(resource_id="loom://triple"),
            operation_descriptor_digest="descriptor",
            program_content_ref=program_ref,
            program_digest=program_ref.version_or_digest,
            io_contract_ref=contract_ref,
        )

    first = package("run-one", first_program)
    second = package("run-two", second_program)
    assert first.package_digest != second.package_digest
    service = SlaveService("slave-a", content_store=store)
    first_command = CapabilityProvisionCommand(
        command_id="command-one",
        package_version_ref=first.version_ref,
        package_digest=first.package_digest,
        target_slave="slave-a",
    )
    second_command = CapabilityProvisionCommand(
        command_id="command-two",
        package_version_ref=second.version_ref,
        package_digest=second.package_digest,
        target_slave="slave-a",
    )
    first_report = await service.provision(first_command, first)
    second_report = await service.provision(second_command, second)
    assert first_report.package_digest == first.package_digest
    assert second_report.package_digest == second.package_digest

    binding = ComputeBinding(
        binding_id="binding-two",
        hole_id="h",
        capability_descriptor_ref=ResourceRef(resource_id="executor://subprocess_json_v1/1"),
        capability_package_ref=ResourceRef(resource_id=second.version_ref, version_or_digest=second.package_digest),
        target_resource_ref=ResourceRef(resource_id="slave-a"),
        realization_digest=second.program_digest,
    )
    result = await service.run("attempt-two", "triple", {"x": 3}, binding=binding)
    assert result.value == {"value": 9}


@pytest.mark.asyncio
async def test_worker_session_provision_uses_content_ref_and_reports_health():
    store = _store()
    program_ref = await store.put(b'import sys,json; print(json.dumps({"ok": True}))', media_type="text/x-python")
    contract_ref = await store.put(
        canonical_json_bytes({"schema_version": "io.v1", "input_schema_ref": None, "output_schema_ref": None, "success_semantics": None, "success_validator_ref": None}),
        media_type="application/vnd.loom.io-contract+json",
    )
    package = CapabilityPackageVersion(
        package_id="pkg-http", package_version="v1", package_closure_version_ref="closure",
        source_run_ref="run", source_closure_version_ref="version",
        operation_descriptor_ref=ResourceRef(resource_id="loom://check"), operation_descriptor_digest="d",
        program_content_ref=program_ref, program_digest=program_ref.version_or_digest,
        io_contract_ref=contract_ref,
    )
    app = create_app("slave-a")
    session = WorkerSession("slave-a", "http://slave-a", transport=httpx.ASGITransport(app=app))
    report = await session.provision(command=CapabilityProvisionCommand(command_id="cmd", package_version_ref="pkg-http:v1", package_digest=package.package_digest, target_slave="slave-a"), package=package)
    assert report.activation_state == "ready"
