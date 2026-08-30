import os

import pytest

from loom_v2.contracts.types import CapabilityPackageVersion, CapabilityProvisionCommand, ComputeBinding, ResourceRef
from loom_v2.content_store import ContentStore, canonical_json_bytes
from loom_v2.slave.service import SlaveService
from loom_v2.observer.worker import WorkerSession
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
