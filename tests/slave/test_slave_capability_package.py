import os

import pytest

from loom_v2.contracts.types import CapabilityPackageVersion, CapabilityProvisionCommand, ComputeBinding, ResourceRef
from loom_v2.content_store import ContentStore, canonical_json_bytes
from loom_v2.digest import digest_json
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


def _package(package_id: str, source_run_ref: str, operation: str, program_ref: ResourceRef, contract_ref: ResourceRef) -> CapabilityPackageVersion:
    descriptor_ref = ResourceRef(
        resource_id=f"loom://{operation}",
        version_or_digest=digest_json({"operation": operation}, domain="loom/operation-descriptor/v1"),
        identity_criterion="descriptor_digest",
    )
    return CapabilityPackageVersion(
        package_id=package_id,
        package_version="v1",
        package_closure_version_ref="closure",
        source_run_ref=source_run_ref,
        source_closure_version_ref="version",
        capability_exports=[{
            "capability_descriptor_ref": descriptor_ref,
            "io_contract_ref": contract_ref,
            "effect_class": "Sandboxed",
            "permissions": [],
            "replay_safety": "DeclaredByPackage",
            "runtime_binding": {},
        }],
        body={"program_content_ref": program_ref.model_dump(mode="json")},
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
    package = _package("pkg", "run-1", "double", program_ref, contract_ref)
    service = SlaveService("slave-a", content_store=store)
    binding = ComputeBinding(
        binding_id="binding",
        hole_id="h",
        capability_descriptor_ref=package.capability_exports[0].capability_descriptor_ref,
        capability_package_ref=ResourceRef(resource_id="capability-package://pkg/v1"),
        target_resource_ref=ResourceRef(resource_id="slave-a"),

    )
    report = await service.provision(CapabilityProvisionCommand(command_id="cmd", package_version_ref=package.version_ref, package_digest=package.package_digest, target_slave="slave-a", idempotency_key="provision-1", activation_revision=1), package)
    assert report.activation_state == "ready"
    result = await service.run("attempt", "double", {"x": 3}, binding=binding)
    assert result.value == {"value": 6}


@pytest.mark.asyncio
async def test_ready_activation_accepts_same_revision_from_a_later_run():
    """Slave activation state outlives a Run, while revisions are Run-local.

    A later execution of the same immutable package starts its Observer-side
    activation revision at one again.  Once the Slave already has the exact
    digest in ``ready`` state, that request is a safe lifecycle replay even
    though its idempotency key belongs to the later Run.
    """
    store = _store()
    program_ref = await store.put(
        b'import sys,json; print(json.dumps({"ok": True}))',
        media_type="text/x-python",
    )
    contract_ref = await store.put(
        canonical_json_bytes(
            {
                "schema_version": "io.v1",
                "input_schema_ref": None,
                "output_schema_ref": None,
                "success_semantics": None,
                "success_validator_ref": None,
            }
        ),
        media_type="application/vnd.loom.io-contract+json",
    )
    package = _package("reused", "run-one", "check", program_ref, contract_ref)
    service = SlaveService("slave-a", content_store=store)
    first = CapabilityProvisionCommand(
        command_id="command-one",
        package_version_ref=package.version_ref,
        package_digest=package.package_digest,
        target_slave="slave-a",
        idempotency_key="run-one-provision",
        activation_revision=1,
    )
    second = first.model_copy(
        update={
            "command_id": "command-two",
            "idempotency_key": "run-two-provision",
        }
    )

    first_report = await service.provision(first, package)
    second_report = await service.provision(second, package)

    assert first_report.activation_state == "ready"
    assert second_report.activation_state == "ready"
    assert second_report.activation_revision == 1


@pytest.mark.asyncio
async def test_same_package_version_rejects_distinct_digest_identity():
    store = _store()
    contract_ref = await store.put(
        canonical_json_bytes({"schema_version": "io.v1", "input_schema_ref": None, "output_schema_ref": None, "success_semantics": None, "success_validator_ref": None}),
        media_type="application/vnd.loom.io-contract+json",
    )
    first_program = await store.put(b'import sys,json; d=json.load(sys.stdin); print(json.dumps({"value": d["x"] * 2}))', media_type="text/x-python")
    second_program = await store.put(b'import sys,json; d=json.load(sys.stdin); print(json.dumps({"value": d["x"] * 3}))', media_type="text/x-python")

    def package(source_run_ref: str, program_ref):
        return _package("same-package", source_run_ref, "triple", program_ref, contract_ref)

    first = package("run-one", first_program)
    second = package("run-two", second_program)
    assert first.package_digest != second.package_digest
    service = SlaveService("slave-a", content_store=store)
    first_command = CapabilityProvisionCommand(
        command_id="command-one",
        package_version_ref=first.version_ref,
        package_digest=first.package_digest,
        target_slave="slave-a",
        idempotency_key="provision-first",
        activation_revision=1,
    )
    second_command = CapabilityProvisionCommand(
        command_id="command-two",
        package_version_ref=second.version_ref,
        package_digest=second.package_digest,
        target_slave="slave-a",
        idempotency_key="provision-second",
        activation_revision=1,
    )
    first_report = await service.provision(first_command, first)
    assert first_report.package_digest == first.package_digest
    with pytest.raises(RuntimeError, match="capability_package_identity_conflict"):
        await service.provision(second_command, second)


@pytest.mark.asyncio
async def test_worker_session_provision_uses_content_ref_and_reports_health():
    store = _store()
    program_ref = await store.put(b'import sys,json; print(json.dumps({"ok": True}))', media_type="text/x-python")
    contract_ref = await store.put(
        canonical_json_bytes({"schema_version": "io.v1", "input_schema_ref": None, "output_schema_ref": None, "success_semantics": None, "success_validator_ref": None}),
        media_type="application/vnd.loom.io-contract+json",
    )
    package = _package("pkg-http", "run", "check", program_ref, contract_ref)
    app = create_app("slave-a")
    session = WorkerSession("slave-a", "http://slave-a", transport=httpx.ASGITransport(app=app))
    report = await session.provision(command=CapabilityProvisionCommand(command_id="cmd", package_version_ref=package.version_ref, package_digest=package.package_digest, target_slave="slave-a", idempotency_key="provision-1", activation_revision=1), package=package)
    assert report.activation_state == "ready"
