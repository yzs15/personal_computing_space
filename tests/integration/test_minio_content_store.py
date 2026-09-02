from __future__ import annotations

import os

import httpx
import pytest

from loom_v2.content_store import ContentStore
from loom_v2.contracts.types import CapabilityPackageVersion, CapabilityProvisionCommand, ComputeBinding, ResourceRef
from loom_v2.slave.app import create_app
from loom_v2.observer.repository import ObserverRepository
from loom_v2.driver.worker import WorkerSession


def _store() -> ContentStore:
    return ContentStore(
        endpoint_url=os.environ["LOOM_S3_ENDPOINT_URL"],
        bucket=os.environ["LOOM_S3_BUCKET"],
        access_key=os.environ["LOOM_S3_ACCESS_KEY"],
        secret_key=os.environ["LOOM_S3_SECRET_KEY"],
    )


@pytest.mark.asyncio
async def test_observer_and_slave_share_immutable_content_store():
    observer_store = _store()
    repo = ObserverRepository(content_store=observer_store)
    run = await repo.open_run("run-minio-e2e", "conversation-minio-e2e", "double")
    code = 'import sys,json; print(json.dumps({"value": json.load(sys.stdin)["x"] * 2}))'
    program_ref = await observer_store.put(code.encode(), media_type="text/x-python")
    io_contract_ref = await observer_store.put(
        b'{"schema_version":"io.v1","input_schema_ref":null,"output_schema_ref":null,"success_semantics":null,"success_validator_ref":null}',
        media_type="application/vnd.loom.io-contract+json",
    )
    patch = await repo.apply_patch(
        run.run_id,
        run.draft_version,
        run.draft_digest,
        "materialize-minio-e2e",
            [
                {"kind": "set_program_ref", "value": "loom://double"},
                {"kind": "set_io_contract_ref", "value": io_contract_ref.model_dump(mode="json")},
                {"kind": "add_typed_hole", "value": {"hole_id": "h_double"}},
            {"kind": "materialize_capability_package_candidate", "value": {"package_id": "pkg-minio-e2e", "program_content_ref": program_ref.model_dump(mode="json"), "io_contract_ref": io_contract_ref.model_dump(mode="json"), "operation_descriptor_ref": "loom://double"}},
        ],
    )
    package = (await repo.get_run(run.run_id)).capability_packages[0]
    binding = ComputeBinding(
        binding_id="binding-minio-e2e",
        hole_id="h_double",
        capability_descriptor_ref=ResourceRef(resource_id="executor://subprocess_json_v1/1"),
        capability_package_ref=ResourceRef(resource_id=package.version_ref),
        target_resource_ref=ResourceRef(resource_id="slave-a"),
        realization_digest=package.program_digest,
    )
    bound = await repo.apply_patch(
        run.run_id,
        patch.draft_version,
        patch.draft_digest,
        "bind-minio-e2e",
        [{"kind": "bind_compute_hole", "value": binding.model_dump(mode="json")}],
    )
    assert bound.readiness["ready"] is True
    committed = await repo.commit(run.run_id, bound.draft_version, bound.draft_digest)
    started = await repo.start(run.run_id, committed.version_id)

    slave_app = create_app("slave-a")
    worker = WorkerSession("slave-a", "http://slave-a", transport=httpx.ASGITransport(app=slave_app))
    command = CapabilityProvisionCommand(
        command_id="command-minio-e2e",
        package_version_ref=package.version_ref,
        package_digest=package.package_digest,
        target_slave="slave-a",
        program_content_ref=package.program_content_ref,
        compute_binding=binding,
    )
    report = await worker.provision(command=command, package=package)
    assert report.activation_state == "ready"
    result = await worker.dispatch(
        attempt_id=started["execution_id"],
        execution_id=started["execution_id"],
        execution_epoch=started["execution_epoch"],
        workspace_id="workspace-default",
        operation="double",
        payload={"x": 7},
        closure=committed.snapshot,
        binding=binding,
    )
    assert result.value == {"value": 14}
    slave_store = _store()
    assert await slave_store.get(package.program_content_ref, expected_digest=package.program_digest) == code.encode()
    assert await observer_store.get(package.program_content_ref) == code.encode()
