import asyncio
import os

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from loom_v2.content_store import ContentStore
from loom_v2.contracts.types import CapabilityHealthReport, CapabilityPackageVersion, CapabilityProvisionCommand, ResourceRef, TaskClosure
from loom_v2.observer.worker import WorkerSession
from loom_v2.slave.app import create_app


def test_slave_capabilities_endpoint_reports_worker_contract():
    app = create_app("slave-a")
    with TestClient(app) as client:
        response = client.get("/worker/v1/capabilities")

    assert response.status_code == 200
    payload = response.json()
    assert payload["slave_id"] == "slave-a"
    assert "sort" in payload["operations"]


def test_slave_dispatch_endpoint_returns_ack_and_terminal_report():
    app = create_app("slave-a")
    closure = TaskClosure(program={"operation_ref": "loom://sort"})
    with TestClient(app) as client:
        response = client.post(
            "/worker/v1/dispatch",
            json={
                "attempt_id": "attempt-worker-api",
                "execution_id": "execution-worker-api",
                "execution_epoch": 1,
                "workspace_id": "workspace-default",
                "operation": "sort",
                "payload": {"items": [3, 1, 2]},
                "closure": closure.model_dump(mode="json"),
            },
        )
    assert response.status_code == 200
    payload = response.json()
    assert payload["accepted"] is True
    assert payload["terminal_report"]["result"]["value"] == {"items": [1, 2, 3]}


@pytest.mark.asyncio
async def test_worker_session_dispatches_over_http_envelope():
    app = create_app("slave-a")
    transport = httpx.ASGITransport(app=app)
    session = WorkerSession("slave-a", "http://slave-a", transport=transport)
    result = await session.dispatch(
        attempt_id="attempt-worker-session",
        execution_id="execution-worker-session",
        execution_epoch=1,
        workspace_id="workspace-default",
        operation="sort",
        payload={"items": [5, 2, 4]},
        closure=TaskClosure(program={"operation_ref": "loom://sort"}),
        binding=None,
    )
    assert result.value == {"items": [2, 4, 5]}


@pytest.mark.asyncio
async def test_worker_session_timeout_is_independent_from_coding_agent_deadline():
    app = FastAPI()

    @app.post("/worker/v1/dispatch")
    async def delayed_dispatch() -> dict[str, object]:
        await asyncio.sleep(0.10)
        return {"accepted": True, "terminal_report": {"state": "completed", "result": {}}}

    session = WorkerSession(
        "slave-a",
        "http://slave-a",
        operation_timeout=0.01,
        transport=httpx.ASGITransport(app=app),
    )
    assert session.operation_timeout == 0.01

    with pytest.raises(RuntimeError, match="worker_operation_timeout"):
        await session.dispatch(
            attempt_id="attempt-worker-timeout",
            execution_id="execution-worker-timeout",
            execution_epoch=1,
            workspace_id="workspace-default",
            operation="sort",
            payload={"items": [2, 1]},
            closure=TaskClosure(program={"operation_ref": "loom://sort"}),
            binding=None,
        )


@pytest.mark.asyncio
async def test_worker_session_provision_sends_only_content_references():
    app = FastAPI()
    captured: dict[str, object] = {}

    @app.post("/worker/v1/provision")
    async def provision(payload: dict[str, object]) -> dict[str, object]:
        captured.update(payload)
        package = payload["package"]
        assert isinstance(package, dict)
        return {"accepted": True, "health_report": CapabilityHealthReport(
            report_id="health-test",
            package_version_ref=str(package["version_ref"] if "version_ref" in package else package["package_id"] + ":" + package["package_version"]),
            package_digest=str(package["package_digest"]),
            target_slave="slave-a",
            activation_state="ready",
        ).model_dump(mode="json")}

    store = ContentStore(
        endpoint_url=os.environ["LOOM_S3_ENDPOINT_URL"],
        bucket=os.environ["LOOM_S3_BUCKET"],
        access_key=os.environ["LOOM_S3_ACCESS_KEY"],
        secret_key=os.environ["LOOM_S3_SECRET_KEY"],
    )
    program_ref = await store.put(b"print(1)", media_type="text/x-python")
    package = CapabilityPackageVersion(
        package_id="pkg-reference-only",
        package_version="v1",
        package_closure_version_ref="closure",
        source_run_ref="run",
        source_closure_version_ref="version",
        operation_descriptor_ref=ResourceRef(resource_id="loom://check"),
        operation_descriptor_digest="descriptor",
        program_content_ref=program_ref,
        program_digest=program_ref.version_or_digest,
    )
    session = WorkerSession("slave-a", "http://slave-a", transport=httpx.ASGITransport(app=app))
    await session.provision(
        command=CapabilityProvisionCommand(
            command_id="command-reference-only",
            package_version_ref=package.version_ref,
            package_digest=package.package_digest,
            target_slave="slave-a",
            program_content_ref=program_ref,
        ),
        package=package,
    )
    assert "program_bytes_b64" not in captured
