import asyncio
import os

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from loom_v2.content_store import ContentStore, canonical_json_bytes
from loom_v2.contracts.types import CapabilityHealthReport, CapabilityPackageVersion, CapabilityProvisionCommand, NodeInputBinding, ResourceRef, TaskClosure
from loom_v2.driver.worker import WorkerSession, WorkerUnavailableError
from loom_v2.slave.app import create_app
from tests.support.run_code_fixture import make_run_code_fixture, provision_run_code_fixture


def dispatch_arguments() -> dict[str, object]:
    return {
        "attempt_id": "attempt-worker-unavailable",
        "execution_id": "execution-worker-unavailable",
        "execution_epoch": 1,
        "workspace_id": "workspace-default",
        "operation": "run_code",
        "payload": {},
        "closure": TaskClosure.minimal(),
        "binding": None,
    }


def application_failure_transport() -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(409, json={"detail": "capability_exec_error"}, request=request)

    return httpx.MockTransport(handler)


@pytest.mark.asyncio
async def test_worker_session_classifies_transport_failure_as_unavailable():
    session = WorkerSession(
        "slave-a",
        "http://slave-a",
        transport=httpx.MockTransport(
            lambda request: (_ for _ in ()).throw(httpx.ConnectError("down", request=request))
        ),
    )

    with pytest.raises(WorkerUnavailableError):
        await session.dispatch(**dispatch_arguments())


@pytest.mark.asyncio
async def test_worker_session_does_not_classify_application_failure_as_unavailable():
    session = WorkerSession("slave-a", "http://slave-a", transport=application_failure_transport())

    with pytest.raises(RuntimeError, match="capability_exec_error"):
        await session.dispatch(**dispatch_arguments())


def test_slave_capabilities_endpoint_reports_worker_contract():
    app = create_app("slave-a")
    with TestClient(app) as client:
        response = client.get("/worker/v1/capabilities")

    assert response.status_code == 200
    payload = response.json()
    assert payload["slave_id"] == "slave-a"
    assert payload["operations"] == ["run_code"]
    assert [item["kind"] for item in payload["executor_descriptors"]] == ["process:json_stdio"]


@pytest.mark.asyncio
async def test_slave_dispatch_endpoint_returns_ack_and_terminal_report():
    app = create_app("slave-a")
    fixture = await make_run_code_fixture(app.state.service, operation="test_worker_api")
    await provision_run_code_fixture(app.state.service, fixture)
    closure = fixture.closure()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/worker/v1/dispatch",
            json={
                "attempt_id": "attempt-worker-api",
                "execution_id": "execution-worker-api",
                "execution_epoch": 1,
                "workspace_id": "workspace-default",
                "operation": fixture.operation,
                "payload": {"value": 3},
                "closure": closure.model_dump(mode="json"),
                "binding": fixture.binding.model_dump(mode="json"),
            },
        )
    assert response.status_code == 200
    payload = response.json()
    assert payload["accepted"] is True
    assert payload["execution_id"] == "execution-worker-api"
    assert payload["terminal_report"]["attempt_id"] == "attempt-worker-api"
    assert payload["terminal_report"]["execution_id"] == "execution-worker-api"
    assert payload["terminal_report"]["execution_epoch"] == 1
    assert payload["terminal_report"]["result"]["value"] == {"value": 6}


def test_slave_dispatch_requires_driver_epoch_when_internal_auth_is_enabled(monkeypatch):
    monkeypatch.setenv("LOOM_INTERNAL_API_SECRET", "secret")
    app = create_app("slave-a")
    with TestClient(app) as client:
        response = client.post(
            "/worker/v1/dispatch",
            headers={"X-Loom-Internal-Token": "secret"},
            json={
                "attempt_id": "attempt-auth-fencing",
                "execution_id": "execution-auth-fencing",
                "execution_epoch": 1,
                "workspace_id": "workspace-default",
                "operation": "run_code",
                "payload": {"value": 3},
            },
        )
    assert response.status_code == 422
    assert response.json()["detail"] == "driver_identity_required"


@pytest.mark.asyncio
async def test_worker_session_dispatches_over_http_envelope():
    app = create_app("slave-a")
    fixture = await make_run_code_fixture(app.state.service, operation="test_worker_session")
    await provision_run_code_fixture(app.state.service, fixture)
    transport = httpx.ASGITransport(app=app)
    session = WorkerSession("slave-a", "http://slave-a", transport=transport)
    result = await session.dispatch(
        attempt_id="attempt-worker-session",
        execution_id="execution-worker-session",
        execution_epoch=1,
        workspace_id="workspace-default",
        operation=fixture.operation,
        payload={"value": 5},
        closure=fixture.closure(),
        binding=fixture.binding,
    )
    assert result.value == {"value": 10}


@pytest.mark.asyncio
async def test_worker_session_preserves_slave_terminal_validation_state():
    app = create_app("slave-a")
    schema_ref = await app.state.service.content_store.put(
        canonical_json_bytes({"type": "object", "required": ["missing"]}),
        media_type="application/schema+json",
    )
    contract_ref = await app.state.service.content_store.put(
        canonical_json_bytes({
            "schema_version": "io.v1",
            "input_schema_ref": None,
            "output_schema_ref": schema_ref.model_dump(mode="json"),
            "success_semantics": None,
            "success_validator_ref": None,
        }),
        media_type="application/vnd.loom.io-contract+json",
    )
    fixture = await make_run_code_fixture(
        app.state.service,
        operation="test_worker_validation",
        io_contract_ref=contract_ref,
        program=b'import json,sys; d=json.load(sys.stdin); print(json.dumps({"text": d["text"]}))',
    )
    await provision_run_code_fixture(app.state.service, fixture)
    session = WorkerSession("slave-a", "http://slave-a", transport=httpx.ASGITransport(app=app))
    result = await session.dispatch(
        attempt_id="attempt-worker-validation",
        execution_id="execution-worker-validation",
        execution_epoch=1,
        workspace_id="workspace-default",
        operation=fixture.operation,
        payload={"text": "hello"},
        closure=fixture.closure(io_contract_ref=contract_ref),
        binding=fixture.binding,
    )
    assert result.terminal_state == "failed"
    assert result.terminal_error["code"] == "output_schema_mismatch"
    assert result.validation_evidence


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
            operation="run_code",
            payload={"value": 2},
            closure=TaskClosure(program={"operation_ref": "loom://run_code"}),
            binding=None,
        )


@pytest.mark.asyncio
async def test_worker_session_rejects_terminal_report_with_wrong_attempt_id():
    app = FastAPI()

    @app.post("/worker/v1/dispatch")
    async def mismatched_dispatch() -> dict[str, object]:
        return {
            "accepted": True,
            "attempt_id": "attempt-worker-fencing",
            "execution_id": "execution-worker-fencing",
            "execution_epoch": 1,
            "terminal_report": {
                "type": "terminal_report",
                "attempt_id": "attempt-other",
                "execution_id": "execution-worker-fencing",
                "execution_epoch": 1,
                "state": "completed",
                "result": {
                    "resource_ref": {"resource_id": "result-fencing"},
                    "value": {"ok": True},
                },
            },
        }

    session = WorkerSession("slave-a", "http://slave-a", transport=httpx.ASGITransport(app=app))
    with pytest.raises(RuntimeError, match="stale_attempt"):
        await session.dispatch(
            attempt_id="attempt-worker-fencing",
            execution_id="execution-worker-fencing",
            execution_epoch=1,
            workspace_id="workspace-default",
            operation="run_code",
            payload={"value": 2},
            closure=TaskClosure(program={"operation_ref": "loom://run_code"}),
            binding=None,
        )


@pytest.mark.asyncio
async def test_worker_session_rejects_terminal_report_with_wrong_execution_id():
    app = FastAPI()

    @app.post("/worker/v1/dispatch")
    async def mismatched_dispatch() -> dict[str, object]:
        return {
            "accepted": True,
            "attempt_id": "attempt-worker-fencing-exec",
            "execution_id": "execution-worker-fencing-exec",
            "execution_epoch": 1,
            "terminal_report": {
                "type": "terminal_report",
                "attempt_id": "attempt-worker-fencing-exec",
                "execution_id": "execution-other",
                "execution_epoch": 1,
                "state": "completed",
                "result": {
                    "resource_ref": {"resource_id": "result-fencing-exec"},
                    "value": {"ok": True},
                },
            },
        }

    session = WorkerSession("slave-a", "http://slave-a", transport=httpx.ASGITransport(app=app))
    with pytest.raises(RuntimeError, match="stale_execution_id"):
        await session.dispatch(
            attempt_id="attempt-worker-fencing-exec",
            execution_id="execution-worker-fencing-exec",
            execution_epoch=1,
            workspace_id="workspace-default",
            operation="run_code",
            payload={"value": 2},
            closure=TaskClosure(program={"operation_ref": "loom://run_code"}),
            binding=None,
        )


@pytest.mark.asyncio
async def test_worker_session_omits_mutable_payload_when_closure_has_input_binding():
    app = FastAPI()
    captured: dict[str, object] = {}

    @app.post("/worker/v1/dispatch")
    async def dispatch(payload: dict[str, object]) -> dict[str, object]:
        captured.update(payload)
        return {
            "accepted": True,
            "attempt_id": "attempt-input-ref",
            "execution_id": "execution-input-ref",
            "execution_epoch": 1,
            "terminal_report": {
                "type": "terminal_report",
                "attempt_id": "attempt-input-ref",
                "execution_id": "execution-input-ref",
                "execution_epoch": 1,
                "state": "completed",
                "result": {
                    "resource_ref": {"resource_id": "result-input-ref"},
                    "value": {"ok": True},
                },
            },
        }

    input_ref = ResourceRef(resource_id="content://sha256/" + "a" * 64)
    closure = TaskClosure(
        program={"operation_ref": "loom://test_input_ref"},
        node_input_bindings=[NodeInputBinding(node_id="test_input_ref", input_ref=input_ref)],
    )
    session = WorkerSession("slave-a", "http://slave-a", transport=httpx.ASGITransport(app=app))
    await session.dispatch(
        attempt_id="attempt-input-ref",
        execution_id="execution-input-ref",
        execution_epoch=1,
        workspace_id="workspace-default",
        operation="test_input_ref",
        payload={"value": "untrusted"},
        closure=closure,
        binding=None,
    )

    assert "payload" not in captured


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
    contract_ref = await store.put(
        canonical_json_bytes({"schema_version": "io.v1", "input_schema_ref": None, "output_schema_ref": None, "success_semantics": None, "success_validator_ref": None}),
        media_type="application/vnd.loom.io-contract+json",
    )
    package = CapabilityPackageVersion(
        package_id="pkg-reference-only",
        package_version="v1",
        package_closure_version_ref="closure",
        source_run_ref="run",
        source_closure_version_ref="version",
        body={"operation_descriptor_ref": ResourceRef(resource_id="loom://check"), "program_content_ref": program_ref, "io_contract_ref": contract_ref},
    )
    session = WorkerSession("slave-a", "http://slave-a", transport=httpx.ASGITransport(app=app))
    await session.provision(
        command=CapabilityProvisionCommand(
            command_id="command-reference-only",
            package_version_ref=package.version_ref,
            package_digest=package.package_digest,
            target_slave="slave-a",
        ),
        package=package,
    )
    assert "program_bytes_b64" not in captured
