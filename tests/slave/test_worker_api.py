import httpx
import pytest
from fastapi.testclient import TestClient

from loom_v2.contracts.types import TaskClosure
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
