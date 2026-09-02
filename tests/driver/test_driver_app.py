import httpx
from fastapi.testclient import TestClient

from loom_v2.driver.app import create_app


class ObserverTransport(httpx.AsyncBaseTransport):
    def __init__(self):
        self.requests = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.url.path.endswith("/register"):
            return httpx.Response(200, json={"agent_id": "driver-default", "instance_id": "test-instance", "workspace_id": "workspace-default", "lease_id": "lease-1", "epoch": 1, "heartbeat_interval_seconds": 60})
        return httpx.Response(200, json={"agent_id": "driver-default", "instance_id": "test-instance", "workspace_id": "workspace-default", "lease_id": "lease-1", "epoch": 1, "heartbeat_interval_seconds": 60})


def test_driver_message_endpoint_requires_internal_token(monkeypatch):
    monkeypatch.setenv("LOOM_INTERNAL_API_SECRET", "secret")
    app = create_app(observer_transport=ObserverTransport())
    with TestClient(app) as client:
        response = client.post("/driver/v1/messages", json={"request_id": "r", "conversation_ref": "c", "text": "hello"})
    assert response.status_code == 401


def test_driver_registers_on_startup():
    transport = ObserverTransport()
    app = create_app(observer_transport=transport)
    with TestClient(app):
        assert transport.requests[0].url.path == "/internal/v1/agents/register"

