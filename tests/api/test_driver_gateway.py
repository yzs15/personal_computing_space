import asyncio
import time

import httpx
from fastapi.testclient import TestClient

from loom_v2.observer.app import create_app


class DriverTransport(httpx.AsyncBaseTransport):
    def __init__(self):
        self.requests = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return httpx.Response(200, json={"conversation_ref": "conversation-1", "status": "completed"})


def test_observer_forwards_message_to_active_driver():
    transport = DriverTransport()
    app = create_app()
    app.state.gateway.transport = transport
    with TestClient(app) as client:
        client.post("/internal/v1/agents/register", json={"role": "driver", "agent_id": "driver-default", "instance_id": "instance-1", "workspace_id": "workspace-default", "endpoint_url": "http://driver:8090", "protocol_version": "loom.v1"})
        response = client.post("/api/v1/messages", json={"request_id": "req-1", "conversation_ref": "conversation-1", "text": "hello"})
        assert response.status_code == 202
        payload = response.json()
        assert payload["accepted"] is True
        assert payload["request_id"] == "req-1"
        assert payload["status"] == "accepted"
        deadline = time.time() + 5
        while not transport.requests and time.time() < deadline:
            time.sleep(0.02)
    assert transport.requests and transport.requests[-1].url.path == "/driver/v1/messages"


class BlockingDriverTransport(httpx.AsyncBaseTransport):
    def __init__(self):
        self.released = asyncio.Event()
        self.requests = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        await self.released.wait()
        return httpx.Response(200, json={"status": "completed"})


def test_observer_deduplicates_in_flight_request_by_request_id():
    transport = BlockingDriverTransport()
    app = create_app()
    app.state.gateway.transport = transport
    with TestClient(app) as client:
        client.post("/internal/v1/agents/register", json={"role": "driver", "agent_id": "driver-default", "instance_id": "instance-1", "workspace_id": "workspace-default", "endpoint_url": "http://driver:8090", "protocol_version": "loom.v1"})
        first = client.post("/api/v1/messages", json={"request_id": "req-dup", "conversation_ref": "conversation-dup", "text": "hello"})
        assert first.status_code == 202
        deadline = time.time() + 5
        while not transport.requests and time.time() < deadline:
            time.sleep(0.02)
        retry = client.post("/api/v1/messages", json={"request_id": "req-dup", "conversation_ref": "conversation-dup", "text": "hello"})
        assert retry.status_code == 202
        assert retry.json()["status"] == "in_flight"
        assert len(transport.requests) == 1
        transport.released.set()
    assert transport.requests[-1].url.path == "/driver/v1/messages"
