from fastapi.testclient import TestClient

from loom_v2.coding_agents.base import AgentEvent
from loom_v2.observer.repository import ObserverRepository
from loom_v2.observer.app import create_app


class ApiStalledProvider:
    async def start(self, conversation_ref: str, workspace_root: str) -> str:
        return "api-stalled-thread"

    async def send_turn(self, user_message: str):
        yield AgentEvent(
            "open_run",
            {"closure_contract": {"closure_id": "api-stalled", "goal": user_message, "body": {"closure_id": "api-stalled"}}},
        )
        yield AgentEvent("agent_stalled", {"code": "coding_agent_stalled", "source": "composite_signal", "message": "no progress"})

    async def interrupt(self, turn_ref: str | None = None) -> None:
        return None

    async def close(self) -> None:
        return None


def test_message_runs_fake_driver_and_sse_exposes_events(monkeypatch):
    monkeypatch.setenv("LOOM_CODING_AGENT_BACKEND", "fake")
    client = TestClient(create_app())
    response = client.post("/api/v1/messages", json={"conversation_ref": "conversation-ui", "text": "echo hello"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["state"] == "completed"
    assert payload["resource_ref"].startswith("result-")
    assert payload["conversation_ref"] == "conversation-ui"
    assert payload["assistant_text"] == "I will refine the closure in multiple patches."
    history = client.get("/api/v1/conversations/conversation-ui")
    assert [message["role"] for message in history.json()["messages"]] == ["user", "assistant"]
    stream = client.get("/api/v1/conversations/conversation-ui/stream")
    assert stream.status_code == 200
    assert "execution_started" in stream.text
    closed = client.post(f"/api/v1/runs/{payload['run_id']}/close")
    assert closed.status_code == 200
    assert closed.json()["state"] == "closed"


def test_completed_conversation_exposes_status(monkeypatch):
    monkeypatch.setenv("LOOM_CODING_AGENT_BACKEND", "fake")
    client = TestClient(create_app())
    response = client.post("/api/v1/messages", json={"conversation_ref": "conversation-status", "text": "echo hello"})
    assert response.status_code == 200
    assert client.get("/api/v1/conversations/conversation-status").json()["status"] == "completed"
    summary = client.get("/api/v1/conversations").json()[0]
    assert summary["status"] == "completed"


def test_interrupt_without_active_turn_returns_conflict():
    client = TestClient(create_app())
    response = client.post("/api/v1/conversations/not-running/interrupt")
    assert response.status_code == 409
    assert response.json()["detail"] == "conversation_not_active"


def test_stall_response_exposes_structured_reason_and_failed_conversation():
    app = create_app(ObserverRepository())
    app.state.driver.provider = ApiStalledProvider()
    client = TestClient(app)

    response = client.post("/api/v1/messages", json={"conversation_ref": "conversation-api-stalled", "text": "refine"})

    assert response.status_code == 503
    assert response.json()["code"] == "coding_agent_stalled"
    assert response.json()["details"]["source"] == "composite_signal"
    conversation = client.get("/api/v1/conversations/conversation-api-stalled").json()
    assert conversation["status"] == "failed"
