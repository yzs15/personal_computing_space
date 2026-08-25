from fastapi.testclient import TestClient

from loom_v2.observer.app import create_app


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
