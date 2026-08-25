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
