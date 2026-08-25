from fastapi.testclient import TestClient

from loom_v2.observer.app import create_app


def test_message_runs_fake_driver_and_sse_exposes_events():
    client = TestClient(create_app())
    response = client.post("/api/v1/messages", json={"conversation_ref": "conversation-ui", "text": "echo hello"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["state"] == "running"
    stream = client.get("/api/v1/conversations/conversation-ui/stream")
    assert stream.status_code == 200
    assert "execution_started" in stream.text
