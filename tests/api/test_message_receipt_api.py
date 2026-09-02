from fastapi.testclient import TestClient

from loom_v2.observer.app import create_app


def test_production_message_endpoint_persists_receipt_without_driver(monkeypatch):
    monkeypatch.setenv("LOOM_INTERNAL_API_SECRET", "test-secret")
    app = create_app()
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/messages",
            json={"request_id": "req-api", "conversation_ref": "conversation-api", "text": "hello"},
        )
        assert response.status_code == 202
        assert response.json()["accepted"] is True
        assert response.json()["status"] == "accepted"
        view = client.get("/api/v1/conversations/conversation-api")
        assert view.status_code == 200
        assert view.json()["messages"][0]["content"] == "hello"
        reused = client.post(
            "/api/v1/messages",
            json={"request_id": "req-api", "conversation_ref": "conversation-api", "text": "different"},
        )
        assert reused.status_code == 409

