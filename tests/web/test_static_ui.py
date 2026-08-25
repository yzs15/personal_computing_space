from fastapi.testclient import TestClient

from loom_v2.observer.app import create_app


def test_homepage_contains_conversation_and_run_drawer():
    client = TestClient(create_app())
    response = client.get("/")
    assert response.status_code == 200
    assert "Run drawer" in response.text
    assert "Event cursor" in response.text
    assert 'id="agent-status"' in response.text
    assert 'id="conversation-list"' in response.text
    assert 'id="new-conversation"' in response.text
    assert 'id="conversation-status"' in response.text
    assert 'id="interrupt-conversation"' in response.text
    assert "Fake coding-agent" not in response.text


def test_frontend_loads_runtime_agent_status():
    client = TestClient(create_app())
    response = client.get("/static/app.js")
    assert response.status_code == 200
    assert "/api/v1/runtime" in response.text
    assert "coding_agent_label" in response.text
    assert "/api/v1/conversations" in response.text
    assert "assistant_text" in response.text
    assert "conversation_ref" in response.text
    assert "/interrupt" in response.text
    assert "thinking" in response.text
    assert "executing" in response.text
