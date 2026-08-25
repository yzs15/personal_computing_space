from fastapi.testclient import TestClient

from loom_v2.observer.app import create_app


def test_homepage_contains_conversation_and_run_drawer():
    client = TestClient(create_app())
    response = client.get("/")
    assert response.status_code == 200
    assert "Run drawer" in response.text
    assert "Event cursor" in response.text
