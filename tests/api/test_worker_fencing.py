from fastapi.testclient import TestClient

from loom_v2.observer.app import create_app


def test_legacy_terminal_report_endpoint_is_removed():
    client = TestClient(create_app())
    response = client.post(
        "/worker/v1/terminal",
        json={
            "attempt_id": "a-1",
            "execution_epoch": 1,
            "session_generation": 1,
            "operation_id": "terminal-1",
            "outcome": {"status": "completed"},
        },
    )
    assert response.status_code == 404
