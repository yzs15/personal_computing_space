from fastapi.testclient import TestClient

from loom_v2.observer.app import create_app


def test_health_endpoint_reports_role():
    response = TestClient(create_app()).get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"ok": True, "service": "observer"}
