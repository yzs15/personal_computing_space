from fastapi.testclient import TestClient

from loom_v2.observer.app import create_app


def test_capabilities_expose_term_support_without_granting_access():
    client = TestClient(create_app())
    response = client.get("/api/v1/capabilities")
    assert response.status_code == 200
    payload = response.json()
    assert {item["slave_id"] for item in payload} == {"slave-a", "slave-b"}
    assert any(term["kind"] == "loom.compute.precision.v1" for item in payload for term in item["term_support"])
