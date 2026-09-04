from fastapi.testclient import TestClient
from datetime import datetime, timedelta, timezone

from loom_v2.observer.app import create_app
from loom_v2.observer.repository import ObserverRepository


def test_capabilities_expose_term_support_without_granting_access():
    client = TestClient(create_app())
    response = client.get("/api/v1/capabilities")
    assert response.status_code == 200
    payload = response.json()
    assert {item["slave_id"] for item in payload} == {"slave-a", "slave-b"}
    assert any(term["kind"] == "loom.compute.precision.v1" for item in payload for term in item["term_support"])


def test_capability_availability_is_derived_from_slave_lease():
    repository = ObserverRepository()
    slave_a = next(
        item
        for key, item in repository.agents.items()
        if key[1:3] == ("slave", "slave-a") and item.get("lease_state") == "active"
    )
    slave_a["last_seen_at"] = datetime.now(timezone.utc) - timedelta(seconds=60)

    with TestClient(create_app(repository)) as client:
        payload = client.get("/api/v1/capabilities").json()

    availability = {item["slave_id"]: item["available"] for item in payload}
    assert availability == {"slave-a": False, "slave-b": True}
