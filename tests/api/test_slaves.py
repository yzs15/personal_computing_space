from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from fastapi.testclient import TestClient
import pytest

from loom_v2.observer.app import create_app
from loom_v2.settings import Settings


@pytest.fixture(params=["memory", "sqlite"])
def client(request, tmp_path):
    database_url = (
        "sqlite+aiosqlite:///:memory:" if request.param == "memory"
        else f"sqlite+aiosqlite:///{tmp_path / 'observer.db'}"
    )
    with TestClient(create_app(settings=Settings(
        database_url=database_url, workspace_id="my-workspace",
        internal_api_secret="internal-only",
    ))) as client:
        yield client


def register(client, slave_id="slave-a", instance="one", workspace="my-workspace", role="slave"):
    response = client.post("/internal/v1/agents/register", headers={
        "X-Loom-Internal-Token": "internal-only",
    }, json={
        "role": role, "agent_id": slave_id, "instance_id": instance,
        "workspace_id": workspace, "endpoint_url": "http://private-worker:8081",
        "protocol_version": "loom.v1",
        "capabilities": {
            "base_operations": ["run_code"],
            "operations": ["run_code", "stale-package-operation"],
            "executor_descriptors": [{"package_type": "function", "kind": "process:json_stdio", "version": "1"}],
            "runtime_plugin_descriptors": [{"plugin_id": "http", "supports": [{"package_type": "service", "execution": {"kind": "container:http", "version": "1"}}]}],
            "term_support": [], "private_token": "do-not-expose",
        },
    })
    assert response.status_code == 200
    return response.json()


def test_public_slaves_empty_and_workspace_bound(client):
    assert client.get("/api/v1/slaves").json() == {"workspace_id": "my-workspace", "slaves": []}
    assert client.get("/api/v1/slaves?workspace_id=my-workspace").status_code == 200
    response = client.get("/api/v1/slaves?workspace_id=other")
    assert response.status_code == 403
    assert response.json()["detail"] == "workspace_binding_mismatch"
    assert client.get("/api/v1/slaves?available_only=invalid").status_code == 422


def test_public_slaves_scoped_deduplicated_and_safe(client):
    register(client, "slave-b")
    register(client)
    register(client, instance="replacement")
    register(client, "foreign-slave", workspace="other")
    register(client, "driver", role="driver")

    response = client.get("/api/v1/slaves")
    assert response.status_code == 200
    slaves = response.json()["slaves"]
    assert [slave["slave_id"] for slave in slaves] == ["slave-a", "slave-b"]
    for slave in slaves:
        assert slave["available"] is True
        assert slave["lease_state"] == "active"
        assert slave["last_seen_at"]
        assert slave["base_operations"] == ["run_code"]
        assert slave["executor_descriptors"][0]["kind"] == "process:json_stdio"
        assert slave["runtime_plugin_descriptors"][0]["plugin_id"] == "http"
    for private in ("lease_id", "lease_id_hash", "endpoint_url", "instance_id", "private_token", "do-not-expose", "foreign-slave"):
        assert private not in response.text
    assert client.get("/api/v1/slaves?available_only=true").json() == response.json()
    assert client.get("/internal/v1/agents/slaves").status_code == 401


def test_public_slaves_released_and_expired_are_not_available(client):
    lease = register(client)
    released = client.post("/internal/v1/agents/slave-a/release", headers={
        "X-Loom-Internal-Token": "internal-only",
    }, json={
        "instance_id": "one", "workspace_id": "my-workspace", "role": "slave",
        "lease_id": lease["lease_id"], "epoch": lease["epoch"],
    })
    assert released.status_code == 200
    register(client, "slave-b")
    with patch("loom_v2.observer.repository.datetime") as clock:
        clock.now.return_value = datetime.now(timezone.utc) + timedelta(minutes=5)
        slaves = client.get("/api/v1/slaves").json()["slaves"]
        assert [(s["slave_id"], s["lease_state"], s["available"]) for s in slaves] == [
            ("slave-a", "released", False), ("slave-b", "expired", False),
        ]
        assert client.get("/api/v1/slaves?available_only=true").json()["slaves"] == []


def test_public_slaves_in_openapi(client):
    schema = client.get("/openapi.json").json()
    operation = schema["paths"]["/api/v1/slaves"]["get"]
    assert operation["responses"]["200"]["content"]["application/json"]["schema"]["$ref"].endswith("/PublicSlaveList")


def test_public_slaves_does_not_read_runs_or_mutate_dispatch_cache(client):
    register(client)
    repo = client.app.state.repo
    repo.slave_capabilities = {"slave-a": {"operations": {"foreign-run-operation"}}}
    with patch.object(repo, "_all_records", side_effect=AssertionError("must not read Runs")):
        response = client.get("/api/v1/slaves")
    assert response.status_code == 200
    assert "foreign-run-operation" not in response.text
    assert repo.slave_capabilities == {"slave-a": {"operations": {"foreign-run-operation"}}}


def test_public_slaves_uses_latest_offline_instance(client):
    register(client)
    future = datetime.now(timezone.utc) + timedelta(seconds=2)
    with patch("loom_v2.observer.repository.datetime") as clock:
        clock.now.return_value = future
        register(client, instance="newer")
    with patch("loom_v2.observer.repository.datetime") as clock:
        clock.now.return_value = future + timedelta(minutes=5)
        slaves = client.get("/api/v1/slaves").json()["slaves"]
    assert len(slaves) == 1
    assert slaves[0]["lease_state"] == "expired"
    seen = datetime.fromisoformat(slaves[0]["last_seen_at"])
    assert seen.replace(tzinfo=timezone.utc) == future
