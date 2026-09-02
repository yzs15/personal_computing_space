from fastapi.testclient import TestClient
import pytest
from datetime import datetime, timedelta, timezone

from loom_v2.observer.app import create_app
from loom_v2.observer.repository import ObserverRepository
from loom_v2.contracts.agents import AgentRegistration, DriverCommand


def registration(instance_id: str, role: str = "driver") -> dict:
    return {"role": role, "agent_id": "driver-default" if role == "driver" else instance_id, "instance_id": instance_id, "workspace_id": "workspace-default", "endpoint_url": "http://driver:8090", "protocol_version": "loom.v1", "capabilities": {"model": "deepseek-v4-flash"}}


def test_registering_new_driver_increments_epoch_and_fences_old_client():
    with TestClient(create_app()) as client:
        first = client.post("/internal/v1/agents/register", json=registration("instance-1")).json()
        second = client.post("/internal/v1/agents/register", json=registration("instance-2")).json()
        assert second["epoch"] == first["epoch"] + 1
        stale = client.post("/internal/v1/driver/commands", json={"request_id": "r", "driver_id": "driver-default", "instance_id": "instance-1", "lease_id": first["lease_id"], "driver_epoch": first["epoch"], "command": "run.get", "arguments": {"run_id": "run-1"}})
        assert stale.status_code == 409
        assert stale.json()["detail"] == "stale_driver_epoch"


def test_observer_starts_without_registered_slave_or_driver():
    with TestClient(create_app()) as client:
        assert client.get("/healthz").json()["ok"] is True
        assert client.post("/api/v1/messages", json={"request_id": "req-1", "conversation_ref": "c", "text": "hello"}).status_code == 503


@pytest.mark.asyncio
async def test_node_fail_command_passes_attempt_and_error_as_keywords():
    repo = ObserverRepository()
    lease = await repo.register_agent(
        AgentRegistration(
            role="driver",
            agent_id="driver-default",
            instance_id="instance-1",
            workspace_id="workspace-default",
            endpoint_url="http://driver:8090",
            protocol_version="loom.v1",
        )
    )
    calls = {}

    async def fake_fail(run_id, node_id, *, attempt_id=None, error=None):
        calls.update(run_id=run_id, node_id=node_id, attempt_id=attempt_id, error=error)
        return type("Node", (), {"model_dump": lambda self, mode="json": {"node_id": node_id}})()

    repo.fail_dynamic_node = fake_fail
    result = await repo.execute_driver_command(
        DriverCommand(
            request_id="node-fail-1",
            driver_id="driver-default",
            instance_id="instance-1",
            lease_id=lease.lease_id,
            driver_epoch=lease.epoch,
            command="node.fail",
            arguments={"run_id": "run-1", "node_id": "node-1", "attempt_id": "attempt-1", "reason": {"code": "boom"}},
        )
    )
    assert result == {"node_id": "node-1"}
    assert calls == {"run_id": "run-1", "node_id": "node-1", "attempt_id": "attempt-1", "error": {"code": "boom"}}


@pytest.mark.asyncio
async def test_slave_runtime_view_prefers_active_instance_over_expired_history():
    repo = ObserverRepository()
    await repo.register_agent(
        AgentRegistration(
            role="slave",
            agent_id="slave-a",
            instance_id="old",
            workspace_id="workspace-default",
            endpoint_url="http://slave-a:8081",
            protocol_version="loom.v1",
            capabilities={"operations": ["echo"]},
        )
    )
    await repo.register_agent(
        AgentRegistration(
            role="slave",
            agent_id="slave-a",
            instance_id="active",
            workspace_id="workspace-default",
            endpoint_url="http://slave-a:8081",
            protocol_version="loom.v1",
            capabilities={"operations": ["echo", "sort"]},
        )
    )
    old_key = ("workspace-default", "slave", "slave-a", "old")
    active_key = ("workspace-default", "slave", "slave-a", "active")
    repo.agents[old_key]["last_seen_at"] = datetime.now(timezone.utc) - timedelta(seconds=60)
    repo.agents = {active_key: repo.agents[active_key], old_key: repo.agents[old_key]}

    await repo.list_agents("workspace-default", role="slave")

    assert repo.slave_availability["slave-a"] is True
    assert repo.slave_capabilities["slave-a"]["operations"] == {"echo", "sort"}


@pytest.mark.asyncio
async def test_registry_keeps_previous_driver_instance_as_expired_history():
    repo = ObserverRepository()
    first = await repo.register_agent(AgentRegistration(role="driver", agent_id="driver-default", instance_id="one", workspace_id="workspace-default", endpoint_url="http://driver:8090", protocol_version="loom.v1"))
    await repo.register_agent(AgentRegistration(role="driver", agent_id="driver-default", instance_id="two", workspace_id="workspace-default", endpoint_url="http://driver:8090", protocol_version="loom.v1"))
    agents = await repo.list_agents("workspace-default", role="driver")
    assert {agent["instance_id"] for agent in agents} == {"one", "two"}
    assert next(agent for agent in agents if agent["instance_id"] == "one")["lease_state"] == "expired"
    with pytest.raises(ValueError, match="stale_driver_epoch"):
        await repo.heartbeat_agent("driver-default", "one", first.lease_id, first.epoch, workspace_id="workspace-default")


@pytest.mark.asyncio
async def test_registry_keeps_only_one_active_slave_instance_per_agent_id():
    repo = ObserverRepository()
    first = await repo.register_agent(
        AgentRegistration(
            role="slave",
            agent_id="slave-a",
            instance_id="one",
            workspace_id="workspace-default",
            endpoint_url="http://slave-a:8081",
            protocol_version="loom.v1",
        )
    )
    second = await repo.register_agent(
        AgentRegistration(
            role="slave",
            agent_id="slave-a",
            instance_id="two",
            workspace_id="workspace-default",
            endpoint_url="http://slave-a:8081",
            protocol_version="loom.v1",
        )
    )

    agents = await repo.list_agents("workspace-default", role="slave")
    assert {item["instance_id"] for item in agents} == {"one", "two"}
    assert next(item for item in agents if item["instance_id"] == "one")["lease_state"] == "expired"
    assert next(item for item in agents if item["instance_id"] == "two")["lease_state"] == "active"
    with pytest.raises(ValueError, match="stale_agent_lease"):
        await repo.heartbeat_agent("slave-a", "one", first.lease_id, first.epoch, workspace_id="workspace-default", role="slave")
    await repo.heartbeat_agent("slave-a", "two", second.lease_id, second.epoch, workspace_id="workspace-default", role="slave")
