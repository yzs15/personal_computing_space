import pytest
from types import SimpleNamespace

from loom_v2.contracts.types import ResourceRef
from loom_v2.driver.remote_repository import RemoteObserverRepository


class Control:
    workspace_id = "workspace-default"

    def __init__(self):
        self.calls = []

    async def command(self, name, arguments=None, **kwargs):
        self.calls.append((name, arguments or {}, kwargs))
        if name == "run.get":
            return {
                "run_id": "run-1",
                "task_ref": "conversation-1",
                "goal": "goal",
                "state": "running",
                "draft": {"version_id": "draft-1", "closure_id": "closure-1", "kind": "draft", "snapshot": {"closure_id": "closure-1"}, "snapshot_digest": "digest", "patch_cursor": 0},
                "committed": None,
                "closure_contract": None,
                "execution_id": "execution-1",
                "execution_epoch": 1,
                "attempts": [],
                "events": [],
                "dynamic_nodes": [],
            }
        if name == "capability.list":
            return {"slaves": [{"agent_id": "slave-a", "lease_state": "active", "capabilities": {"operations": ["echo"]}}]}
        return {}

    async def list_slaves(self):
        return [{"agent_id": "slave-a", "lease_state": "active", "capabilities": {"operations": ["echo"]}}]


@pytest.mark.asyncio
async def test_remote_repository_reads_runs_and_refreshes_registered_slaves():
    control = Control()
    repository = RemoteObserverRepository(control, content_store=None)
    record = await repository.get_run("run-1")
    assert record.run_id == "run-1"
    await repository.refresh_slaves()
    assert "slave-a" in repository.slave_capabilities
    assert repository.slave_capabilities["slave-a"]["operations"] == {"echo"}


@pytest.mark.asyncio
async def test_remote_repository_namespaces_message_command_request_id():
    control = Control()
    repository = RemoteObserverRepository(control, content_store=None)

    await repository.append_message("run-1", "user", "hello", request_id="turn-1")

    name, arguments, kwargs = control.calls[-1]
    assert name == "message.append"
    assert arguments["request_id"] == "turn-1"
    assert kwargs["request_id"] == "turn-1:message"


class MixedControl:
    workspace_id = "workspace-default"

    async def list_slaves(self):
        return [
            {"agent_id": "slave-a", "instance_id": "slave-a-old", "lease_state": "active", "capabilities": {"operations": ["echo"]}},
            {"agent_id": "slave-a", "instance_id": "slave-a-expired", "lease_state": "expired", "capabilities": {"operations": ["echo"]}},
            {"agent_id": "slave-b", "instance_id": "slave-b-expired", "lease_state": "expired", "capabilities": {"operations": ["echo"]}},
            {
                "agent_id": "slave-b",
                "instance_id": "slave-b-new",
                "lease_state": "active",
                "capabilities": {
                    "operations": ["run_code"],
                    "executor_descriptors": ["subprocess_json_v1"],
                },
            },
        ]


@pytest.mark.asyncio
async def test_refresh_slaves_prefers_active_lease_over_stale_registration():
    repository = RemoteObserverRepository(MixedControl(), content_store=None)
    await repository.refresh_slaves()
    assert repository.slave_agents["slave-a"]["instance_id"] == "slave-a-old"
    assert repository.slave_agents["slave-b"]["instance_id"] == "slave-b-new"
    assert repository.slave_instances[("slave-b", "slave-b-expired")]["lease_state"] == "expired"
    assert repository.slave_capabilities["slave-b"]["operations"] == {"run_code"}
    supported = SimpleNamespace(executor_operation="run_code", executor_kind="subprocess_json_v1")
    unsupported = SimpleNamespace(executor_operation="run_code", executor_kind="unknown_v1")
    assert repository._slave_supports_package("slave-b", supported) is True
    assert repository._slave_supports_package("slave-b", unsupported) is False


@pytest.mark.asyncio
async def test_remote_repository_reassigns_with_idempotent_request_id():
    control = Control()
    repository = RemoteObserverRepository(control, content_store=None)
    replacement = {"attempt_id": "attempt-new", "target": "slave-b"}

    async def command(name, arguments=None, **kwargs):
        control.calls.append((name, arguments or {}, kwargs))
        return {"attempt": replacement}

    control.command = command
    result = await repository.reassign_dynamic_node(
        "run-1",
        "node-1",
        lost_attempt_id="attempt-old",
        expected_execution_id="execution-1",
        expected_execution_epoch=1,
        target="slave-b",
        reason="worker_lease_expired",
    )

    assert result == replacement
    name, arguments, kwargs = control.calls[-1]
    assert name == "node.reassign"
    assert arguments["lost_attempt_id"] == "attempt-old"
    assert kwargs["request_id"] == "node-reassign:attempt-old:slave-b"
