import pytest

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
            {"agent_id": "slave-b", "instance_id": "slave-b-new", "lease_state": "active", "capabilities": {"operations": ["run_code"]}},
        ]


@pytest.mark.asyncio
async def test_refresh_slaves_prefers_active_lease_over_stale_registration():
    repository = RemoteObserverRepository(MixedControl(), content_store=None)
    await repository.refresh_slaves()
    assert repository.slave_availability == {"slave-a": True, "slave-b": True}
    assert repository.slave_capabilities["slave-b"]["operations"] == {"run_code"}
