import pytest

from loom_v2.coding_agents.base import AgentEvent
from loom_v2.coding_agents.fake import FakeCodingAgentProvider
from loom_v2.driver.service import DriverService
from loom_v2.observer.repository import ObserverRepository
from loom_v2.slave.service import SlaveService


@pytest.mark.asyncio
async def test_driver_applies_fake_patches_continuously_and_starts_execution():
    repo = ObserverRepository()
    driver = DriverService(repo, FakeCodingAgentProvider())
    result = await driver.run_prompt("conversation-1", "echo hello")
    assert result["state"] == "completed"
    assert result["patches"] >= 3
    assert result["closure_version"].startswith("committed-")
    assert result["resource_ref"].startswith("result-")
    assert result["conversation_ref"] == "conversation-1"
    assert result["assistant_text"] == "I will refine the closure in multiple patches."
    conversation = await repo.get_conversation("conversation-1")
    assert [message["role"] for message in conversation["messages"]] == ["user", "assistant"]


class SortClosureProvider:
    async def start(self, conversation_ref: str, workspace_root: str) -> str:
        return "sort-thread"

    async def send_turn(self, user_message: str):
        yield AgentEvent("apply_plan_patch", {"ops": [{"kind": "set_program_ref", "value": "loom://sort"}]})
        yield AgentEvent("apply_plan_patch", {"ops": [{"kind": "set_compute_spec", "value": {"operation_ref": "loom://sort"}}]})
        yield AgentEvent("apply_plan_patch", {"ops": [{"kind": "add_typed_hole", "value": {"hole_id": "h_sort"}}]})
        yield AgentEvent("apply_plan_patch", {"ops": [{"kind": "set_execution_payload", "value": {"items": [3, 1, 2]}}]})
        yield AgentEvent(
            "apply_plan_patch",
            {
                "ops": [
                    {
                        "kind": "bind_compute_hole",
                        "value": {
                            "binding_id": "binding-sort",
                            "hole_id": "h_sort",
                            "capability_descriptor_ref": {"resource_id": "capability://slave-a/sort"},
                            "target_resource_ref": {"resource_id": "slave-a"},
                            "realization_digest": "realization-sort",
                        },
                    }
                ]
            },
        )
        yield AgentEvent("inspect_plan_readiness", {})
        yield AgentEvent("commit_plan", {})

    async def interrupt(self, turn_ref: str | None = None) -> None:
        return None

    async def close(self) -> None:
        return None


@pytest.mark.asyncio
async def test_driver_dispatches_committed_operation_to_bound_slave():
    repo = ObserverRepository()
    driver = DriverService(repo, SortClosureProvider(), slaves={"slave-a": SlaveService("slave-a")})

    result = await driver.run_prompt("conversation-sort", "sort this")

    assert result["state"] == "completed"
    run = await repo.get_run(result["run_id"])
    assert run.outcome["value"] == {"items": [1, 2, 3]}
    assert run.attempts[0]["target"] == "slave-a"
