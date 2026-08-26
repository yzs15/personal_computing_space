import pytest
import httpx

from loom_v2.coding_agents.base import AgentEvent
from loom_v2.coding_agents.fake import FakeCodingAgentProvider
from loom_v2.driver.service import DriverService
from loom_v2.observer.repository import ObserverRepository
from loom_v2.observer.worker import WorkerSession
from loom_v2.slave.service import SlaveService
from loom_v2.slave.app import create_app as create_slave_app


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
        yield AgentEvent(
            "open_run",
            {
                "closure_contract": {
                    "closure_id": "closure-sort",
                    "goal": user_message,
                    "required_success_criteria": [],
                    "allowed_effects": ["read_workspace"],
                    "resource_budget": {"max_attempts": 1},
                    "recovery_policy": {},
                    "result_expectations": [{"kind": "content"}],
                    "body": {"closure_id": "closure-sort"},
                }
            },
        )
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
        yield AgentEvent("start_run", {})

    async def interrupt(self, turn_ref: str | None = None) -> None:
        return None

    async def close(self) -> None:
        return None


class DynamicToolProvider:
    def set_tool_handler(self, tools, handler) -> None:
        self.tool_names = {tool["name"] for tool in tools}
        self.tool_handler = handler

    async def start(self, conversation_ref: str, workspace_root: str) -> str:
        return "dynamic-tool-thread"

    async def send_turn(self, user_message: str):
        assert "loom_open_run" in self.tool_names
        await self.tool_handler(
            "loom_open_run",
            {
                "closure_contract": {
                    "closure_id": "closure-dynamic-tool",
                    "goal": user_message,
                    "required_success_criteria": [{"criterion_id": "echoed"}],
                    "allowed_effects": [],
                    "resource_budget": {"max_attempts": 1},
                    "recovery_policy": {"allow_reassignment": False},
                    "result_expectations": [{"kind": "content"}],
                    "body": {
                        "closure_id": "closure-dynamic-tool",
                        "program": {"operation_ref": "loom://echo"},
                        "compute": {"operation_ref": "loom://echo"},
                    },
                }
            },
        )
        yield AgentEvent("tool_call", {"tool": "loom_open_run"})
        await self.tool_handler("loom_commit_plan", {})
        yield AgentEvent("tool_call", {"tool": "loom_commit_plan"})
        await self.tool_handler("loom_start_run", {})
        yield AgentEvent("tool_call", {"tool": "loom_start_run"})

    async def interrupt(self, turn_ref: str | None = None) -> None:
        return None

    async def close(self) -> None:
        return None


class OpenOnlyProvider:
    async def start(self, conversation_ref: str, workspace_root: str) -> str:
        return "open-only-thread"

    async def send_turn(self, user_message: str):
        yield AgentEvent(
            "open_run",
            {
                "closure_contract": {
                    "closure_id": "closure-open-only",
                    "goal": user_message,
                    "body": {"closure_id": "closure-open-only"},
                }
            },
        )

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


@pytest.mark.asyncio
async def test_driver_dispatches_through_worker_session_http_boundary():
    repo = ObserverRepository()
    slave_app = create_slave_app("slave-a")
    worker = WorkerSession("slave-a", "http://slave-a", transport=httpx.ASGITransport(app=slave_app))
    driver = DriverService(repo, SortClosureProvider(), workers={"slave-a": worker})

    result = await driver.run_prompt("conversation-worker-http", "sort this")

    assert result["state"] == "completed"
    run = await repo.get_run(result["run_id"])
    assert run.outcome["value"] == {"items": [1, 2, 3]}


@pytest.mark.asyncio
async def test_driver_adopts_run_opened_through_dynamic_mcp_tool():
    repo = ObserverRepository()
    driver = DriverService(repo, DynamicToolProvider(), slaves={"slave-a": SlaveService("slave-a")})

    result = await driver.run_prompt("conversation-dynamic", "echo through MCP")

    assert result["run_id"] is not None
    assert result["state"] == "completed"
    run = await repo.get_run(result["run_id"])
    assert run.closure_contract is not None
    assert run.closure_contract.origin_conversation_ref == "conversation-dynamic"
    assert run.outcome["value"] == {"text": "echo through MCP"}


@pytest.mark.asyncio
async def test_driver_does_not_commit_or_start_when_agent_only_opens_run():
    repo = ObserverRepository()
    driver = DriverService(repo, OpenOnlyProvider(), slaves={"slave-a": SlaveService("slave-a")})

    result = await driver.run_prompt("conversation-open-only", "clarify before execution")

    assert result["run_id"] is not None
    assert result["state"] == "thinking"
    assert result["status"] == "thinking"
    assert result["closure_version"] is None
    assert result["execution_id"] is None
