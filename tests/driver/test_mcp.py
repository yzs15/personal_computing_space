import asyncio
import hashlib

import pytest

from loom_v2.content_store import canonical_json_bytes
from loom_v2.contracts.types import ResourceRef, TaskClosure
from loom_v2.driver.mcp import DriverMCP
from loom_v2.driver.service import DriverService
from loom_v2.observer.repository import ObserverRepository
from loom_v2.coding_agents.fake import FakeCodingAgentProvider
from loom_v2.slave.executor import ExecutionResult
from loom_v2.testing.observer import seed_embedded_slaves


def _seeded_repository() -> ObserverRepository:
    repo = ObserverRepository()
    seed_embedded_slaves(repo)
    return repo


@pytest.mark.asyncio
async def test_open_run_is_agent_decision_and_contract_is_scoped_by_driver():
    repo = ObserverRepository()
    mcp = DriverMCP(repo, "conversation-mcp")

    opened = await mcp.call(
        "loom_open_run",
        {
            "closure_contract": {
                "closure_id": "closure-mcp",
                "goal": "transform numbers",
                "required_success_criteria": [{"criterion_id": "transformed"}],
                "allowed_effects": ["read_workspace"],
                "resource_budget": {"max_attempts": 2},
                "recovery_policy": {"allow_reassignment": False},
                "result_expectations": [{"kind": "content"}],
                "body": TaskClosure(program={"operation_ref": "loom://test_transform"}).model_dump(mode="json"),
            }
        },
    )

    assert opened["run_id"]
    run = await repo.get_run(opened["run_id"])
    assert run.task_ref == "conversation-mcp"
    assert run.closure_contract is not None
    assert run.closure_contract.origin_conversation_ref == "conversation-mcp"
    assert run.closure_contract.workspace_id == "workspace-default"
    assert run.draft.closure_id == "closure-mcp"
    assert run.draft.snapshot.closure_id == "closure-mcp"


@pytest.mark.asyncio
async def test_query_capabilities_is_available_before_open_run():
    mcp = DriverMCP(_seeded_repository(), "conversation-capabilities-first")

    result = await mcp.call("loom_query_capabilities")

    assert result["workspace_id"] == "workspace-default"
    assert {item["resource_id"] for item in result["capabilities"]} == {"slave-a", "slave-b"}


def test_driver_mcp_exposes_dynamic_tool_specs():
    specs = DriverMCP.tool_specs()
    names = {item["name"] for item in specs}
    assert {"loom_open_run", "loom_apply_plan_patch", "loom_commit_plan", "loom_start_run"} <= names
    patch_spec = next(item for item in specs if item["name"] == "loom_apply_plan_patch")
    op_schema = patch_spec["inputSchema"]["properties"]["ops"]["items"]
    assert "kind" in op_schema["properties"]
    assert "set_program_ref" in op_schema["properties"]["kind"]["enum"]
    description = patch_spec["description"].lower()
    assert "batch" in description
    assert "atomic" in description
    assert "readiness" in description
    put_spec = next(item for item in specs if item["name"] == "loom_put_content")
    assert "operation-descriptor+json" in put_spec["description"]
    assert "do not calculate" in put_spec["description"].lower()
    assert "capability_exports" in description
    assert "omit `package_digest`" in description


@pytest.mark.asyncio
async def test_apply_plan_patch_accepts_explicit_operation_alias_from_json_tool_call():
    repo = _seeded_repository()
    mcp = DriverMCP(repo, "conversation-mcp-alias")
    await mcp.call(
        "loom_open_run",
        {"closure_contract": {"closure_id": "closure-alias", "goal": "run test code", "body": {"closure_id": "closure-alias"}}},
    )

    result = await mcp.call(
        "loom_apply_plan_patch",
        {"ops": [{"op": "set_program_ref", "value": {"program_ref": "loom://test_alias"}}]},
    )

    assert result["draft_version"].startswith("draft-")
    run = await repo.get_run(mcp.run_id)
    assert run.draft.snapshot.program.operation_ref == "loom://test_alias"


@pytest.mark.asyncio
async def test_bind_compute_hole_accepts_capability_uri_target_for_registered_slave():
    repo = ObserverRepository()
    seed_embedded_slaves(repo)
    mcp = DriverMCP(repo, "conversation-mcp-binding-uri")
    program = await mcp.call(
        "loom_put_content",
        {"media_type": "text/x-python", "content": "import json,sys; print(json.dumps(json.load(sys.stdin)))"},
    )
    contract = await mcp.call(
        "loom_put_content",
        {
            "media_type": "application/vnd.loom.io-contract+json",
            "content": {
                "schema_version": "io.v1",
                "input_schema_ref": None,
                "output_schema_ref": None,
                "success_semantics": None,
                "success_validator_ref": None,
            },
        },
    )
    descriptor = await mcp.call(
        "loom_put_content",
        {
            "media_type": "application/vnd.loom.operation-descriptor+json",
            "content": {"schema_version": "operation.v1", "resource_id": "loom://test_binding", "name": "test_binding"},
        },
    )
    await mcp.call(
        "loom_open_run", {"closure_contract": {"closure_id": "closure-binding-uri", "goal": "run package", "body": {"closure_id": "closure-binding-uri"}}},
    )
    await mcp.call(
        "loom_apply_plan_patch",
        {"ops": [
            {"kind": "set_program_ref", "value": "loom://test_binding"},
            {"kind": "set_io_contract_ref", "value": contract["resource_ref"]},
            {"kind": "set_compute_spec", "value": {"operation_ref": "loom://test_binding"}},
            {"kind": "add_typed_hole", "value": {"hole_id": "h_binding"}},
        ]},
    )
    await mcp.call(
        "loom_apply_plan_patch",
        {
            "ops": [{
                "kind": "materialize_capability_package_candidate",
                "value": {
                             "package_id": "mcp-binding-package",
                             "package_version": "v1",
                             "package_type": "function",
                             "execution": {"kind": "process:json_stdio", "version": "1"},
                             "capability_exports": [{
                                 "capability_descriptor_ref": descriptor["resource_ref"],
                                 "io_contract_ref": contract["resource_ref"],
                                 "effect_class": "Sandboxed",
                                 "permissions": [],
                                 "replay_safety": "DeclaredByPackage",
                                 "runtime_binding": {},
                             }],
                             "body": {
                                 "program_content_ref": program["resource_ref"],
                             },
                         },
            }],
        },
    )

    await mcp.call(
        "loom_apply_plan_patch",
        {
            "ops": [
                {
                    "kind": "bind_compute_hole",
                    "value": {
                        "binding_id": "binding-test",
                        "hole_id": "h_binding",
                        "capability_descriptor_ref": descriptor["resource_ref"],
                        "capability_package_ref": {"resource_id": "capability-package://mcp-binding-package/v1"},
                        "target_resource_ref": {"resource_id": "loom://compute/slave-a"},
                    },
                }
            ]
        },
    )

    readiness = await mcp.call("loom_inspect_plan_readiness")
    assert readiness["ready"] is True


@pytest.mark.asyncio
async def test_open_run_coerces_natural_language_body_to_task_closure_metadata():
    repo = ObserverRepository()
    mcp = DriverMCP(repo, "conversation-mcp-body-text")

    opened = await mcp.call(
        "loom_open_run",
        {
            "closure_contract": {
                "closure_id": "closure-body-text",
                "goal": "transform numbers",
                "body": "Invoke the transform program and return the result.",
            }
        },
    )

    run = await repo.get_run(opened["run_id"])
    assert run.closure_contract.body.metadata["description"].startswith("Invoke the transform")


@pytest.mark.asyncio
async def test_open_run_uses_goal_as_execution_prompt_when_transport_has_no_prompt():
    mcp = DriverMCP(ObserverRepository(), "conversation-mcp-no-prompt")

    await mcp.call(
        "loom_open_run",
        {
            "closure_contract": {
                "closure_id": "closure-no-prompt",
                "goal": "run the goal",
                "body": {"closure_id": "closure-no-prompt"},
            }
        },
    )

    assert mcp.prompt == "run the goal"


@pytest.mark.asyncio
async def test_start_run_returns_repair_outcome_as_successful_tool_call():
    repo = ObserverRepository()

    async def failing_executor(_operation, payload):
        digest = hashlib.sha256(canonical_json_bytes(payload)).hexdigest()
        return ExecutionResult(
            resource_ref=ResourceRef(
                resource_id=f"content://sha256/{digest}",
                identity_criterion="content_digest",
            ),
            value=payload,
            replay_safety="Idempotent",
            terminal_state="failed",
            terminal_error={"code": "worker_failed"},
        )

    service = DriverService(repo, FakeCodingAgentProvider(), executor=failing_executor)
    mcp = DriverMCP(
        repo,
        "conversation-mcp-repair",
        prompt="run failure",
        run_executor=service._execute_and_wait_local,
    )
    await mcp.call(
        "loom_open_run",
        {
            "closure_contract": {
                "closure_id": "closure-mcp-repair",
                "goal": "run failure",
                "body": {
                    "closure_id": "closure-mcp-repair",
                    "program": {"operation_ref": ""},
                },
            }
        },
    )
    await mcp.call("loom_apply_plan_patch", {"ops": []})
    committed = await mcp.call("loom_commit_plan")

    result = await mcp.call("loom_start_run", {"closure_version": committed["closure_version"]})

    assert result["success"] is True
    assert result["state"] == "awaiting_decision"
    assert result["disposition"] == "awaiting_decision"
    assert result["decision"] == "repair"
    assert result["terminal_error"] == {"code": "worker_failed"}
    assert result["decision_hint"] == "repair_plan_and_retry"


@pytest.mark.asyncio
async def test_start_run_wakes_when_run_is_cancelled():
    repo = ObserverRepository()
    executor_started = asyncio.Event()
    executor_cancelled = asyncio.Event()

    async def blocking_executor(_operation, _payload):
        executor_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            executor_cancelled.set()
            raise

    service = DriverService(repo, FakeCodingAgentProvider(), executor=blocking_executor)
    mcp = DriverMCP(
        repo,
        "conversation-mcp-cancelled",
        prompt="wait",
        run_executor=service._execute_and_wait_local,
    )
    await mcp.call(
        "loom_open_run",
        {
            "closure_contract": {
                "closure_id": "closure-mcp-cancelled",
                "goal": "wait",
                "body": {
                    "closure_id": "closure-mcp-cancelled",
                    "program": {"operation_ref": ""},
                },
            }
        },
    )
    await mcp.call("loom_apply_plan_patch", {"ops": []})
    committed = await mcp.call("loom_commit_plan")
    start_task = asyncio.create_task(
        mcp.call("loom_start_run", {"closure_version": committed["closure_version"]})
    )
    await asyncio.wait_for(executor_started.wait(), timeout=1)

    await repo.cancel_run(mcp.run_id)
    result = await asyncio.wait_for(start_task, timeout=1)

    assert result["success"] is True
    assert result["state"] == "cancelled"
    assert result["disposition"] == "cancelled"
    assert result["decision"] is None
    assert executor_cancelled.is_set()
