import pytest

from loom_v2.contracts.types import TaskClosure
from loom_v2.driver.mcp import DriverMCP
from loom_v2.observer.repository import ObserverRepository


@pytest.mark.asyncio
async def test_open_run_is_agent_decision_and_contract_is_scoped_by_driver():
    repo = ObserverRepository()
    mcp = DriverMCP(repo, "conversation-mcp")

    opened = await mcp.call(
        "loom_open_run",
        {
            "closure_contract": {
                "closure_id": "closure-mcp",
                "goal": "sort numbers",
                "required_success_criteria": [{"criterion_id": "sorted"}],
                "allowed_effects": ["read_workspace"],
                "resource_budget": {"max_attempts": 2},
                "recovery_policy": {"allow_reassignment": False},
                "result_expectations": [{"kind": "content"}],
                "body": TaskClosure(program={"operation_ref": "loom://sort"}).model_dump(mode="json"),
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
    mcp = DriverMCP(ObserverRepository(), "conversation-capabilities-first")

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


@pytest.mark.asyncio
async def test_apply_plan_patch_accepts_explicit_operation_alias_from_json_tool_call():
    repo = ObserverRepository()
    mcp = DriverMCP(repo, "conversation-mcp-alias")
    await mcp.call(
        "loom_open_run",
        {"closure_contract": {"closure_id": "closure-alias", "goal": "echo", "body": {"closure_id": "closure-alias"}}},
    )

    result = await mcp.call(
        "loom_apply_plan_patch",
        {"ops": [{"op": "set_program_ref", "value": {"program_ref": "loom://echo"}}]},
    )

    assert result["draft_version"].startswith("draft-")
    run = await repo.get_run(mcp.run_id)
    assert run.draft.snapshot.program.operation_ref == "loom://echo"


@pytest.mark.asyncio
async def test_bind_compute_hole_accepts_capability_uri_target_for_registered_slave():
    repo = ObserverRepository()
    mcp = DriverMCP(repo, "conversation-mcp-binding-uri")
    await mcp.call(
        "loom_open_run",
        {"closure_contract": {"closure_id": "closure-binding-uri", "goal": "sort", "body": {"closure_id": "closure-binding-uri"}}},
    )
    await mcp.call(
        "loom_apply_plan_patch",
        {"ops": [{"kind": "set_program_ref", "value": "loom://sort"}, {"kind": "add_typed_hole", "value": {"hole_id": "h_sort"}}]},
    )

    await mcp.call(
        "loom_apply_plan_patch",
        {
            "ops": [
                {
                    "kind": "bind_compute_hole",
                    "value": {
                        "binding_id": "binding-sort",
                        "hole_id": "h_sort",
                        "capability_descriptor_ref": {"resource_id": "loom://capability/slave-a"},
                        "target_resource_ref": {"resource_id": "loom://compute/slave-a"},
                        "realization_digest": "sort-binding",
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
                "goal": "sort numbers",
                "body": "Invoke the sort program and return the sorted result.",
            }
        },
    )

    run = await repo.get_run(opened["run_id"])
    assert run.closure_contract.body.metadata["description"].startswith("Invoke the sort")
