import httpx
import pytest

from loom_v2.contracts.types import ClosureContract, ResourceRef, TaskClosure
from loom_v2.driver.mcp import DriverMCP
from loom_v2.driver.orchestrator import DockerOrchestrationExecutor
from loom_v2.driver.service import DriverService
from loom_v2.observer.repository import ObserverRepository
from loom_v2.driver.worker import WorkerSession
from loom_v2.slave.app import create_app as create_slave_app


async def _put(mcp: DriverMCP, content, media_type: str) -> ResourceRef:
    result = await mcp.call("loom_put_content", {"content": content, "media_type": media_type})
    return ResourceRef.model_validate(result["resource_ref"])


@pytest.mark.integration
@pytest.mark.asyncio
async def test_dynamic_orchestration_single_node_stress_and_admission():
    repo = ObserverRepository()
    mcp = DriverMCP(repo, "conversation-dynamic-stress")
    input_schema = await _put(mcp, {"type": "object", "required": ["partition"]}, "application/schema+json")
    node_input_schema = await _put(mcp, {"type": "object", "required": ["items"]}, "application/schema+json")
    output_schema = await _put(
        mcp,
        {"type": "object", "required": ["count", "sum", "min", "max", "sumsq"]},
        "application/schema+json",
    )
    parent_contract = await _put(
        mcp,
        {
            "schema_version": "io.v1",
            "input_schema_ref": input_schema.model_dump(mode="json"),
            "output_schema_ref": output_schema.model_dump(mode="json"),
            "success_semantics": None,
            "success_validator_ref": None,
        },
        "application/vnd.loom.io-contract+json",
    )
    node_contract = await _put(
        mcp,
        {
            "schema_version": "io.v1",
            "input_schema_ref": node_input_schema.model_dump(mode="json"),
            "output_schema_ref": output_schema.model_dump(mode="json"),
            "success_semantics": None,
            "success_validator_ref": None,
        },
        "application/vnd.loom.io-contract+json",
    )
    partition_ref = await _put(mcp, {"items": [1, 2, 3]}, "application/json")
    input_ref = await _put(mcp, {"partition": partition_ref.model_dump(mode="json")}, "application/json")
    node_program = await _put(
        mcp,
        'import json,sys; d=json.load(sys.stdin); items=d["items"]; print(json.dumps({"count":len(items),"sum":sum(items),"min":min(items),"max":max(items),"sumsq":sum(x*x for x in items)}))',
        "text/x-python",
    )
    opened = await mcp.call(
        "loom_open_run",
        {
            "closure_contract": ClosureContract(
                closure_id="closure-dynamic-stress",
                goal="summarize one partition",
                resource_budget={"max_nodes": 2, "max_live_nodes": 1},
                recovery_policy={"allow_reassignment": True},
                body=TaskClosure(
                    closure_id="closure-dynamic-stress",
                    program={"operation_ref": "loom://orchestrate", "io_contract_ref": parent_contract.model_dump(mode="json")},
                ),
            ).model_dump(mode="json")
        },
    )
    run_id = opened["run_id"]
    first = await mcp.call(
        "loom_apply_plan_patch",
        {
            "ops": [
                {"kind": "set_execution_payload", "value": {"node_id": "loom://orchestrate", "input_ref": input_ref.model_dump(mode="json")}},
                {
                    "kind": "materialize_capability_package_candidate",
                    "value": {
                        "package_id": "summarize",
                        "package_version": "v1",
                        "program_content_ref": node_program.model_dump(mode="json"),
                        "io_contract_ref": node_contract.model_dump(mode="json"),
                        "operation_descriptor_ref": "loom://summarize",
                    },
                },
            ]
        },
    )
    node_package = next(item for item in (await repo.get_run(run_id)).capability_packages if item.package_id == "summarize")
    package_ref = ResourceRef(resource_id=node_package.version_ref, version_or_digest=node_package.package_digest)
    orchestration_program = await _put(
        mcp,
        f'PACKAGE_REF = {package_ref.model_dump(mode="json")}\n\nasync def orchestrate(ctx: "OrchestrationContext", input_ref: "ResourceRef") -> "ResourceRef":\n    document = await ctx.read_json(input_ref)\n    handle = ctx.emit_node(PACKAGE_REF, [document["partition"]])\n    return await ctx.result(handle)\n',
        "text/x-python",
    )
    patched = await mcp.call(
        "loom_apply_plan_patch",
        {
            "ops": [
                {
                    "kind": "materialize_capability_package_candidate",
                    "value": {
                        "package_id": "orchestrate",
                        "package_version": "v1",
                        "program_content_ref": orchestration_program.model_dump(mode="json"),
                        "io_contract_ref": parent_contract.model_dump(mode="json"),
                        "operation_descriptor_ref": "loom://orchestrate",
                        "executor_kind": "orchestrator_python_v1",
                        "executor_operation": "orchestrate",
                        "allowed_node_package_refs": [package_ref.model_dump(mode="json")],
                        "max_nodes": 2,
                        "max_live_nodes": 1,
                    },
                }
            ]
        },
    )
    assert patched["readiness"]["ready"] is True
    committed = await mcp.call("loom_commit_plan")
    await mcp.call("loom_start_run", {"closure_version": committed["closure_version"]})
    slave_app = create_slave_app("slave-a")
    driver = DriverService(
        repo,
        provider=object(),
        workers={"slave-a": WorkerSession("slave-a", "http://slave-a", transport=httpx.ASGITransport(app=slave_app))},
        orchestration_executor=DockerOrchestrationExecutor(image="python:3.12-slim"),
    )
    record, result = await driver._dispatch_execution(run_id, "summarize one partition")
    assert record.state == "completed"
    assert result.resource_ref.resource_id.startswith("content://sha256/")
    assert len(record.dynamic_nodes) == 1
    assert record.dynamic_nodes[0].parent_execution_ref == record.execution_id
    assert [event["phase"] for event in record.events if event["phase"].startswith("node_")] == ["node_requested", "node_accepted", "node_dispatched", "node_completed"]

    assert record.outcome["resource_ref"]["resource_id"].startswith("content://sha256/")
    assert record.dynamic_nodes[0].package_digest == node_package.package_digest
