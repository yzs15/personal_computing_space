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
async def test_dynamic_distributed_map_reduce_analysis():
    repo = ObserverRepository()
    mcp = DriverMCP(repo, "conversation-dynamic-analysis")
    parent_input_schema = await _put(mcp, {"type": "object", "required": ["partitions"]}, "application/schema+json")
    summarize_input_schema = await _put(mcp, {"type": "object", "required": ["items"]}, "application/schema+json")
    summarize_output_schema = await _put(
        mcp,
        {"type": "object", "required": ["count", "sum", "min", "max", "sumsq"]},
        "application/schema+json",
    )
    merge_input_schema = await _put(mcp, {"type": "object", "required": ["inputs"]}, "application/schema+json")
    merge_output_schema = await _put(
        mcp,
        {"type": "object", "required": ["count", "sum", "min", "max", "sumsq", "mean", "stddev"]},
        "application/schema+json",
    )
    parent_contract = await _put(
        mcp,
        {
            "schema_version": "io.v1",
            "input_schema_ref": parent_input_schema.model_dump(mode="json"),
            "output_schema_ref": merge_output_schema.model_dump(mode="json"),
            "success_semantics": None,
            "success_validator_ref": None,
        },
        "application/vnd.loom.io-contract+json",
    )
    summarize_contract = await _put(
        mcp,
        {
            "schema_version": "io.v1",
            "input_schema_ref": summarize_input_schema.model_dump(mode="json"),
            "output_schema_ref": summarize_output_schema.model_dump(mode="json"),
            "success_semantics": None,
            "success_validator_ref": None,
        },
        "application/vnd.loom.io-contract+json",
    )
    merge_contract = await _put(
        mcp,
        {
            "schema_version": "io.v1",
            "input_schema_ref": merge_input_schema.model_dump(mode="json"),
            "output_schema_ref": merge_output_schema.model_dump(mode="json"),
            "success_semantics": None,
            "success_validator_ref": None,
        },
        "application/vnd.loom.io-contract+json",
    )
    summarize_program = await _put(
        mcp,
        'import json,sys; d=json.load(sys.stdin); items=d["items"]; print(json.dumps({"count":len(items),"sum":sum(items),"min":min(items),"max":max(items),"sumsq":sum(x*x for x in items)}))',
        "text/x-python",
    )
    merge_program = await _put(
        mcp,
        'import json,sys,math; d=json.load(sys.stdin); xs=d["inputs"]; count=sum(x["count"] for x in xs); total=sum(x["sum"] for x in xs); sq=sum(x["sumsq"] for x in xs); lo=min(x["min"] for x in xs); hi=max(x["max"] for x in xs); mean=total/count; print(json.dumps({"count":count,"sum":total,"min":lo,"max":hi,"sumsq":sq,"mean":mean,"stddev":math.sqrt(max(0.0,sq/count-mean*mean))}))',
        "text/x-python",
    )
    partitions = [
        await _put(mcp, {"items": [1, 2]}, "application/json"),
        await _put(mcp, {"items": [3, 4]}, "application/json"),
    ]
    input_ref = await _put(mcp, {"partitions": [item.model_dump(mode="json") for item in partitions]}, "application/json")
    opened = await mcp.call(
        "loom_open_run",
        {
            "closure_contract": ClosureContract(
                closure_id="closure-dynamic-analysis",
                goal="distributed statistics",
                resource_budget={"max_nodes": 6, "max_live_nodes": 2},
                body=TaskClosure(
                    closure_id="closure-dynamic-analysis",
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
                        "program_content_ref": summarize_program.model_dump(mode="json"),
                        "io_contract_ref": summarize_contract.model_dump(mode="json"),
                        "operation_descriptor_ref": "loom://summarize",
                    },
                },
                {
                    "kind": "materialize_capability_package_candidate",
                    "value": {
                        "package_id": "merge-summaries",
                        "package_version": "v1",
                        "program_content_ref": merge_program.model_dump(mode="json"),
                        "io_contract_ref": merge_contract.model_dump(mode="json"),
                        "operation_descriptor_ref": "loom://merge-summaries",
                    },
                },
            ]
        },
    )
    packages = (await repo.get_run(run_id)).capability_packages
    summarize_package = next(item for item in packages if item.package_id == "summarize")
    merge_package = next(item for item in packages if item.package_id == "merge-summaries")
    summarize_ref = ResourceRef(resource_id=summarize_package.version_ref, version_or_digest=summarize_package.package_digest)
    merge_ref = ResourceRef(resource_id=merge_package.version_ref, version_or_digest=merge_package.package_digest)
    orchestration_source = f'''SUMMARIZE = {summarize_ref.model_dump(mode="json")}
MERGE = {merge_ref.model_dump(mode="json")}

async def orchestrate(ctx: "OrchestrationContext", input_ref: "ResourceRef") -> "ResourceRef":
    document = await ctx.read_json(input_ref)
    handles = [ctx.emit_node(SUMMARIZE, [partition]) for partition in document["partitions"]]
    summaries = [await ctx.result(handle) for handle in handles]
    merged = ctx.emit_node(MERGE, summaries)
    return await ctx.result(merged)
'''
    orchestration_program = await _put(mcp, orchestration_source, "text/x-python")
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
                        "allowed_node_package_refs": [summarize_ref.model_dump(mode="json"), merge_ref.model_dump(mode="json")],
                        "max_nodes": 6,
                        "max_live_nodes": 2,
                    },
                }
            ]
        },
    )
    assert patched["readiness"]["ready"] is True
    committed = await mcp.call("loom_commit_plan")
    await mcp.call("loom_start_run", {"closure_version": committed["closure_version"]})
    workers = {
        slave_id: WorkerSession(slave_id, f"http://{slave_id}", transport=httpx.ASGITransport(app=create_slave_app(slave_id)))
        for slave_id in ("slave-a", "slave-b")
    }
    driver = DriverService(repo, provider=object(), workers=workers, orchestration_executor=DockerOrchestrationExecutor(image="python:3.12-slim"))
    record, result = await driver._dispatch_execution(run_id, "distributed statistics")
    assert record.state == "completed"
    assert result.value["count"] == 4
    assert result.value["sum"] == 10
    assert result.value["mean"] == 2.5
    assert len(record.dynamic_nodes) == 3
    assert {node.package_ref.resource_id for node in record.dynamic_nodes} == {summarize_package.version_ref, merge_package.version_ref}
    accepted = [event for event in record.events if event["phase"] == "node_accepted"]
    assert {event["selected_target"] for event in accepted[:2]} == {"slave-a", "slave-b"}
    assert all(event.get("package_digest") for event in accepted)
    assert record.outcome["resource_ref"]["resource_id"].startswith("content://sha256/")
