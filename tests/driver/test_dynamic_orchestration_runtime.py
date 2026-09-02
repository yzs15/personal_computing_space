import pytest
import httpx

from loom_v2.contracts.types import ClosureContract, ResourceRef, TaskClosure
from loom_v2.contracts.types import ComputeRequirement
from loom_v2.driver.orchestration_runtime import DynamicOrchestrationRuntime
from loom_v2.driver.orchestrator import DockerOrchestrationExecutor
from loom_v2.driver.service import DriverService
from loom_v2.observer.repository import ObserverRepository
from loom_v2.driver.worker import WorkerSession
from loom_v2.slave.app import create_app as create_slave_app


async def _content(repo: ObserverRepository, value, media_type: str):
    return await repo.put_content(value, media_type=media_type)


@pytest.mark.integration
async def test_dynamic_orchestration_runtime_executes_docker_program_and_slave_node():
    repo = ObserverRepository()
    input_schema = await _content(repo, {"type": "object", "required": ["partition"]}, "application/schema+json")
    output_schema = await _content(
        repo,
        {
            "type": "object",
            "required": ["count", "sum", "min", "max", "sumsq"],
            "properties": {
                "count": {"type": "integer"},
                "sum": {"type": "number"},
                "min": {"type": "number"},
                "max": {"type": "number"},
                "sumsq": {"type": "number"},
            },
        },
        "application/schema+json",
    )
    node_input_schema = await _content(
        repo,
        {"type": "object", "required": ["items"], "properties": {"items": {"type": "array"}}},
        "application/schema+json",
    )
    parent_contract = await _content(
        repo,
        {
            "schema_version": "io.v1",
            "input_schema_ref": input_schema.model_dump(mode="json"),
            "output_schema_ref": output_schema.model_dump(mode="json"),
            "success_semantics": None,
            "success_validator_ref": None,
        },
        "application/vnd.loom.io-contract+json",
    )
    node_contract = await _content(
        repo,
        {
            "schema_version": "io.v1",
            "input_schema_ref": node_input_schema.model_dump(mode="json"),
            "output_schema_ref": output_schema.model_dump(mode="json"),
            "success_semantics": None,
            "success_validator_ref": None,
        },
        "application/vnd.loom.io-contract+json",
    )
    node_input = await _content(repo, {"items": [1, 2, 3]}, "application/json")
    parent_input = await _content(
        repo,
        {"partition": node_input.model_dump(mode="json")},
        "application/json",
    )
    node_program = await _content(
        repo,
        'import json,sys; d=json.load(sys.stdin); items=d["items"]; print(json.dumps({"count":len(items),"sum":sum(items),"min":min(items),"max":max(items),"sumsq":sum(x*x for x in items)}))',
        "text/x-python",
    )

    closure = ClosureContract(
        closure_id="closure-runtime",
        goal="dynamic summarize",
        body=TaskClosure(
            closure_id="closure-runtime",
            program={"operation_ref": "loom://orchestrate", "io_contract_ref": parent_contract.model_dump(mode="json")},
        ),
    )
    run = await repo.open_run("run-dynamic-runtime", "conversation-dynamic-runtime", "dynamic summarize", closure_contract=closure)
    first = await repo.apply_patch(
        run.run_id,
        run.draft_version,
        run.draft_digest,
        "materialize-node",
        [
            {"kind": "set_execution_payload", "value": {"node_id": "loom://orchestrate", "input_ref": parent_input.model_dump(mode="json")}},
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
        ],
    )
    node_package = next(
        package
        for package in (await repo.get_run(run.run_id)).capability_packages
        if package.package_id == "summarize"
    )
    package_ref = ResourceRef(resource_id=node_package.version_ref, version_or_digest=node_package.package_digest)
    orchestration_source = f'''
PACKAGE_REF = {package_ref.model_dump(mode="json")}


async def orchestrate(ctx: "OrchestrationContext", input_ref: "ResourceRef") -> "ResourceRef":
    document = await ctx.read_json(input_ref)
    handle = ctx.emit_node(PACKAGE_REF, [document["partition"]])
    return await ctx.result(handle)
'''
    orchestration_program = await _content(repo, orchestration_source, "text/x-python")
    patched = await repo.apply_patch(
        run.run_id,
        first.draft_version,
        first.draft_digest,
        "materialize-orchestration",
        [
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
            },
        ],
    )
    assert patched.readiness["ready"] is True
    committed = await repo.commit(run.run_id, patched.draft_version, patched.draft_digest)
    started = await repo.start(run.run_id, committed.version_id)

    slave_app = create_slave_app("slave-a")
    driver = DriverService(
        repo,
        provider=object(),
        workers={
            "slave-a": WorkerSession(
                "slave-a",
                "http://slave-a",
                transport=httpx.ASGITransport(app=slave_app),
            )
        },
        orchestration_executor=DockerOrchestrationExecutor(image="python:3.12-slim"),
    )
    completed, result = await driver._dispatch_execution(run.run_id, "dynamic summarize")

    assert completed.state == "completed"
    assert result.value == {"count": 3, "sum": 6, "min": 1, "max": 3, "sumsq": 14}
    assert result.resource_ref.resource_id.startswith("content://sha256/")
    record = await repo.get_run(run.run_id)
    assert started["execution_id"] == record.execution_id
    assert len(record.dynamic_nodes) == 1
    assert record.dynamic_nodes[0].state == "completed"
    assert record.attempts[0]["node_id"] == record.dynamic_nodes[0].node_id
    assert record.attempts[0]["target"] == "slave-a"
    assert record.events[-1]["phase"] == "orchestration_completed"


async def test_target_selection_applies_permissions_locality_and_round_robin():
    repo = ObserverRepository()
    run, patched = await _dynamic_fixture_for_target_selection(repo)
    record = await repo.get_run(run.run_id)
    package = next(package for package in record.capability_packages if package.package_id == "summarize")
    runtime = DynamicOrchestrationRuntime(
        repository=repo,
        executor=object(),
        workers={
            "slave-a": WorkerSession("slave-a", "http://slave-a"),
            "slave-b": WorkerSession("slave-b", "http://slave-b"),
        },
    )

    denied = package.model_copy(update={"permissions": ["compute"], "package_digest": ""})
    try:
        runtime._select_target(denied, record)
    except RuntimeError as exc:
        assert str(exc) == "node_permission_denied"
    else:
        raise AssertionError("expected permission denial")

    record.committed.snapshot.compute.requirements.append(
        ComputeRequirement(key="loom.data.locality.v1", value="slave-b", view="systems")
    )
    assert runtime._select_target(package, record) == "slave-b"

    localized = record.committed.snapshot.compute.requirements.pop()
    assert localized.value == "slave-b"
    assert [runtime._select_target(package, record) for _ in range(2)] == ["slave-a", "slave-b"]


@pytest.mark.asyncio
async def test_runtime_reuses_completed_nodes_after_driver_restart():
    repo = ObserverRepository()
    run, _patched = await _dynamic_fixture_for_target_selection(repo)
    record = await repo.get_run(run.run_id)
    package = next(package for package in record.capability_packages if package.package_id == "summarize")
    package_ref = ResourceRef(resource_id=package.version_ref, version_or_digest=package.package_digest)

    class ReplayExecutor:
        async def run(self, program, input_ref, *, read_json, emit_node, result):
            handle = await emit_node(package_ref, [input_ref])
            return await result(handle)

    runtime = DynamicOrchestrationRuntime(
        repository=repo,
        executor=ReplayExecutor(),
        workers={"slave-a": WorkerSession("slave-a", "http://slave-a", transport=httpx.ASGITransport(app=create_slave_app("slave-a")))},
    )
    completed, _result = await runtime.run(run.run_id)
    assert completed.state == "completed"
    first_attempts = list((await repo.get_run(run.run_id)).attempts)

    restarted = await repo.get_run(run.run_id)
    restarted.state = "running"
    restarted.outcome = None
    await repo._persist(restarted)
    replayed, _replay_result = await runtime.run(run.run_id)

    assert replayed.state == "completed"
    assert (await repo.get_run(run.run_id)).attempts == first_attempts


async def _dynamic_fixture_for_target_selection(repo: ObserverRepository):
    input_schema = await _content(repo, {"type": "object"}, "application/schema+json")
    contract = await _content(
        repo,
        {
            "schema_version": "io.v1",
            "input_schema_ref": input_schema.model_dump(mode="json"),
            "output_schema_ref": None,
            "success_semantics": None,
            "success_validator_ref": None,
        },
        "application/vnd.loom.io-contract+json",
    )
    program = await _content(repo, 'async def orchestrate(ctx: "OrchestrationContext", input_ref: "ResourceRef") -> "ResourceRef": return input_ref', "text/x-python")
    node_program = await _content(repo, "print(1)", "text/x-python")
    input_ref = await _content(repo, {"ok": True}, "application/json")
    closure = ClosureContract(
        closure_id="closure-target-selection",
        goal="target selection",
        body=TaskClosure(
            closure_id="closure-target-selection",
            program={"operation_ref": "loom://orchestrate", "io_contract_ref": contract.model_dump(mode="json")},
        ),
    )
    run = await repo.open_run("run-target-selection", "conversation-target-selection", "target selection", closure_contract=closure)
    first = await repo.apply_patch(
        run.run_id,
        run.draft_version,
        run.draft_digest,
        "target-node-package",
        [
            {"kind": "set_execution_payload", "value": {"node_id": "loom://orchestrate", "input_ref": input_ref.model_dump(mode="json")}},
            {
                "kind": "materialize_capability_package_candidate",
                "value": {
                    "package_id": "summarize",
                    "package_version": "v1",
                    "program_content_ref": node_program.model_dump(mode="json"),
                    "io_contract_ref": contract.model_dump(mode="json"),
                    "operation_descriptor_ref": "loom://summarize",
                },
            },
        ],
    )
    node_package = next(
        package
        for package in (await repo.get_run(run.run_id)).capability_packages
        if package.package_id == "summarize"
    )
    package_ref = ResourceRef(resource_id=node_package.version_ref, version_or_digest=node_package.package_digest)
    patched = await repo.apply_patch(
        run.run_id,
        first.draft_version,
        first.draft_digest,
        "target-orchestration-package",
        [
            {
                "kind": "materialize_capability_package_candidate",
                "value": {
                    "package_id": "orchestrate",
                    "package_version": "v1",
                    "program_content_ref": program.model_dump(mode="json"),
                    "io_contract_ref": contract.model_dump(mode="json"),
                    "operation_descriptor_ref": "loom://orchestrate",
                    "executor_kind": "orchestrator_python_v1",
                    "executor_operation": "orchestrate",
                    "allowed_node_package_refs": [package_ref.model_dump(mode="json")],
                    "max_nodes": 2,
                    "max_live_nodes": 2,
                },
            },
        ],
    )
    committed = await repo.commit(run.run_id, patched.draft_version, patched.draft_digest)
    await repo.start(run.run_id, committed.version_id)
    return run, patched
