import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from unittest.mock import patch

from loom_v2.contracts.types import ClosureContract, NodeIntent, ResourceRef, TaskClosure
from loom_v2.contracts.errors import DomainError
from loom_v2.content_store import canonical_json_bytes
from loom_v2.observer.repository import ObserverRepository


async def _schema_ref(repo: ObserverRepository, schema: dict):
    return await repo.put_content(schema, media_type="application/schema+json")


async def _contract_ref(repo: ObserverRepository, input_schema_ref: ResourceRef | None):
    return await repo.put_content(
        {
            "schema_version": "io.v1",
            "input_schema_ref": input_schema_ref.model_dump(mode="json") if input_schema_ref else None,
            "output_schema_ref": None,
            "success_semantics": None,
            "success_validator_ref": None,
        },
        media_type="application/vnd.loom.io-contract+json",
    )


async def _dynamic_run(repo: ObserverRepository, *, max_nodes: int = 2, max_live_nodes: int = 1, orchestration_source: str | None = None):
    parent_schema = await _schema_ref(repo, {"type": "object", "required": ["partitions"]})
    parent_contract = await _contract_ref(repo, parent_schema)
    node_schema = await _schema_ref(repo, {"type": "object", "required": ["items"], "properties": {"items": {"type": "array"}}})
    node_contract = await _contract_ref(repo, node_schema)
    node_program = await repo.put_content("print('summarize')", media_type="text/x-python")
    orchestration_program = await repo.put_content(
        orchestration_source or 'async def orchestrate(ctx: "OrchestrationContext", input_ref: "ResourceRef") -> "ResourceRef":\n    return input_ref\n',
        media_type="text/x-python",
    )
    input_ref = await repo.put_content({"partitions": []}, media_type="application/json")
    node_input_ref = await repo.put_content({"items": [1, 2, 3]}, media_type="application/json")

    closure = ClosureContract(
        closure_id="closure-dynamic",
        goal="dynamic orchestration",
        body=TaskClosure(
            closure_id="closure-dynamic",
            program={"operation_ref": "loom://orchestrate", "io_contract_ref": parent_contract.model_dump(mode="json")},
        ),
    )
    run = await repo.open_run("run-dynamic", "conversation-dynamic", "dynamic orchestration", closure_contract=closure)
    first_patch = await repo.apply_patch(
        run.run_id,
        run.draft_version,
        run.draft_digest,
        "dynamic-node-package",
        [
            {"kind": "set_execution_payload", "value": {"node_id": "loom://orchestrate", "input_ref": input_ref.model_dump(mode="json")}},
            {"kind": "set_execution_payload", "value": {"node_id": "loom://summarize", "input_ref": node_input_ref.model_dump(mode="json")}},
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
    patched = await repo.apply_patch(
        run.run_id,
        first_patch.draft_version,
        first_patch.draft_digest,
        "dynamic-orchestration-package",
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
                    "allowed_node_package_refs": [
                        ResourceRef(
                            resource_id=node_package.version_ref,
                            version_or_digest=node_package.package_digest,
                        ).model_dump(mode="json")
                    ],
                    "max_nodes": max_nodes,
                    "max_live_nodes": max_live_nodes,
                },
            },
        ],
    )
    return run, patched


@pytest.mark.asyncio
async def test_orchestration_package_is_bound_and_parent_run_is_ready():
    repo = ObserverRepository()
    run, patched = await _dynamic_run(repo)

    assert patched.readiness["ready"] is True
    record = await repo.get_run(run.run_id)
    orchestration_package = next(package for package in record.capability_packages if package.executor_kind == "orchestrator_python_v1")
    assert record.draft.snapshot.program_systems.package_ref == ResourceRef(
        resource_id=orchestration_package.version_ref,
        version_or_digest=orchestration_package.package_digest,
    )


@pytest.mark.asyncio
async def test_orchestration_readiness_returns_precise_pyright_blocker_without_starting():
    repo = ObserverRepository()
    run, patched = await _dynamic_run(
        repo,
        orchestration_source='async def orchestrate(ctx: "OrchestrationContext", input_ref: "ResourceRef") -> "ResourceRef":\n    return null\n',
    )

    blocker = next(item for item in patched.readiness["blockers"] if item["code"] == "orchestration_program_unresolved_name")
    diagnostic = blocker["diagnostics"][0]
    assert diagnostic["file"] == "orchestration.py"
    assert diagnostic["rule"] == "reportUndefinedVariable"
    assert diagnostic["line"] == 2
    assert diagnostic["column"] == 12

    with pytest.raises(DomainError) as caught:
        await repo.commit(run.run_id, patched.draft_version, patched.draft_digest)
    assert caught.value.envelope.code == "readiness_blocked"
    assert (await repo.get_run(run.run_id)).execution_id is None


@pytest.mark.asyncio
async def test_parent_readiness_reports_missing_docker_runtime():
    repo = ObserverRepository()
    run, patched = await _dynamic_run(repo)
    with patch("loom_v2.observer.repository.shutil.which", return_value=None):
        readiness = await repo.inspect_readiness(run.run_id, patched.draft_version)
    assert readiness["ready"] is False
    assert any(item["code"] == "orchestrator_runtime_unavailable" for item in readiness["blockers"])


@pytest.mark.asyncio
async def test_accept_node_intent_materializes_dynamic_node():
    repo = ObserverRepository()
    run, patched = await _dynamic_run(repo)
    committed = await repo.commit(run.run_id, patched.draft_version, patched.draft_digest)
    started = await repo.start(run.run_id, committed.version_id)
    record = await repo.get_run(run.run_id)
    assert record.attempts == []
    node_package = next(package for package in record.capability_packages if package.package_id == "summarize")
    input_ref = next(binding.input_ref for binding in record.draft.snapshot.node_input_bindings if binding.node_id == "loom://summarize")
    intent = NodeIntent(
        intent_id="intent-1",
        execution_id=started["execution_id"],
        package_ref=ResourceRef(resource_id=node_package.version_ref, version_or_digest=node_package.package_digest),
        input_refs=[input_ref],
    )

    node = await repo.accept_node_intent(run.run_id, intent, selected_target="slave-a")

    assert node.node_id.startswith("node-")
    assert node.parent_execution_ref == started["execution_id"]
    assert node.intent_id == "intent-1"
    assert node.package_digest == node_package.package_digest
    assert node.state == "accepted"
    record = await repo.get_run(run.run_id)
    assert record.dynamic_nodes == [node]
    assert [event["phase"] for event in record.events[-2:]] == ["node_requested", "node_accepted"]
    assert record.events[-1]["selected_target"] == "slave-a"


@pytest.mark.asyncio
async def test_accept_node_intent_rejects_unauthorized_package_and_live_limit():
    repo = ObserverRepository()
    run, patched = await _dynamic_run(repo)
    committed = await repo.commit(run.run_id, patched.draft_version, patched.draft_digest)
    started = await repo.start(run.run_id, committed.version_id)
    record = await repo.get_run(run.run_id)
    node_package = next(package for package in record.capability_packages if package.package_id == "summarize")
    orchestration_package = next(package for package in record.capability_packages if package.executor_kind == "orchestrator_python_v1")
    input_ref = next(binding.input_ref for binding in record.draft.snapshot.node_input_bindings if binding.node_id == "loom://summarize")
    package_ref = ResourceRef(resource_id=node_package.version_ref, version_or_digest=node_package.package_digest)
    intent = NodeIntent(
        intent_id="intent-1",
        execution_id=started["execution_id"],
        package_ref=package_ref,
        input_refs=[input_ref],
    )

    orchestration_package.allowed_node_package_refs = []
    with pytest.raises(ValueError, match="node_package_not_allowed"):
        await repo.accept_node_intent(run.run_id, intent, selected_target="slave-a")

    orchestration_package.allowed_node_package_refs = [package_ref]
    orchestration_package.allowed_node_package_refs = [package_ref]
    accepted = await repo.accept_node_intent(run.run_id, intent, selected_target="slave-a")
    second_intent = intent.model_copy(update={"intent_id": "intent-2"})

    with pytest.raises(ValueError, match="orchestration_live_node_limit_exceeded"):
        await repo.accept_node_intent(run.run_id, second_intent, selected_target="slave-b")

    assert accepted.state == "accepted"


@pytest.mark.asyncio
async def test_accept_node_intent_validates_input_schema():
    repo = ObserverRepository()
    run, patched = await _dynamic_run(repo)
    committed = await repo.commit(run.run_id, patched.draft_version, patched.draft_digest)
    started = await repo.start(run.run_id, committed.version_id)
    record = await repo.get_run(run.run_id)
    node_package = next(package for package in record.capability_packages if package.package_id == "summarize")
    orchestration_package = next(package for package in record.capability_packages if package.executor_kind == "orchestrator_python_v1")
    package_ref = ResourceRef(resource_id=node_package.version_ref, version_or_digest=node_package.package_digest)
    orchestration_package.allowed_node_package_refs = [package_ref]
    invalid_input = await repo.put_content({"wrong": True}, media_type="application/json")
    intent = NodeIntent(
        intent_id="intent-invalid",
        execution_id=started["execution_id"],
        package_ref=package_ref,
        input_refs=[invalid_input],
    )

    with pytest.raises(ValueError, match="node_input_schema_mismatch"):
        await repo.accept_node_intent(run.run_id, intent, selected_target="slave-a")


@pytest.mark.asyncio
async def test_dynamic_nodes_persist_across_repository_restart():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    repo = ObserverRepository(engine)
    await repo.init_db()
    run, patched = await _dynamic_run(repo)
    committed = await repo.commit(run.run_id, patched.draft_version, patched.draft_digest)
    started = await repo.start(run.run_id, committed.version_id)
    record = await repo.get_run(run.run_id)
    node_package = next(package for package in record.capability_packages if package.package_id == "summarize")
    input_ref = next(binding.input_ref for binding in record.draft.snapshot.node_input_bindings if binding.node_id == "loom://summarize")
    intent = NodeIntent(
        intent_id="intent-persist",
        execution_id=started["execution_id"],
        package_ref=ResourceRef(resource_id=node_package.version_ref, version_or_digest=node_package.package_digest),
        input_refs=[input_ref],
    )
    node = await repo.accept_node_intent(run.run_id, intent, selected_target="slave-a")

    restored_repo = ObserverRepository(engine)
    await restored_repo.init_db()
    restored = await restored_repo.get_run(run.run_id)

    assert restored.dynamic_nodes == [node]
    assert restored.events[-1]["phase"] == "node_accepted"


@pytest.mark.asyncio
async def test_dynamic_node_dispatch_result_and_orchestration_completion():
    repo = ObserverRepository()
    run, patched = await _dynamic_run(repo)
    committed = await repo.commit(run.run_id, patched.draft_version, patched.draft_digest)
    started = await repo.start(run.run_id, committed.version_id)
    record = await repo.get_run(run.run_id)
    node_package = next(package for package in record.capability_packages if package.package_id == "summarize")
    input_ref = next(binding.input_ref for binding in record.draft.snapshot.node_input_bindings if binding.node_id == "loom://summarize")
    intent = NodeIntent(
        intent_id="intent-exec",
        execution_id=started["execution_id"],
        package_ref=ResourceRef(resource_id=node_package.version_ref, version_or_digest=node_package.package_digest),
        input_refs=[input_ref],
    )
    node = await repo.accept_node_intent(run.run_id, intent, selected_target="slave-a")

    attempt_id = await repo.dispatch_dynamic_node(run.run_id, node.node_id, target="slave-a")
    record = await repo.get_run(run.run_id)
    assert record.attempts == [
        {
            "attempt_id": attempt_id,
            "node_id": node.node_id,
            "target": "slave-a",
            "state": "created",
            "execution_epoch": 1,
        }
    ]
    assert record.events[-1]["phase"] == "node_dispatched"

    value = {"count": 3, "sum": 6, "min": 1, "max": 3, "sumsq": 14}
    completed_node = await repo.record_dynamic_node_result(
        run.run_id,
        node.node_id,
        {
            "attempt_id": attempt_id,
            "execution_id": started["execution_id"],
            "execution_epoch": 1,
            "value": value,
            "digest": repo._result_digest(value),
        },
    )
    record = await repo.get_run(run.run_id)
    assert completed_node.state == "completed"
    assert record.dynamic_nodes[0].state == "completed"
    assert record.attempts[0]["state"] == "completed"
    assert record.events[-1]["phase"] == "node_completed"
    result_ref = record.events[-1]["result_ref"]
    assert result_ref["resource_id"].startswith("content://sha256/")
    assert await repo.content_store.get(ResourceRef.model_validate(result_ref)) == canonical_json_bytes(value)

    completed_run = await repo.complete_orchestration(run.run_id, ResourceRef.model_validate(result_ref))
    assert completed_run.state == "completed"
    assert completed_run.outcome["resource_ref"] == result_ref
    assert completed_run.events[-1]["phase"] == "orchestration_completed"


@pytest.mark.asyncio
async def test_dynamic_node_dispatch_rejects_target_change_after_acceptance():
    repo = ObserverRepository()
    run, patched = await _dynamic_run(repo)
    committed = await repo.commit(run.run_id, patched.draft_version, patched.draft_digest)
    started = await repo.start(run.run_id, committed.version_id)
    record = await repo.get_run(run.run_id)
    node_package = next(package for package in record.capability_packages if package.package_id == "summarize")
    input_ref = next(binding.input_ref for binding in record.draft.snapshot.node_input_bindings if binding.node_id == "loom://summarize")
    node = await repo.accept_node_intent(
        run.run_id,
        NodeIntent(
            intent_id="intent-target-fence",
            execution_id=started["execution_id"],
            package_ref=ResourceRef(resource_id=node_package.version_ref, version_or_digest=node_package.package_digest),
            input_refs=[input_ref],
        ),
        selected_target="slave-a",
    )
    with pytest.raises(ValueError, match="node_target_mismatch"):
        await repo.dispatch_dynamic_node(run.run_id, node.node_id, target="slave-b")
