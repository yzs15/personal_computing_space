import pytest
from copy import deepcopy
from sqlalchemy.ext.asyncio import create_async_engine
from unittest.mock import patch
from datetime import datetime, timedelta, timezone

from loom_v2.contracts.types import ClosureContract, ComputeRequirement, NodeIntent, ResourceRef, TaskClosure
from loom_v2.contracts.agents import AgentRegistration
from loom_v2.contracts.errors import DomainError
from loom_v2.content_store import canonical_json_bytes
from loom_v2.observer.repository import ObserverRepository


async def _register_slave(repo: ObserverRepository, slave_id: str, instance_id: str):
    return await repo.register_agent(
        AgentRegistration(
            role="slave",
            agent_id=slave_id,
            instance_id=instance_id,
            workspace_id="workspace-default",
            endpoint_url=f"http://{slave_id}",
            protocol_version="loom.v1",
            capabilities={"operations": ["run_code"]},
        )
    )


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


async def _dynamic_run(
    repo: ObserverRepository,
    *,
    max_nodes: int = 2,
    max_live_nodes: int = 1,
    orchestration_source: str | None = None,
    allow_reassignment: bool = False,
    node_replay_safety: str = "DeterministicByEventLog",
    max_attempts: int | None = None,
):
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
        recovery_policy={"allow_reassignment": allow_reassignment},
        resource_budget={"max_attempts": max_attempts} if max_attempts is not None else {},
        body=TaskClosure(
            closure_id="closure-dynamic",
            program={"operation_ref": "loom://orchestrate", "io_contract_ref": parent_contract.model_dump(mode="json")},
        ),
    )
    run = await repo.open_run(
        "run-dynamic",
        "conversation-dynamic",
        "dynamic orchestration",
        allow_reassignment=allow_reassignment,
        closure_contract=closure,
    )
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
                    "replay_safety": node_replay_safety,
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


async def _expire_slave(repo: ObserverRepository, slave_id: str) -> None:
    key = next(
        key
        for key in repo.agents
        if key[1:3] == ("slave", slave_id) and repo.agents[key].get("lease_state") == "active"
    )
    repo.agents[key]["last_seen_at"] = datetime.now(timezone.utc) - timedelta(seconds=60)
    await repo.refresh_slaves("workspace-default")


async def _dispatched_node(
    repo: ObserverRepository,
    *,
    allow_reassignment: bool = True,
    node_replay_safety: str = "NonReplayable",
    max_attempts: int | None = None,
):
    run, patched = await _dynamic_run(
        repo,
        allow_reassignment=allow_reassignment,
        node_replay_safety=node_replay_safety,
        max_attempts=max_attempts,
    )
    committed = await repo.commit(run.run_id, patched.draft_version, patched.draft_digest)
    started = await repo.start(run.run_id, committed.version_id)
    record = await repo.get_run(run.run_id)
    package = next(item for item in record.capability_packages if item.package_id == "summarize")
    input_ref = next(
        item.input_ref
        for item in record.committed.snapshot.node_input_bindings
        if item.node_id == "loom://summarize"
    )
    node = await repo.accept_node_intent(
        run.run_id,
        NodeIntent(
            intent_id="intent-reassign",
            execution_id=started["execution_id"],
            package_ref=ResourceRef(resource_id=package.version_ref, version_or_digest=package.package_digest),
            input_refs=[input_ref],
        ),
        selected_target="slave-a",
    )
    attempt = await repo.dispatch_dynamic_node(run.run_id, node.node_id, target="slave-a")
    return run, started, node, attempt


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
    await _register_slave(repo, "slave-a", "slave-a-instance")
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

    attempt = await repo.dispatch_dynamic_node(run.run_id, node.node_id, target="slave-a")
    attempt_id = attempt["attempt_id"]
    record = await repo.get_run(run.run_id)
    assert record.attempts == [attempt]
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
async def test_dynamic_node_dispatch_binds_active_slave_instance():
    repo = ObserverRepository()
    await _register_slave(repo, "slave-a", "slave-a-instance")
    run, patched = await _dynamic_run(repo)
    committed = await repo.commit(run.run_id, patched.draft_version, patched.draft_digest)
    started = await repo.start(run.run_id, committed.version_id)
    record = await repo.get_run(run.run_id)
    package = next(item for item in record.capability_packages if item.package_id == "summarize")
    input_ref = next(
        item.input_ref
        for item in record.committed.snapshot.node_input_bindings
        if item.node_id == "loom://summarize"
    )
    node = await repo.accept_node_intent(
        run.run_id,
        NodeIntent(
            intent_id="intent-instance-binding",
            execution_id=started["execution_id"],
            package_ref=ResourceRef(resource_id=package.version_ref, version_or_digest=package.package_digest),
            input_refs=[input_ref],
        ),
        selected_target="slave-a",
    )

    attempt = await repo.dispatch_dynamic_node(run.run_id, node.node_id, target="slave-a")

    assert attempt == {
        "attempt_id": attempt["attempt_id"],
        "node_id": node.node_id,
        "target": "slave-a",
        "target_instance_id": "slave-a-instance",
        "target_agent_epoch": 1,
        "state": "created",
        "execution_epoch": 1,
        "replaces_attempt_id": None,
        "replaced_by_attempt_id": None,
    }


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


@pytest.mark.asyncio
async def test_reassign_marks_old_attempt_lost_without_bumping_run_epoch():
    repo = ObserverRepository()
    await _register_slave(repo, "slave-a", "slave-a-instance")
    await _register_slave(repo, "slave-b", "slave-b-instance")
    run, started, node, old_attempt = await _dispatched_node(repo)
    await _expire_slave(repo, "slave-a")

    replacement = await repo.reassign_dynamic_node(
        run.run_id,
        node.node_id,
        lost_attempt_id=old_attempt["attempt_id"],
        expected_execution_id=started["execution_id"],
        expected_execution_epoch=1,
        target="slave-b",
        reason="worker_lease_expired",
    )

    repeated = await repo.reassign_dynamic_node(
        run.run_id,
        node.node_id,
        lost_attempt_id=old_attempt["attempt_id"],
        expected_execution_id=started["execution_id"],
        expected_execution_epoch=1,
        target="slave-b",
        reason="worker_lease_expired",
    )
    record = await repo.get_run(run.run_id)
    assert repeated == replacement
    assert record.execution_epoch == 1
    assert record.attempts[0]["state"] == "lost"
    assert record.attempts[0]["replaced_by_attempt_id"] == replacement["attempt_id"]
    assert replacement["replaces_attempt_id"] == old_attempt["attempt_id"]
    assert replacement["target_instance_id"] == "slave-b-instance"
    assert record.events[-1]["package_replay_safety"] == "NonReplayable"
    assert record.events[-1]["reassignment_authorization"]["value"] is True

    stale_value = {"value": "late"}
    with pytest.raises(ValueError, match="stale_attempt"):
        await repo.record_dynamic_node_result(
            run.run_id,
            node.node_id,
            {
                "attempt_id": old_attempt["attempt_id"],
                "execution_id": started["execution_id"],
                "execution_epoch": 1,
                "value": stale_value,
                "digest": repo._result_digest(stale_value),
            },
        )


@pytest.mark.asyncio
async def test_reassign_rejects_missing_authorization_without_partial_mutation():
    repo = ObserverRepository()
    await _register_slave(repo, "slave-a", "slave-a-instance")
    await _register_slave(repo, "slave-b", "slave-b-instance")
    run, started, node, old_attempt = await _dispatched_node(repo, allow_reassignment=False)
    await _expire_slave(repo, "slave-a")
    before = deepcopy((await repo.get_run(run.run_id)).attempts)

    with pytest.raises(ValueError, match="reassignment_not_allowed"):
        await repo.reassign_dynamic_node(
            run.run_id,
            node.node_id,
            lost_attempt_id=old_attempt["attempt_id"],
            expected_execution_id=started["execution_id"],
            expected_execution_epoch=1,
            target="slave-b",
            reason="worker_lease_expired",
        )

    assert (await repo.get_run(run.run_id)).attempts == before


@pytest.mark.asyncio
async def test_reassign_rejects_active_source_lease_without_partial_mutation():
    repo = ObserverRepository()
    await _register_slave(repo, "slave-a", "slave-a-instance")
    await _register_slave(repo, "slave-b", "slave-b-instance")
    run, started, node, old_attempt = await _dispatched_node(repo)
    before = deepcopy((await repo.get_run(run.run_id)).attempts)

    with pytest.raises(ValueError, match="worker_lease_active"):
        await repo.reassign_dynamic_node(
            run.run_id,
            node.node_id,
            lost_attempt_id=old_attempt["attempt_id"],
            expected_execution_id=started["execution_id"],
            expected_execution_epoch=1,
            target="slave-b",
            reason="worker_lease_expired",
        )

    assert (await repo.get_run(run.run_id)).attempts == before


@pytest.mark.asyncio
async def test_reassign_rejects_exhausted_attempt_budget_without_partial_mutation():
    repo = ObserverRepository()
    await _register_slave(repo, "slave-a", "slave-a-instance")
    await _register_slave(repo, "slave-b", "slave-b-instance")
    run, started, node, old_attempt = await _dispatched_node(repo, max_attempts=1)
    await _expire_slave(repo, "slave-a")
    before = deepcopy((await repo.get_run(run.run_id)).attempts)

    with pytest.raises(ValueError, match="orchestration_attempt_limit_exceeded"):
        await repo.reassign_dynamic_node(
            run.run_id,
            node.node_id,
            lost_attempt_id=old_attempt["attempt_id"],
            expected_execution_id=started["execution_id"],
            expected_execution_epoch=1,
            target="slave-b",
            reason="worker_lease_expired",
        )

    assert (await repo.get_run(run.run_id)).attempts == before


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_kind", ["locality", "input"])
async def test_reassign_rejects_unsatisfied_mechanical_constraints_without_partial_mutation(failure_kind):
    repo = ObserverRepository()
    await _register_slave(repo, "slave-a", "slave-a-instance")
    await _register_slave(repo, "slave-b", "slave-b-instance")
    run, started, node, old_attempt = await _dispatched_node(repo)
    await _expire_slave(repo, "slave-a")
    record = await repo.get_run(run.run_id)
    if failure_kind == "locality":
        record.committed.snapshot.compute.requirements.append(
            ComputeRequirement(key="loom.data.locality.v1", value="slave-a", view="systems")
        )
        expected = "node_target_unavailable"
    else:
        missing_digest = "0" * 64
        node.input_refs = [
            ResourceRef(
                resource_id=f"content://sha256/{missing_digest}",
                version_or_digest=missing_digest,
                identity_criterion="content_digest",
            )
        ]
        expected = "node_input_unavailable"
    before = deepcopy(record.attempts)

    with pytest.raises(ValueError, match=expected):
        await repo.reassign_dynamic_node(
            run.run_id,
            node.node_id,
            lost_attempt_id=old_attempt["attempt_id"],
            expected_execution_id=started["execution_id"],
            expected_execution_epoch=1,
            target="slave-b",
            reason="worker_lease_expired",
        )

    assert (await repo.get_run(run.run_id)).attempts == before


@pytest.mark.asyncio
async def test_reassign_persistence_failure_leaves_original_projection_unchanged():
    repo = ObserverRepository()
    await _register_slave(repo, "slave-a", "slave-a-instance")
    await _register_slave(repo, "slave-b", "slave-b-instance")
    run, started, node, old_attempt = await _dispatched_node(repo)
    await _expire_slave(repo, "slave-a")
    before = deepcopy(await repo.get_run(run.run_id))

    async def fail_persist(_record):
        raise RuntimeError("persistence_failed")

    repo._persist = fail_persist
    with pytest.raises(RuntimeError, match="persistence_failed"):
        await repo.reassign_dynamic_node(
            run.run_id,
            node.node_id,
            lost_attempt_id=old_attempt["attempt_id"],
            expected_execution_id=started["execution_id"],
            expected_execution_epoch=1,
            target="slave-b",
            reason="worker_lease_expired",
        )

    current = await repo.get_run(run.run_id)
    assert current.attempts == before.attempts
    assert current.events == before.events
    assert current.dynamic_nodes == before.dynamic_nodes


def test_dynamic_node_event_replay_keeps_reassigned_node_dispatched():
    package_ref = ResourceRef(resource_id="capability-package://summarize/v1", version_or_digest="a" * 64)
    input_ref = ResourceRef(
        resource_id=f"content://sha256/{'b' * 64}",
        version_or_digest="b" * 64,
        identity_criterion="content_digest",
    )

    nodes = ObserverRepository._dynamic_nodes_from_events(
        [
            {
                "phase": "node_accepted",
                "node_id": "node-1",
                "execution_id": "execution-1",
                "intent_id": "intent-1",
                "package_ref": package_ref.model_dump(mode="json"),
                "package_digest": "a" * 64,
                "input_refs": [input_ref.model_dump(mode="json")],
            },
            {
                "phase": "node_reassigned",
                "node_id": "node-1",
                "lost_attempt_id": "attempt-old",
                "replacement_attempt_id": "attempt-new",
            },
        ]
    )

    assert len(nodes) == 1
    assert nodes[0].state == "dispatched"
