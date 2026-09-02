import hashlib
import json

from fastapi.testclient import TestClient
import pytest

from loom_v2.contracts.types import ClosureContract, TaskClosure
from loom_v2.content_store import canonical_json_bytes
from loom_v2.observer.app import create_app
from loom_v2.observer.repository import ObserverRepository


def test_draft_patch_commit_and_start_are_explicit():
    client = TestClient(create_app())
    run = client.post("/api/v1/runs", json={"task_ref": "task-1", "goal": "echo"}).json()
    patch = client.post(
        f"/api/v1/runs/{run['run_id']}/patches",
        json={
            "operation_id": "op-1",
            "base_draft_version": run["draft_version"],
            "base_snapshot_digest": run["draft_digest"],
            "ops": [{"kind": "set_result_expectation", "value": {"kind": "content"}}],
        },
    ).json()
    assert patch["kind"] == "draft"
    assert client.post(f"/api/v1/runs/{run['run_id']}/start", json={"closure_version": "draft"}).status_code == 409
    assert client.post(
        f"/api/v1/runs/{run['run_id']}/commit",
        json={"draft_version": patch["draft_version"], "draft_digest": patch["draft_digest"]},
    ).status_code == 200


def test_open_run_persists_complete_closure_contract():
    client = TestClient(create_app())
    contract = {
        "closure_id": "closure-sort",
        "goal": "sort numbers",
        "required_success_criteria": [{"criterion_id": "sorted", "kind": "deterministic"}],
        "allowed_effects": ["read_workspace"],
        "resource_budget": {"max_node_concurrency": 1, "max_attempts": 2},
        "recovery_policy": {"allow_reassignment": True, "retry_on": ["timeout"]},
        "result_expectations": [{"kind": "content", "identity_criterion": "content_digest"}],
        "declared_constraints": [],
        "body": {
            "closure_id": "closure-sort",
            "program": {"operation_ref": "loom://sort"},
            "compute": {"operation_ref": "loom://sort"},
        },
    }
    response = client.post(
        "/api/v1/runs",
        json={"task_ref": "conversation-contract", "closure_contract": contract},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["goal"] == "sort numbers"
    assert payload["closure_contract"]["required_success_criteria"][0]["criterion_id"] == "sorted"
    assert payload["closure_contract"]["resource_budget"]["max_attempts"] == 2


def test_duplicate_patch_returns_same_receipt():
    client = TestClient(create_app())
    run = client.post("/api/v1/runs", json={"task_ref": "task-2", "goal": "echo"}).json()
    body = {
        "operation_id": "same-op",
        "base_draft_version": run["draft_version"],
        "base_snapshot_digest": run["draft_digest"],
        "ops": [],
    }
    first = client.post(f"/api/v1/runs/{run['run_id']}/patches", json=body).json()
    second = client.post(f"/api/v1/runs/{run['run_id']}/patches", json=body).json()
    assert second["receipt"] == first["receipt"]


@pytest.mark.asyncio
async def test_observer_record_result_projects_schema_failure_to_awaiting_decision():
    repo = ObserverRepository()
    schema_ref = await repo.put_content({"type": "object", "required": ["expected"]}, media_type="application/schema+json")
    contract_ref = await repo.put_content(
        {
            "schema_version": "io.v1",
            "input_schema_ref": None,
            "output_schema_ref": schema_ref.model_dump(mode="json"),
            "success_semantics": None,
            "success_validator_ref": None,
        },
        media_type="application/vnd.loom.io-contract+json",
    )
    contract = ClosureContract(
        closure_id="closure-record-validation",
        goal="echo",
        body=TaskClosure(
            closure_id="closure-record-validation",
            program={"operation_ref": "loom://echo", "io_contract_ref": contract_ref.model_dump(mode="json")},
        ),
    )
    record = await repo.open_run("run-record-validation", "conversation-record-validation", "echo", closure_contract=contract)
    await repo.begin_refinement(record.run_id)
    committed = await repo.commit(record.run_id, record.draft_version, record.draft_digest)
    await repo.start(record.run_id, committed.version_id)
    attempt_id = (await repo.get_run(record.run_id)).attempts[0]["attempt_id"]
    value = {"wrong": True}
    digest = hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()

    updated = await repo.record_result(
        record.run_id,
        {
            "attempt_id": attempt_id,
            "execution_id": record.execution_id,
            "execution_epoch": 1,
            "resource_ref": {"resource_id": f"result-{digest[:16]}", "version_or_digest": digest, "identity_criterion": "content_digest"},
            "digest": digest,
            "value": value,
        },
    )

    assert updated.state == "awaiting_decision"
    assert updated.outcome["error"]["code"] == "output_schema_mismatch"
    assert updated.outcome["resource_ref"] is None
    assert any(event["phase"] == "io_schema_rejected" for event in updated.events)


@pytest.mark.asyncio
async def test_observer_recomputes_output_digest_and_resource_ref_identity():
    repo = ObserverRepository()
    record = await repo.open_run("run-digest-validation", "conversation-digest-validation", "echo")
    await repo.begin_refinement(record.run_id)
    committed = await repo.commit(record.run_id, record.draft_version, record.draft_digest)
    await repo.start(record.run_id, committed.version_id)
    attempt_id = (await repo.get_run(record.run_id)).attempts[0]["attempt_id"]

    with pytest.raises(ValueError, match="output_digest_mismatch"):
        await repo.record_result(
            record.run_id,
            {
                "attempt_id": attempt_id,
                "execution_id": record.execution_id,
                "execution_epoch": 1,
                "resource_ref": {"resource_id": "result-forged", "version_or_digest": "f" * 64},
                "digest": "f" * 64,
                "value": {"text": "hello"},
            },
        )


@pytest.mark.asyncio
async def test_observer_requires_execution_id_on_terminal_report():
    repo = ObserverRepository()
    record = await repo.open_run("run-execution-id", "conversation-execution-id", "echo")
    await repo.begin_refinement(record.run_id)
    committed = await repo.commit(record.run_id, record.draft_version, record.draft_digest)
    await repo.start(record.run_id, committed.version_id)
    attempt_id = (await repo.get_run(record.run_id)).attempts[0]["attempt_id"]

    with pytest.raises(ValueError, match="stale_execution_id"):
        await repo.record_result(
            record.run_id,
            {"attempt_id": attempt_id, "execution_epoch": 1, "value": {"text": "hello"}, "digest": "d"},
        )


@pytest.mark.asyncio
async def test_observer_rejects_stale_attempt_and_epoch_terminal_reports():
    repo = ObserverRepository()
    record = await repo.open_run("run-stale-terminal", "conversation-stale-terminal", "echo")
    await repo.begin_refinement(record.run_id)
    committed = await repo.commit(record.run_id, record.draft_version, record.draft_digest)
    await repo.start(record.run_id, committed.version_id)
    attempt_id = (await repo.get_run(record.run_id)).attempts[0]["attempt_id"]

    with pytest.raises(ValueError, match="stale_attempt"):
        await repo.record_result(
            record.run_id,
            {"attempt_id": "other-attempt", "execution_id": record.execution_id, "execution_epoch": 1, "value": {"text": "hello"}, "digest": "d"},
        )
    with pytest.raises(ValueError, match="stale_execution_epoch"):
        await repo.record_result(
            record.run_id,
            {"attempt_id": attempt_id, "execution_id": record.execution_id, "execution_epoch": 2, "value": {"text": "hello"}, "digest": "d"},
        )


@pytest.mark.asyncio
async def test_observer_reassignment_fences_old_attempt_epoch():
    repo = ObserverRepository()
    record = await repo.open_run("run-reassignment-fencing", "conversation-reassignment-fencing", "echo", allow_reassignment=True)
    await repo.begin_refinement(record.run_id)
    committed = await repo.commit(record.run_id, record.draft_version, record.draft_digest)
    await repo.start(record.run_id, committed.version_id)
    old_attempt = (await repo.get_run(record.run_id)).attempts[0]["attempt_id"]
    await repo.set_slave_availability("slave-a", False)
    updated = await repo.reconcile(record.run_id)

    assert updated.execution_epoch == 2
    assert updated.attempts[-1]["execution_epoch"] == 2
    with pytest.raises(ValueError, match="stale_execution_epoch"):
        await repo.record_result(
            record.run_id,
            {
                "attempt_id": old_attempt,
                "execution_id": record.execution_id,
                "execution_epoch": 1,
                "value": {"text": "hello"},
                "digest": "d",
            },
        )


@pytest.mark.asyncio
async def test_observer_reexecutes_validator_instead_of_trusting_slave_evidence():
    repo = ObserverRepository()
    validator_ref = await repo.put_content(
        b'import json; print(json.dumps({"result": "fail", "errors": [{"path": "$", "keyword": "criterion", "message": "not sorted"}]}))',
        media_type="text/x-python",
    )
    contract_ref = await repo.put_content(
        {
            "schema_version": "io.v1",
            "input_schema_ref": None,
            "output_schema_ref": None,
            "success_semantics": {"criterion": "sorted"},
            "success_validator_ref": validator_ref.model_dump(mode="json"),
        },
        media_type="application/vnd.loom.io-contract+json",
    )
    contract = ClosureContract(
        closure_id="closure-validator-authority",
        goal="echo",
        body=TaskClosure(
            closure_id="closure-validator-authority",
            program={"operation_ref": "loom://echo", "io_contract_ref": contract_ref.model_dump(mode="json")},
        ),
    )
    record = await repo.open_run("run-validator-authority", "conversation-validator-authority", "echo", closure_contract=contract)
    await repo.begin_refinement(record.run_id)
    committed = await repo.commit(record.run_id, record.draft_version, record.draft_digest)
    await repo.start(record.run_id, committed.version_id)
    current = await repo.get_run(record.run_id)
    attempt_id = current.attempts[0]["attempt_id"]
    value = {"text": "hello"}
    digest = hashlib.sha256(canonical_json_bytes(value)).hexdigest()

    updated = await repo.record_result(
        record.run_id,
        {
            "attempt_id": attempt_id,
            "execution_id": current.execution_id,
            "execution_epoch": current.execution_epoch,
            "resource_ref": {"resource_id": f"result-{digest[:16]}", "version_or_digest": digest, "identity_criterion": "content_digest"},
            "digest": digest,
            "value": value,
            "validation_evidence": [
                {
                    "evidence_id": "slave-forged-pass",
                    "attempt_id": attempt_id,
                    "execution_epoch": current.execution_epoch,
                    "validator_ref": validator_ref.model_dump(mode="json"),
                    "result": "pass",
                    "issuer": "slave",
                    "created_at": "2026-08-29T00:00:00+00:00",
                }
            ],
        },
    )

    assert updated.state == "awaiting_decision"
    assert updated.outcome["terminal_error"]["code"] == "success_validation_failed"
    assert any(item.get("issuer") == "observer" and item.get("result") == "fail" for item in updated.outcome["validation_evidence"])
