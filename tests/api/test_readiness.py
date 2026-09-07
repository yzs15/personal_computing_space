import pytest
from fastapi.testclient import TestClient

from loom_v2.contracts.types import ClosureContract, TaskClosure
from loom_v2.contracts.errors import DomainError
from loom_v2.observer.app import create_app
from loom_v2.observer.repository import ObserverRepository


def _binding(operation: str = "run_code", target: str = "slave-a", *, package_ref: dict | None = None) -> dict:
    return {
        "binding_id": f"binding-{operation}",
        "hole_id": "h_run_code",
        "capability_descriptor_ref": {"resource_id": "executor://subprocess_json_v1/1"},
        "capability_package_ref": package_ref,
        "target_resource_ref": {"resource_id": target},
        "realization_digest": f"realization-{target}-{operation}",
        "bound_by": "test-driver",
    }


def _add_run_code_hole(client: TestClient, suffix: str = "default") -> tuple[dict, dict]:
    run = client.post("/api/v1/runs", json={"task_ref": "task-readiness", "goal": "run test code"}).json()
    patch = client.post(
        f"/api/v1/runs/{run['run_id']}/patches",
        json={
            "operation_id": f"run-code-plan-{suffix}",
            "base_draft_version": run["draft_version"],
            "base_snapshot_digest": run["draft_digest"],
            "ops": [
                {"kind": "set_program_ref", "value": "loom://run_code"},
                {"kind": "set_compute_spec", "value": {"operation_ref": "loom://run_code"}},
                {"kind": "add_typed_hole", "value": {"hole_id": "h_run_code"}},
            ],
        },
    ).json()
    return run, patch


def test_unbound_typed_hole_blocks_commit_and_reports_readiness():
    client = TestClient(create_app())
    run, patch = _add_run_code_hole(client, "unbound")

    assert patch["readiness"]["ready"] is False
    assert patch["readiness"]["blockers"] == [{"code": "typed_hole_unbound", "hole_id": "h_run_code"}]
    response = client.post(
        f"/api/v1/runs/{run['run_id']}/commit",
        json={"draft_version": patch["draft_version"], "draft_digest": patch["draft_digest"]},
    )

    assert response.status_code == 409
    detail = response.json()["detail"]
    assert detail["code"] == "readiness_blocked"
    assert detail["details"]["blockers"][0]["code"] == "typed_hole_unbound"


def test_run_code_binding_requires_package_and_then_allows_start():
    repository = ObserverRepository()
    client = TestClient(create_app(repository))
    run, patch = _add_run_code_hole(client, "blocked")
    blocked = client.post(
        f"/api/v1/runs/{run['run_id']}/patches",
        json={
            "operation_id": "run-code-binding-without-package",
            "base_draft_version": patch["draft_version"],
            "base_snapshot_digest": patch["draft_digest"],
            "ops": [{"kind": "bind_compute_hole", "value": _binding()}],
        },
    ).json()
    commit = client.post(
        f"/api/v1/runs/{run['run_id']}/commit",
        json={"draft_version": blocked["draft_version"], "draft_digest": blocked["draft_digest"]},
    )

    assert commit.status_code == 409
    detail = commit.json()["detail"]
    assert detail["code"] == "readiness_blocked"
    assert detail["details"]["blockers"][0]["code"] == "capability_package_required"

    program_ref = client.post(
        "/api/v1/content",
        json={"media_type": "text/x-python", "content": "import json,sys; print(json.dumps(json.load(sys.stdin)))"},
    ).json()
    contract_ref = client.post(
        "/api/v1/content",
        json={
            "media_type": "application/vnd.loom.io-contract+json",
            "content": {
                "schema_version": "io.v1",
                "input_schema_ref": None,
                "output_schema_ref": None,
                "success_semantics": None,
                "success_validator_ref": None,
            },
        },
    ).json()
    run2, patch2 = _add_run_code_hole(client, "valid")
    packaged = client.post(
        f"/api/v1/runs/{run2['run_id']}/patches",
        json={
            "operation_id": "run-code-package",
            "base_draft_version": patch2["draft_version"],
            "base_snapshot_digest": patch2["draft_digest"],
            "ops": [
                {"kind": "set_io_contract_ref", "value": contract_ref},
                {
                    "kind": "materialize_capability_package_candidate",
                    "value": {
                        "package_id": "readiness-run-code",
                        "package_version": "v1",
                        "operation_descriptor_ref": "loom://run_code",
                        "program_content_ref": program_ref,
                        "io_contract_ref": contract_ref,
                        "executor_kind": "subprocess_json_v1",
                        "executor_operation": "run_code",
                    },
                },
            ],
        },
    ).json()
    bound = client.post(
        f"/api/v1/runs/{run2['run_id']}/patches",
        json={
            "operation_id": "run-code-binding-valid",
            "base_draft_version": packaged["draft_version"],
            "base_snapshot_digest": packaged["draft_digest"],
            "ops": [
                {
                    "kind": "bind_compute_hole",
                    "value": {
                        **_binding(
                            package_ref={
                                "resource_id": "capability-package://readiness-run-code/v1",
                            }
                        ),
                        "realization_digest": program_ref["version_or_digest"],
                    },
                }
            ],
        },
    ).json()
    commit2 = client.post(
        f"/api/v1/runs/{run2['run_id']}/commit",
        json={"draft_version": bound["draft_version"], "draft_digest": bound["draft_digest"]},
    )

    assert commit2.status_code == 200
    started = client.post(f"/api/v1/runs/{run2['run_id']}/start", json={"closure_version": commit2.json()["closure_version"]})
    assert started.status_code == 200
    assert client.get(f"/api/v1/runs/{run2['run_id']}/readiness").json()["ready"] is True


@pytest.mark.asyncio
async def test_input_schema_requires_bound_ref_and_rejects_wrapped_payload() -> None:
    repo = ObserverRepository()
    schema_ref = await repo.put_content(
        {"type": "object", "required": ["scores"], "properties": {"scores": {"type": "array"}}},
        media_type="application/schema+json",
    )
    contract_ref = await repo.put_content(
        {
            "schema_version": "io.v1",
            "input_schema_ref": schema_ref.model_dump(mode="json"),
            "output_schema_ref": None,
            "success_semantics": None,
            "success_validator_ref": None,
        },
        media_type="application/vnd.loom.io-contract+json",
    )
    contract = ClosureContract(
        closure_id="closure-input-schema",
        goal="validate scores",
        body=TaskClosure(
            closure_id="closure-input-schema",
            program={"operation_ref": "loom://test_input_validation", "io_contract_ref": contract_ref.model_dump(mode="json")},
        ),
    )
    record = await repo.open_run("run-input-schema", "task-input-schema", "validate scores", closure_contract=contract)

    missing = await repo.inspect_readiness(record.run_id)
    assert missing["ready"] is False
    assert any(item["code"] == "payload_missing" for item in missing["blockers"])

    wrapped_ref = await repo.put_content({"payload": {"scores": [80]}}, media_type="application/json")
    wrapped = await repo.apply_patch(
        record.run_id,
        record.draft_version,
        record.draft_digest,
        "input-wrapped",
        [{"kind": "set_execution_payload", "value": {"node_id": "loom://test_input_validation", "input_ref": wrapped_ref.model_dump(mode="json")}}],
    )
    mismatch = next(item for item in wrapped.readiness["blockers"] if item["code"] == "payload_schema_mismatch")
    assert mismatch["schema_digest"] == schema_ref.version_or_digest
    assert mismatch["errors"][0]["keyword"] == "required"

    input_ref = await repo.put_content({"scores": [80]}, media_type="application/json")
    valid = await repo.apply_patch(
        record.run_id,
        wrapped.draft_version,
        wrapped.draft_digest,
        "input-valid",
        [{"kind": "set_execution_payload", "value": {"node_id": "loom://test_input_validation", "input_ref": input_ref.model_dump(mode="json")}}],
    )
    assert not any(item["code"].startswith("payload_") for item in valid.readiness["blockers"])


@pytest.mark.asyncio
async def test_commit_and_start_reuse_input_readiness_blockers() -> None:
    repo = ObserverRepository()
    schema_ref = await repo.put_content({"type": "object", "required": ["value"]}, media_type="application/schema+json")
    contract_ref = await repo.put_content(
        {
            "schema_version": "io.v1",
            "input_schema_ref": schema_ref.model_dump(mode="json"),
            "output_schema_ref": None,
            "success_semantics": None,
            "success_validator_ref": None,
        },
        media_type="application/vnd.loom.io-contract+json",
    )
    contract = ClosureContract(
        closure_id="closure-commit-input",
        goal="validate input",
        body=TaskClosure(
            closure_id="closure-commit-input",
            program={"operation_ref": "loom://test_input_validation", "io_contract_ref": contract_ref.model_dump(mode="json")},
        ),
    )
    record = await repo.open_run("run-commit-input", "task-commit-input", "validate input", closure_contract=contract)
    await repo.begin_refinement(record.run_id)

    with pytest.raises(DomainError) as caught:
        await repo.commit(record.run_id, record.draft_version, record.draft_digest)
    assert caught.value.envelope.code == "readiness_blocked"
    assert caught.value.envelope.details["blockers"][0]["code"] == "payload_missing"


@pytest.mark.asyncio
async def test_program_inline_io_fields_are_not_an_executable_contract() -> None:
    repo = ObserverRepository()
    contract_ref = await repo.put_content(
        {
            "schema_version": "io.v1",
            "input_schema_ref": None,
            "output_schema_ref": None,
            "success_semantics": None,
            "success_validator_ref": None,
        },
        media_type="application/vnd.loom.io-contract+json",
    )
    contract = ClosureContract(
        closure_id="closure-inline-schema",
        goal="validate input",
        body=TaskClosure(
            closure_id="closure-inline-schema",
            program={
                "operation_ref": "loom://test_input_validation",
                "io_contract_ref": contract_ref.model_dump(mode="json"),
                "input_schema": {"type": "object"},
                "output_schema": {"type": "object"},
                "success_semantics": {"criterion": "validated"},
            },
        ),
    )
    record = await repo.open_run("run-inline-schema", "task-inline-schema", "validate input", closure_contract=contract)

    readiness = await repo.inspect_readiness(record.run_id)

    assert readiness["ready"] is False
    blocker = next(item for item in readiness["blockers"] if item["code"] == "inline_io_contract_forbidden")
    assert blocker == {
        "code": "inline_io_contract_forbidden",
        "fields": ["input_schema", "output_schema", "success_semantics"],
    }
