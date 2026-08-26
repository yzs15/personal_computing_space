from fastapi.testclient import TestClient

from loom_v2.observer.app import create_app


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
