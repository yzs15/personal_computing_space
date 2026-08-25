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
