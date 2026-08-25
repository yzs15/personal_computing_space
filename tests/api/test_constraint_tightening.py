from fastapi.testclient import TestClient

from loom_v2.observer.app import create_app


def test_relaxed_constraint_patch_is_rejected_without_polluting_draft():
    client = TestClient(create_app())
    run = client.post("/api/v1/runs", json={"task_ref": "task-constraint", "goal": "echo"}).json()
    first = client.post(
        f"/api/v1/runs/{run['run_id']}/patches",
        json={
            "operation_id": "constraint-1",
            "base_draft_version": run["draft_version"],
            "base_snapshot_digest": run["draft_digest"],
            "ops": [{"kind": "add_constraint", "value": {"subject": ["ComputeSpec"], "predicate": {"op": "le", "field": "cpu_seconds", "value": 60}, "source": "Requester", "fate": "preserve"}}],
        },
    ).json()
    response = client.post(
        f"/api/v1/runs/{run['run_id']}/patches",
        json={
            "operation_id": "constraint-2",
            "base_draft_version": first["draft_version"],
            "base_snapshot_digest": first["draft_digest"],
            "ops": [{"kind": "tighten_constraint", "value": {"parent": {"op": "le", "field": "cpu_seconds", "value": 60}, "child": {"op": "le", "field": "cpu_seconds", "value": 90}}}],
        },
    )
    assert response.status_code == 422
    state = client.get(f"/api/v1/runs/{run['run_id']}").json()
    assert state["draft_version"] == first["draft_version"]
