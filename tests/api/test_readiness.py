from fastapi.testclient import TestClient

from loom_v2.observer.app import create_app
from loom_v2.observer.repository import ObserverRepository


def _binding(operation: str = "sort", target: str = "slave-a") -> dict:
    return {
        "binding_id": f"binding-{operation}",
        "hole_id": "h_sort",
        "capability_descriptor_ref": {"resource_id": f"capability://{target}/{operation}"},
        "target_resource_ref": {"resource_id": target},
        "realization_digest": f"realization-{target}-{operation}",
        "bound_by": "test-driver",
    }


def _add_sort_hole(client: TestClient, suffix: str = "default") -> tuple[dict, dict]:
    run = client.post("/api/v1/runs", json={"task_ref": "task-readiness", "goal": "sort"}).json()
    patch = client.post(
        f"/api/v1/runs/{run['run_id']}/patches",
        json={
            "operation_id": f"sort-plan-{suffix}",
            "base_draft_version": run["draft_version"],
            "base_snapshot_digest": run["draft_digest"],
            "ops": [
                {"kind": "set_program_ref", "value": "loom://sort"},
                {"kind": "set_compute_spec", "value": {"operation_ref": "loom://sort"}},
                {"kind": "add_typed_hole", "value": {"hole_id": "h_sort"}},
            ],
        },
    ).json()
    return run, patch


def test_unbound_typed_hole_blocks_commit_and_reports_readiness():
    client = TestClient(create_app())
    run, patch = _add_sort_hole(client, "unbound")

    assert patch["readiness"]["ready"] is False
    assert patch["readiness"]["blockers"] == [{"code": "typed_hole_unbound", "hole_id": "h_sort"}]
    response = client.post(
        f"/api/v1/runs/{run['run_id']}/commit",
        json={"draft_version": patch["draft_version"], "draft_digest": patch["draft_digest"]},
    )

    assert response.status_code == 409
    assert "typed_hole_unbound" in response.json()["detail"]


def test_binding_requires_target_capability_and_then_allows_start():
    repository = ObserverRepository()
    repository.slave_capabilities["slave-a"]["operations"] = {"echo", "hash"}
    client = TestClient(create_app(repository))
    run, patch = _add_sort_hole(client, "blocked")
    blocked = client.post(
        f"/api/v1/runs/{run['run_id']}/patches",
        json={
            "operation_id": "sort-binding",
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
    assert "capability_unavailable" in commit.json()["detail"]

    repository.slave_capabilities["slave-a"]["operations"] = {"echo", "hash", "sort"}
    run2, patch2 = _add_sort_hole(client, "valid")
    bound = client.post(
        f"/api/v1/runs/{run2['run_id']}/patches",
        json={
            "operation_id": "sort-binding-valid",
            "base_draft_version": patch2["draft_version"],
            "base_snapshot_digest": patch2["draft_digest"],
            "ops": [{"kind": "bind_compute_hole", "value": _binding("sort")}],
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
