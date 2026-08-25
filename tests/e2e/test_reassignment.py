from fastapi.testclient import TestClient

from loom_v2.observer.app import create_app


def test_slave_a_loss_creates_new_attempt_without_new_execution():
    client = TestClient(create_app())
    run = client.post("/api/v1/runs", json={"task_ref": "task-1", "goal": "echo", "allow_reassignment": True}).json()
    committed = client.post(f"/api/v1/runs/{run['run_id']}/commit", json={"draft_version": run["draft_version"], "draft_digest": run["draft_digest"]}).json()
    started = client.post(f"/api/v1/runs/{run['run_id']}/start", json={"closure_version": committed["closure_version"]}).json()
    client.post("/api/v1/slaves/slave-a/availability", json={"available": False})
    client.post(f"/api/v1/runs/{run['run_id']}/reconcile", json={})
    state = client.get(f"/api/v1/runs/{run['run_id']}").json()
    assert state["execution_id"] == started["execution_id"]
    assert [attempt["target"] for attempt in state["attempts"]] == ["slave-a", "slave-b"]
