from fastapi.testclient import TestClient

from loom_v2.observer.app import create_app


def test_runtime_reports_codex_backend_and_model(monkeypatch):
    monkeypatch.setenv("LOOM_CODING_AGENT_BACKEND", "codex")
    monkeypatch.setenv("LOOM_CODEX_MODEL", "deepseek-v4-flash")

    response = TestClient(create_app()).get("/api/v1/runtime")

    assert response.status_code == 200
    assert response.json() == {
        "coding_agent_backend": "codex",
        "coding_agent_label": "Codex app-server",
        "model": "deepseek-v4-flash",
    }


def test_runtime_identifies_fake_backend_for_test_profile(monkeypatch):
    monkeypatch.setenv("LOOM_CODING_AGENT_BACKEND", "fake")

    response = TestClient(create_app()).get("/api/v1/runtime")

    assert response.status_code == 200
    assert response.json() == {
        "coding_agent_backend": "fake",
        "coding_agent_label": "Fake coding agent",
        "model": None,
    }


def test_runtime_reports_driver_unavailable_in_split_deployment(monkeypatch):
    monkeypatch.setenv("LOOM_INTERNAL_API_SECRET", "test-internal-secret")

    response = TestClient(create_app()).get("/api/v1/runtime")

    assert response.status_code == 503
    assert response.json() == {"code": "driver_unavailable", "retryable": True}
