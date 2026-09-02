from loom_v2.driver.bootstrap import bootstrap_from_environment


def test_bootstrap_allows_proxy_without_codex_api_key(tmp_path, monkeypatch):
    internal_secret = tmp_path / "internal_api_secret"
    internal_secret.write_text("internal-secret", encoding="utf-8")
    monkeypatch.setenv("LOOM_CODEX_API_KEY_FILE", str(tmp_path / "missing_codex_key"))
    monkeypatch.setenv("LOOM_INTERNAL_API_SECRET_FILE", str(internal_secret))
    monkeypatch.setenv("LOOM_CODEX_BASE_URL", "http://host.docker.internal:8787")
    monkeypatch.setenv("LOOM_CODEX_PROVIDER", "proxy")
    monkeypatch.setenv("LOOM_CODEX_MODEL_CATALOG_JSON", "/var/lib/loom/codex/model_catalog.json")
    monkeypatch.setenv("CODEX_HOME", str(tmp_path / "codex"))

    codex_key, internal_key, config_path = bootstrap_from_environment()

    assert codex_key is None
    assert internal_key == "internal-secret"
    config_text = config_path.read_text(encoding="utf-8")
    assert 'model_provider = "proxy"' in config_text
    assert 'model_reasoning_effort = "xhigh"' in config_text
    assert 'approvals_reviewer = "guardian_subagent"' in config_text
    assert 'sandbox_mode = "danger-full-access"' in config_text
    assert 'model_catalog_json = "/var/lib/loom/codex/model_catalog.json"' in config_text
    assert 'base_url = "http://host.docker.internal:8787"' in config_text
    assert "env_key" not in config_text
    assert 'wire_api = "responses"' in config_text
