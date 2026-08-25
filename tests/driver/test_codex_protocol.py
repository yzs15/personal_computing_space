from loom_v2.coding_agents.codex import CodexAppServerProvider


def test_codex_provider_defaults_to_requested_model():
    assert CodexAppServerProvider().model == "deepseek-v4-flash"
