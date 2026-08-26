from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    service_name: str = "observer"
    database_url: str = "sqlite+aiosqlite:///:memory:"
    # The production/runtime default is the real Codex app-server.  The
    # deterministic Fake provider is selected explicitly by the test profile.
    coding_agent_backend: str = "codex"
    codex_model: str = "deepseek-v4-flash"
    workspace_id: str = "workspace-default"
    workspace_root: str = "/workspace"
    slave_a_url: str | None = None
    slave_b_url: str | None = None

    model_config = SettingsConfigDict(env_prefix="LOOM_", extra="ignore")
