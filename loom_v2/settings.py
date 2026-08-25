from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    service_name: str = "observer"
    database_url: str = "sqlite+aiosqlite:///:memory:"
    coding_agent_backend: str = "fake"
    codex_model: str = "deepseek-v4-flash"
    workspace_id: str = "workspace-default"
    workspace_root: str = "/workspace"

    model_config = SettingsConfigDict(env_prefix="LOOM_", extra="ignore")
