from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    service_name: str = "observer"
    database_url: str = "sqlite+aiosqlite:///:memory:"
    # The production/runtime default is the real Codex app-server.  The
    # deterministic Fake provider is selected explicitly by the test profile.
    coding_agent_backend: str = "codex"
    codex_model: str = "deepseek-v4-flash"
    coding_agent_deadline_seconds: float = 86400.0
    coding_agent_poll_interval_seconds: float = 5.0
    coding_agent_protocol_failure_seconds: float = 60.0
    worker_operation_timeout_seconds: float = 90.0
    capability_operation_timeout_seconds: float = 30.0
    s3_endpoint_url: str | None = None
    s3_bucket: str = "loom-content"
    s3_access_key: str | None = None
    s3_secret_key: str | None = None
    s3_region: str = "us-east-1"
    s3_prefix: str = ""
    workspace_id: str = "workspace-default"
    workspace_root: str = "/workspace"
    slave_a_url: str | None = None
    slave_b_url: str | None = None

    model_config = SettingsConfigDict(env_prefix="LOOM_", extra="ignore")
