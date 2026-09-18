from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    service_name: str = "observer"
    database_url: str = "sqlite+aiosqlite:///:memory:"
    # The production/runtime default is the real Codex app-server.  The
    # deterministic Fake provider is selected explicitly by the test profile.
    coding_agent_backend: str = "codex"
    codex_model: str = "deepseek-v4-flash"
    codex_message_limit_bytes: int = 16 * 1024 * 1024
    coding_agent_deadline_seconds: float = 86400.0
    coding_agent_poll_interval_seconds: float = 5.0
    coding_agent_protocol_failure_seconds: float = 60.0
    worker_operation_timeout_seconds: float = 90.0
    observer_forward_timeout_seconds: float = 86400.0
    capability_operation_timeout_seconds: float = 30.0
    capability_health_interval_seconds: float = 5.0
    runtime_plugin_dir: str = "/opt/loom/runtime-plugins"
    package_contract_dir: str = "/opt/loom/package-contracts"
    runtime_plugin_socket_dir: str = "/run/loom/runtime-plugins"
    runtime_plugin_startup_timeout_seconds: float = 10.0
    runtime_plugin_call_timeout_seconds: float = 90.0
    runtime_plugin_network: str = "loom-internal"
    runtime_plugin_memory: str = "512m"
    runtime_plugin_cpus: float = 1.0
    runtime_plugin_pids_limit: int = 128
    runtime_plugin_tmpfs_size: str = "64m"
    runtime_plugin_request_timeout_seconds: float = 30.0
    runtime_plugin_response_max_bytes: int = 4 * 1024 * 1024
    runtime_plugin_max_concurrency: int = 16
    runtime_plugin_docker_timeout_seconds: float = 120.0
    orchestrator_image: str = "python:3.12-slim"
    orchestrator_memory: str = "512m"
    orchestrator_cpus: float = 1.0
    orchestrator_pids_limit: int = 128
    orchestrator_max_program_bytes: int = 1_048_576
    orchestrator_max_message_bytes: int = 4_194_304
    orchestrator_timeout_seconds: float = 300.0
    s3_endpoint_url: str | None = None
    s3_bucket: str = "loom-content"
    s3_access_key: str | None = None
    s3_secret_key: str | None = None
    s3_region: str = "us-east-1"
    s3_prefix: str = ""
    workspace_id: str = "workspace-default"
    workspace_root: str = "/workspace"
    internal_api_secret: str = ""
    observer_url: str = "http://observer:8080"
    agent_id: str = "driver-default"
    agent_instance_id: str | None = None
    agent_protocol_version: str = "loom.v1"
    agent_heartbeat_interval_seconds: float = 5.0
    agent_registration_retry_seconds: float = 2.0
    agent_lease_ttl_seconds: float = 15.0
    driver_url: str | None = None
    slave_endpoint_url: str | None = None
    slave_a_url: str | None = None
    slave_b_url: str | None = None

    model_config = SettingsConfigDict(env_prefix="LOOM_", extra="ignore")
