from __future__ import annotations

import os
from pathlib import Path


def read_secret(path: str | os.PathLike[str]) -> str:
    value = Path(path).read_text(encoding="utf-8").strip()
    if not value:
        raise RuntimeError(f"empty_secret:{path}")
    return value


def read_optional_secret(path: str | os.PathLike[str]) -> str | None:
    secret_path = Path(path)
    if not secret_path.is_file():
        return None
    return read_secret(secret_path)


def write_codex_config(
    codex_home: str | os.PathLike[str],
    *,
    model: str = "deepseek-v4-flash",
    base_url: str = "http://host.docker.internal:8787",
    provider: str = "proxy",
    codex_api_key: str | None = None,
    model_reasoning_effort: str = "xhigh",
    approvals_reviewer: str = "guardian_subagent",
    sandbox_mode: str = "danger-full-access",
    model_catalog_json: str | None = None,
    wire_api: str = "responses",
    api_key_env: str = "OPENAI_API_KEY",
) -> Path:
    home = Path(codex_home)
    home.mkdir(parents=True, exist_ok=True)
    config = home / "config.toml"
    catalog_line = f'model_catalog_json = "{model_catalog_json}"\n' if model_catalog_json else ""
    env_key_line = f'env_key = "{api_key_env}"\n' if codex_api_key else ""
    config.write_text(
        f'model_provider = "{provider}"\nmodel = "{model}"\nmodel_reasoning_effort = "{model_reasoning_effort}"\napprovals_reviewer = "{approvals_reviewer}"\nsandbox_mode = "{sandbox_mode}"\n{catalog_line}\n[model_providers.{provider}]\nname = "{provider}"\nbase_url = "{base_url}"\n{env_key_line}wire_api = "{wire_api}"\n',
        encoding="utf-8",
    )
    config.chmod(0o600)
    return config


def bootstrap_from_environment() -> tuple[str | None, str, Path]:
    codex_key = read_optional_secret(os.getenv("LOOM_CODEX_API_KEY_FILE", "/run/secrets/codex_api_key"))
    internal_key = read_secret(os.getenv("LOOM_INTERNAL_API_SECRET_FILE", "/run/secrets/internal_api_secret"))
    config = write_codex_config(
        os.getenv("CODEX_HOME", "/var/lib/loom/codex"),
        model=os.getenv("LOOM_CODEX_MODEL", "deepseek-v4-flash"),
        base_url=os.getenv("LOOM_CODEX_BASE_URL", "http://host.docker.internal:8787"),
        provider=os.getenv("LOOM_CODEX_PROVIDER", "proxy"),
        codex_api_key=codex_key,
        model_reasoning_effort=os.getenv("LOOM_CODEX_MODEL_REASONING_EFFORT", "xhigh"),
        approvals_reviewer=os.getenv("LOOM_CODEX_APPROVALS_REVIEWER", "guardian_subagent"),
        sandbox_mode=os.getenv("LOOM_CODEX_SANDBOX_MODE", "danger-full-access"),
        model_catalog_json=os.getenv("LOOM_CODEX_MODEL_CATALOG_JSON") or None,
        wire_api=os.getenv("LOOM_CODEX_WIRE_API", "responses"),
        api_key_env=os.getenv("LOOM_CODEX_API_KEY_ENV", "OPENAI_API_KEY"),
    )
    return codex_key, internal_key, config
