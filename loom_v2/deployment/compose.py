"""Deterministic Docker Compose documents for one deployment machine."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

from .config import DeploymentConfig, MachineConfig


@dataclass(frozen=True)
class RenderedProject:
    """Files and preview text produced for one machine's Compose project."""

    machine: str
    project_name: str
    compose_text: str
    env_text: str
    secret_files: dict[str, str]
    model_catalog_file: Path | None

    @property
    def redacted_preview(self) -> str:
        preview = f"{self.compose_text}\n{self.env_text}"
        for secret in self.secret_files.values():
            preview = preview.replace(secret, "<redacted>")
        return preview


def render_project(config: DeploymentConfig, machine_name: str) -> RenderedProject:
    machine = config.machine(machine_name)
    project_name = _project_name(config.name, machine.name)
    if machine.role == "observer":
        document, env = _render_observer(config, machine)
    elif machine.role == "slave":
        document, env = _render_slave(config, machine)
    elif machine.role == "driver":
        document, env = _render_driver(config, machine)
    elif machine.role == "minio":
        document, env = _render_minio(config, machine)
    else:  # pragma: no cover - config validation prevents this
        raise ValueError(f"unsupported role: {machine.role}")
    return RenderedProject(
        machine=machine.name,
        project_name=project_name,
        compose_text=json.dumps(document, indent=2, sort_keys=True) + "\n",
        env_text=_env_text(env),
        secret_files={
            "internal_api_secret": config.internal_api_secret,
            "postgres_password": config.postgres_password,
            "minio_secret_key": config.minio_secret_key,
        },
        model_catalog_file=config.driver.model_catalog_file if machine.role == "driver" else None,
    )


def _base_service(config: DeploymentConfig, machine: MachineConfig, *, dockerfile: str) -> dict[str, Any]:
    return {
        "build": {"context": "./source", "dockerfile": dockerfile},
        "image": f"{_project_name(config.name, machine.name)}-{machine.role}",
        "restart": "unless-stopped",
    }


def _postgres_service(volume_name: str, database: str) -> dict[str, Any]:
    return {
        "image": "postgres:16-alpine",
        "restart": "unless-stopped",
        "environment": {
            "POSTGRES_DB": database,
            "POSTGRES_PASSWORD": "${POSTGRES_PASSWORD}",
            "POSTGRES_USER": "loom",
        },
        "healthcheck": {
            "test": ["CMD-SHELL", f"pg_isready -U loom -d {database}"],
            "interval": "2s",
            "timeout": "3s",
            "retries": 30,
        },
        "volumes": [f"{volume_name}:/var/lib/postgresql/data"],
    }


def _role_environment(config: DeploymentConfig, machine: MachineConfig, database_url: str) -> dict[str, str]:
    return {
        "LOOM_DATABASE_URL": database_url.replace(quote_password(config.postgres_password), "${POSTGRES_PASSWORD}"),
        "LOOM_S3_ACCESS_KEY": "${MINIO_ACCESS_KEY}",
        "LOOM_S3_BUCKET": "${LOOM_S3_BUCKET}",
        "LOOM_S3_ENDPOINT_URL": "${LOOM_S3_ENDPOINT_URL}",
        "LOOM_S3_REGION": "us-east-1",
        "LOOM_S3_SECRET_KEY": "${MINIO_SECRET_KEY}",
        "LOOM_SERVICE_NAME": machine.service_id or machine.role,
        "LOOM_WORKSPACE_ID": config.workspace_id,
    }


def _render_observer(config: DeploymentConfig, machine: MachineConfig) -> tuple[dict[str, Any], dict[str, str]]:
    database = "loom_observer"
    app = _base_service(config, machine, dockerfile="Dockerfile")
    app.update(
        {
            "command": [
                "/bin/sh",
                "-c",
                'export LOOM_INTERNAL_API_SECRET="$$(cat /run/secrets/internal_api_secret)" && exec uvicorn loom_v2.observer.app:app --host 0.0.0.0 --port 8080',
            ],
            "environment": _role_environment(config, machine, config.database_url(machine)),
            "ports": [f"{machine.service_port}:8080"],
            "secrets": ["internal_api_secret"],
            "depends_on": {"postgres": {"condition": "service_healthy"}},
        }
    )
    document = {
        "services": {"observer": app, "postgres": _postgres_service(f"{machine.name}-postgres-data", database)},
        "secrets": {"internal_api_secret": {"file": "./secrets/internal_api_secret"}},
        "volumes": {f"{machine.name}-postgres-data": {}},
    }
    env = _common_env(config, database_url=config.database_url(machine))
    return document, env


def _render_slave(config: DeploymentConfig, machine: MachineConfig) -> tuple[dict[str, Any], dict[str, str]]:
    assert machine.service_id is not None
    database = f"loom_{machine.service_id.replace('-', '_')}"
    app = _base_service(config, machine, dockerfile="Dockerfile")
    app.update(
        {
            "command": [
                "/bin/sh",
                "-c",
                'export LOOM_INTERNAL_API_SECRET="$$(cat /run/secrets/internal_api_secret)" && exec uvicorn loom_v2.slave.app:create_app --factory --host 0.0.0.0 --port 8080',
            ],
            "environment": {
                **_role_environment(config, machine, config.database_url(machine)),
                "LOOM_OBSERVER_URL": config.urls.observer,
                "LOOM_SLAVE_ENDPOINT_URL": machine.endpoint_url,
            },
            "ports": [f"{machine.service_port}:8080"],
            "secrets": ["internal_api_secret"],
            "depends_on": {"postgres": {"condition": "service_healthy"}},
        }
    )
    document = {
        "services": {"slave": app, "postgres": _postgres_service(f"{machine.name}-postgres-data", database)},
        "secrets": {"internal_api_secret": {"file": "./secrets/internal_api_secret"}},
        "volumes": {f"{machine.name}-postgres-data": {}},
    }
    env = _common_env(config, database_url=config.database_url(machine))
    env["LOOM_OBSERVER_URL"] = config.urls.observer
    return document, env


def _render_driver(config: DeploymentConfig, machine: MachineConfig) -> tuple[dict[str, Any], dict[str, str]]:
    app = _base_service(config, machine, dockerfile="Dockerfile.driver")
    environment = {
        "LOOM_AGENT_ID": "driver-default",
        "LOOM_CODEX_API_KEY_ENV": "OPENAI_API_KEY",
        "LOOM_CODEX_BASE_URL": config.driver.codex_base_url,
        "LOOM_CODEX_MODEL": config.driver.codex_model,
        "LOOM_CODEX_MODEL_REASONING_EFFORT": "xhigh",
        "LOOM_CODEX_PROVIDER": config.driver.codex_provider,
        "LOOM_CODEX_SANDBOX_MODE": "danger-full-access",
        "LOOM_CODEX_WIRE_API": config.driver.codex_wire_api,
        "LOOM_DRIVER_URL": machine.endpoint_url,
        "LOOM_OBSERVER_URL": config.urls.observer,
        "LOOM_S3_ACCESS_KEY": "${MINIO_ACCESS_KEY}",
        "LOOM_S3_BUCKET": "${LOOM_S3_BUCKET}",
        "LOOM_S3_ENDPOINT_URL": "${LOOM_S3_ENDPOINT_URL}",
        "LOOM_S3_REGION": "us-east-1",
        "LOOM_S3_SECRET_KEY": "${MINIO_SECRET_KEY}",
        "LOOM_SERVICE_NAME": "driver",
        "LOOM_WORKSPACE_ID": config.workspace_id,
        "LOOM_WORKSPACE_ROOT": "/workspace",
    }
    if "slave-a" in config.urls.slaves:
        environment["LOOM_SLAVE_A_URL"] = config.urls.slaves["slave-a"]
    if "slave-b" in config.urls.slaves:
        environment["LOOM_SLAVE_B_URL"] = config.urls.slaves["slave-b"]
    if config.driver.model_catalog_file is not None:
        environment["LOOM_CODEX_MODEL_CATALOG_JSON"] = "/var/lib/loom/codex/model_catalog.json"
    app.update(
        {
            "environment": environment,
            "ports": [f"{machine.service_port}:8090"],
            "secrets": ["internal_api_secret"],
            "volumes": [
                f"{config.driver.workspace_path}:/workspace",
                "driver-codex-state:/var/lib/loom/codex",
                "/var/run/docker.sock:/var/run/docker.sock",
            ],
        }
    )
    if config.driver.model_catalog_file is not None:
        app["volumes"].insert(2, "./model_catalog.json:/var/lib/loom/codex/model_catalog.json:ro")
    document = {
        "services": {"driver": app},
        "secrets": {"internal_api_secret": {"file": "./secrets/internal_api_secret"}},
        "volumes": {"driver-codex-state": {}},
    }
    env = {
        "MINIO_ACCESS_KEY": config.minio_access_key,
        "MINIO_SECRET_KEY": config.minio_secret_key,
        "LOOM_S3_BUCKET": "loom-content",
        "LOOM_S3_ENDPOINT_URL": config.urls.minio,
    }
    return document, env


def _render_minio(config: DeploymentConfig, machine: MachineConfig) -> tuple[dict[str, Any], dict[str, str]]:
    assert machine.console_port is not None
    minio = {
        "image": "minio/minio:latest",
        "restart": "unless-stopped",
        "command": ["server", "/data", "--console-address", ":9001"],
        "environment": {
            "MINIO_ROOT_PASSWORD": "${MINIO_SECRET_KEY}",
            "MINIO_ROOT_USER": "${MINIO_ACCESS_KEY}",
        },
        "ports": [f"{machine.service_port}:9000", f"{machine.console_port}:9001"],
        "volumes": [f"{machine.name}-minio-data:/data"],
        "healthcheck": {
            "test": ["CMD-SHELL", "curl -f http://localhost:9000/minio/health/live || exit 1"],
            "interval": "2s",
            "timeout": "3s",
            "retries": 30,
        },
    }
    init = {
        "image": "minio/mc:latest",
        "depends_on": {"minio": {"condition": "service_healthy"}},
        "environment": {
            "MINIO_ACCESS_KEY": "${MINIO_ACCESS_KEY}",
            "MINIO_SECRET_KEY": "${MINIO_SECRET_KEY}",
            "LOOM_S3_BUCKET": "${LOOM_S3_BUCKET}",
        },
        "entrypoint": ["/bin/sh", "-c"],
        "command": 'mc alias set loom http://minio:9000 "$${MINIO_ACCESS_KEY}" "$${MINIO_SECRET_KEY}" && mc mb --ignore-existing "loom/$${LOOM_S3_BUCKET}"',
        "restart": "no",
    }
    document = {
        "services": {"minio": minio, "minio-init": init},
        "volumes": {f"{machine.name}-minio-data": {}},
    }
    env = {
        "MINIO_ACCESS_KEY": config.minio_access_key,
        "MINIO_SECRET_KEY": config.minio_secret_key,
        "LOOM_S3_BUCKET": "loom-content",
    }
    return document, env


def _common_env(config: DeploymentConfig, *, database_url: str) -> dict[str, str]:
    return {
        "LOOM_DATABASE_URL": database_url,
        "MINIO_ACCESS_KEY": config.minio_access_key,
        "MINIO_SECRET_KEY": config.minio_secret_key,
        "LOOM_S3_BUCKET": "loom-content",
        "LOOM_S3_ENDPOINT_URL": config.urls.minio,
    }


def _env_text(values: dict[str, str]) -> str:
    return "".join(f"{key}={_dotenv_quote(value)}\n" for key, value in sorted(values.items()))


def _dotenv_quote(value: str) -> str:
    return "'" + value.replace("'", "\\'") + "'"


def quote_password(password: str) -> str:
    """Return the URL-encoded representation used in database URLs."""

    from urllib.parse import quote

    return quote(password, safe="")


def _project_name(cluster: str, machine: str) -> str:
    return f"{cluster}-{machine}".lower().replace("_", "-")
