"""Configuration and topology validation for remote Loom deployments."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
import tomllib
from typing import Annotated, Any, Literal, Self
from urllib.parse import quote

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, ValidationError, field_validator, model_validator


class DeploymentConfigError(ValueError):
    """Raised when a deployment file cannot describe a safe topology."""


_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
NonEmptyString = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
Port = Annotated[int, Field(strict=True, ge=1, le=65535)]


class ConfigModel(BaseModel):
    model_config = ConfigDict(frozen=True, str_strip_whitespace=True, hide_input_in_errors=True)


class MachineConfig(ConfigModel):
    name: NonEmptyString
    ssh_host: NonEmptyString
    ssh_port: Port = 22
    ssh_user: NonEmptyString
    role: Literal["minio", "observer", "driver", "slave"]
    service_port: Port
    console_port: Port | None = None
    service_id: Literal["slave-a", "slave-b"] | None = None
    ssh_key: Path | None = None

    @model_validator(mode="before")
    @classmethod
    def port_defaults(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        value = dict(value)
        role = value.get("role")
        if isinstance(role, str):
            role = value["role"] = role.strip().lower()
            default_port = {"minio": 9000, "observer": 8080, "driver": 8090}.get(role, 8081 if value.get("service_id") == "slave-a" else 8082)
            value.setdefault("service_port", default_port)
            if role == "minio":
                value.setdefault("console_port", 9001)
        return value

    @field_validator("name")
    @classmethod
    def valid_name(cls, value: str) -> str:
        if not _NAME_RE.fullmatch(value):
            raise ValueError("name contains unsupported characters")
        return value

    @field_validator("ssh_host", "ssh_user")
    @classmethod
    def ssh_token(cls, value: str, info) -> str:
        if any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in value):
            raise ValueError(f"{info.field_name} must not contain whitespace or control characters")
        return value

    @field_validator("ssh_key")
    @classmethod
    def expand_key(cls, value: Path | None) -> Path | None:
        return value.expanduser() if value is not None else None

    @model_validator(mode="after")
    def role_fields(self) -> Self:
        if self.role == "slave" and self.service_id is None:
            raise ValueError("service_id is required for a slave")
        if self.role != "slave" and self.service_id is not None:
            raise ValueError("service_id is only valid for a slave")
        if self.role != "minio" and self.console_port is not None:
            raise ValueError("console_port is only valid for minio")
        return self

    @property
    def advertised_host(self) -> str:
        return self.ssh_host

    @property
    def endpoint_url(self) -> str:
        host = self.ssh_host
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        return f"http://{host}:{self.service_port}"


class DriverConfig(ConfigModel):
    codex_base_url: NonEmptyString = "http://host.docker.internal:8787"
    workspace_path: NonEmptyString = "/srv/loom/workspace"
    model_catalog_file: Path | None = None
    codex_version: NonEmptyString = "0.151.0"
    codex_model: NonEmptyString = "deepseek-v4-flash"
    codex_provider: NonEmptyString = "proxy"
    codex_wire_api: NonEmptyString = "responses"
    codex_api_key_env: NonEmptyString = "OPENAI_API_KEY"
    codex_api_key_file: Path | None = None

    @field_validator("workspace_path")
    @classmethod
    def absolute_workspace(cls, value: str) -> str:
        if not value.startswith("/"):
            raise ValueError("workspace_path must be absolute")
        return value


class ClusterConfig(ConfigModel):
    name: NonEmptyString
    remote_dir: NonEmptyString
    workspace_id: NonEmptyString
    build_network: Literal["default", "host", "none"] = "default"
    internal_secret_file: NonEmptyString
    postgres_password_file: NonEmptyString
    minio_secret_key_file: NonEmptyString
    minio_access_key: NonEmptyString = "loom"

    @field_validator("name")
    @classmethod
    def valid_name(cls, value: str) -> str:
        if not _NAME_RE.fullmatch(value):
            raise ValueError("cluster.name contains unsupported characters")
        return value

    @field_validator("remote_dir")
    @classmethod
    def absolute_remote_dir(cls, value: str) -> str:
        if not value.startswith("/"):
            raise ValueError("cluster.remote_dir must be absolute")
        return value


class DeploymentInput(ConfigModel):
    cluster: ClusterConfig
    machines: tuple[MachineConfig, ...] = Field(min_length=1)
    driver: DriverConfig = Field(default_factory=DriverConfig)


@dataclass(frozen=True)
class EndpointMap:
    observer: str
    driver: str
    minio: str
    minio_console: str
    slaves: dict[str, str]


@dataclass(frozen=True)
class DeploymentConfig:
    """Fully resolved, validated deployment configuration."""

    config_path: Path
    name: str
    remote_dir: str
    workspace_id: str
    internal_api_secret: str
    postgres_password: str
    minio_access_key: str
    minio_secret_key: str
    codex_api_key: str | None
    machines: tuple[MachineConfig, ...]
    driver: DriverConfig
    secret_paths: tuple[Path, ...]
    build_network: str = "default"

    @classmethod
    def from_file(cls, path: str | Path) -> "DeploymentConfig":
        config_path = Path(path).expanduser().resolve()
        try:
            with config_path.open("rb") as handle:
                raw = tomllib.load(handle)
        except OSError as exc:
            raise DeploymentConfigError(f"cannot read config: {config_path}") from exc
        except tomllib.TOMLDecodeError as exc:
            raise DeploymentConfigError(f"invalid TOML: {exc}") from exc
        if not isinstance(raw, dict):
            raise DeploymentConfigError("config must be a TOML table")
        return cls._from_raw(config_path, raw)

    @classmethod
    def _from_raw(cls, config_path: Path, raw: dict[str, Any]) -> "DeploymentConfig":
        try:
            parsed = DeploymentInput.model_validate(raw)
        except ValidationError as exc:
            error = exc.errors()[0]
            location = ".".join(str(item) for item in error.get("loc", ()))
            message = str(error.get("msg", "invalid deployment configuration"))
            if location:
                message = f"{location} {message}"
            raise DeploymentConfigError(message) from exc
        cluster = parsed.cluster
        name, remote_dir, workspace_id = cluster.name, cluster.remote_dir, cluster.workspace_id
        build_network = cluster.build_network
        base_dir = config_path.parent
        internal_secret_path = _resolve_secret_path(base_dir, cluster.internal_secret_file, "cluster.internal_secret_file")
        postgres_password_path = _resolve_secret_path(base_dir, cluster.postgres_password_file, "cluster.postgres_password_file")
        minio_secret_path = _resolve_secret_path(base_dir, cluster.minio_secret_key_file, "cluster.minio_secret_key_file")
        internal_secret = _read_secret(internal_secret_path, "cluster.internal_secret_file")
        postgres_password = _read_secret(postgres_password_path, "cluster.postgres_password_file")
        minio_secret = _read_secret(minio_secret_path, "cluster.minio_secret_key_file")
        minio_access_key = cluster.minio_access_key
        machines = parsed.machines
        _validate_topology(machines)
        driver = parsed.driver
        codex_api_key_path = driver.codex_api_key_file
        codex_api_key = None
        if codex_api_key_path is not None:
            if not codex_api_key_path.is_absolute():
                codex_api_key_path = (base_dir / codex_api_key_path).resolve()
            else:
                codex_api_key_path = codex_api_key_path.resolve()
            codex_api_key = _read_secret(codex_api_key_path, "driver.codex_api_key_file")
        catalog_path = driver.model_catalog_file
        if catalog_path is not None:
            if not catalog_path.is_absolute():
                catalog_path = (base_dir / catalog_path).resolve()
            else:
                catalog_path = catalog_path.resolve()
            if not catalog_path.is_file():
                raise DeploymentConfigError(f"driver.model_catalog_file does not exist: {catalog_path}")
        driver = driver.model_copy(
            update={
                "model_catalog_file": catalog_path,
                "codex_api_key_file": codex_api_key_path,
            }
        )
        secret_paths = [internal_secret_path, postgres_password_path, minio_secret_path]
        if codex_api_key_path is not None:
            secret_paths.append(codex_api_key_path)
        return cls(
            config_path=config_path,
            name=name,
            remote_dir=remote_dir,
            workspace_id=workspace_id,
            internal_api_secret=internal_secret,
            postgres_password=postgres_password,
            minio_access_key=minio_access_key.strip(),
            minio_secret_key=minio_secret,
            codex_api_key=codex_api_key,
            machines=machines,
            driver=driver,
            secret_paths=tuple(secret_paths),
            build_network=build_network,
        )

    def machine(self, name_or_role: str) -> MachineConfig:
        matches = [item for item in self.machines if item.name == name_or_role or item.role == name_or_role]
        if len(matches) != 1:
            raise DeploymentConfigError(f"machine is not uniquely defined: {name_or_role}")
        return matches[0]

    @property
    def slaves(self) -> tuple[MachineConfig, ...]:
        return tuple(sorted((item for item in self.machines if item.role == "slave"), key=lambda item: item.service_id or ""))

    @property
    def urls(self) -> EndpointMap:
        observer = self.machine("observer")
        driver = self.machine("driver")
        minio = self.machine("minio")
        return EndpointMap(
            observer=observer.endpoint_url,
            driver=driver.endpoint_url,
            minio=minio.endpoint_url,
            minio_console=(
                f"http://{_url_host(minio.advertised_host)}:{minio.console_port}"
                if minio.console_port is not None
                else minio.endpoint_url
            ),
            slaves={item.service_id: item.endpoint_url for item in self.slaves if item.service_id is not None},
        )

    def database_url(self, machine: MachineConfig) -> str:
        if machine.role not in {"observer", "slave"}:
            raise DeploymentConfigError(f"database is not defined for role: {machine.role}")
        if machine.role == "observer":
            db_name = "loom_observer"
        else:
            service_id = machine.service_id
            if service_id is None:  # defensive guard for callers constructing MachineConfig directly
                raise DeploymentConfigError("slave service_id is required for database URL")
            db_name = f"loom_{service_id.replace('-', '_')}"
        return f"postgresql+asyncpg://loom:{quote(self.postgres_password, safe='')}@postgres:5432/{db_name}"


def _resolve_secret_path(base_dir: Path, raw_path: Any, field: str) -> Path:
    if not isinstance(raw_path, str) or not raw_path.strip():
        raise DeploymentConfigError(f"{field} is required")
    path = Path(raw_path).expanduser()
    if not path.is_absolute():
        path = base_dir / path
    return path.resolve()


def _read_secret(path: Path, field: str) -> str:
    try:
        value = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise DeploymentConfigError(f"{field} cannot be read: {path}") from exc
    value = value.strip()
    if not value or "\n" in value or "\r" in value:
        raise DeploymentConfigError(f"{field} must contain one non-empty line")
    return value


def _url_host(host: str) -> str:
    return f"[{host}]" if ":" in host and not host.startswith("[") else host


def _compose_component(value: str) -> str:
    return value.lower().replace("_", "-").replace(".", "-")


def _validate_topology(machines: tuple[MachineConfig, ...]) -> None:
    names = [item.name for item in machines]
    if len(names) != len(set(names)):
        raise DeploymentConfigError("machine names must be unique")
    normalized_names = [_compose_component(name) for name in names]
    if len(normalized_names) != len(set(normalized_names)):
        raise DeploymentConfigError("project names must be unique")
    for role in ("minio", "driver", "observer"):
        if sum(item.role == role for item in machines) != 1:
            raise DeploymentConfigError(f"exactly one {role} machine is required")
    slaves = [item for item in machines if item.role == "slave"]
    if not 1 <= len(slaves) <= 2:
        raise DeploymentConfigError("one or two slave machines are required")
    slave_ids = [item.service_id for item in slaves]
    if len(slave_ids) != len(set(slave_ids)):
        raise DeploymentConfigError("slave service_id values must be unique")

    occupied: dict[tuple[str, int], str] = {}
    for machine in machines:
        ports = [machine.service_port]
        if machine.console_port is not None:
            ports.append(machine.console_port)
        if len(ports) != len(set(ports)):
            raise DeploymentConfigError(f"{machine.name} published ports must be distinct")
        for port in ports:
            key = (machine.ssh_host, port)
            previous = occupied.get(key)
            if previous is not None:
                raise DeploymentConfigError(f"published port {port} on {machine.ssh_host} is used by {previous} and {machine.name}")
            occupied[key] = machine.name
