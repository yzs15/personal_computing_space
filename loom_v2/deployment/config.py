"""Configuration and topology validation for remote Loom deployments."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
import tomllib
from typing import Any
from urllib.parse import quote


class DeploymentConfigError(ValueError):
    """Raised when a deployment file cannot describe a safe topology."""


_ROLES = {"minio", "observer", "driver", "slave"}
_SLAVE_IDS = {"slave-a", "slave-b"}
_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")


@dataclass(frozen=True)
class MachineConfig:
    name: str
    ssh_host: str
    ssh_port: int
    ssh_user: str
    role: str
    service_port: int
    console_port: int | None = None
    service_id: str | None = None
    ssh_key: Path | None = None

    @property
    def advertised_host(self) -> str:
        return self.ssh_host

    @property
    def endpoint_url(self) -> str:
        host = self.ssh_host
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        return f"http://{host}:{self.service_port}"


@dataclass(frozen=True)
class DriverConfig:
    codex_base_url: str = "http://host.docker.internal:8787"
    workspace_path: str = "/srv/loom/workspace"
    model_catalog_file: Path | None = None
    codex_version: str = "0.151.0"
    codex_model: str = "deepseek-v4-flash"
    codex_provider: str = "proxy"
    codex_wire_api: str = "responses"


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
    machines: tuple[MachineConfig, ...]
    driver: DriverConfig
    secret_paths: tuple[Path, ...]

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
        cluster = raw.get("cluster")
        if not isinstance(cluster, dict):
            raise DeploymentConfigError("cluster table is required")

        name = _required_string(cluster, "name")
        if not _NAME_RE.fullmatch(name):
            raise DeploymentConfigError("cluster.name contains unsupported characters")
        remote_dir = _required_string(cluster, "remote_dir")
        if not remote_dir.startswith("/"):
            raise DeploymentConfigError("cluster.remote_dir must be absolute")
        workspace_id = _required_string(cluster, "workspace_id")
        base_dir = config_path.parent
        internal_secret_path = _resolve_secret_path(base_dir, cluster.get("internal_secret_file"), "cluster.internal_secret_file")
        postgres_password_path = _resolve_secret_path(base_dir, cluster.get("postgres_password_file"), "cluster.postgres_password_file")
        minio_secret_path = _resolve_secret_path(base_dir, cluster.get("minio_secret_key_file"), "cluster.minio_secret_key_file")
        internal_secret = _read_secret(internal_secret_path, "cluster.internal_secret_file")
        postgres_password = _read_secret(postgres_password_path, "cluster.postgres_password_file")
        minio_secret = _read_secret(minio_secret_path, "cluster.minio_secret_key_file")
        minio_access_key = cluster.get("minio_access_key", "loom")
        if not isinstance(minio_access_key, str) or not minio_access_key.strip():
            raise DeploymentConfigError("cluster.minio_access_key must be non-empty")

        machines_raw = raw.get("machines")
        if not isinstance(machines_raw, list) or not machines_raw:
            raise DeploymentConfigError("machines must contain at least one entry")
        machines = tuple(_parse_machine(item) for item in machines_raw)
        _validate_topology(machines)

        driver_raw = raw.get("driver", {})
        if not isinstance(driver_raw, dict):
            raise DeploymentConfigError("driver must be a table")
        catalog = driver_raw.get("model_catalog_file")
        catalog_path: Path | None = None
        if catalog is not None:
            if not isinstance(catalog, str) or not catalog.strip():
                raise DeploymentConfigError("driver.model_catalog_file must be a path")
            catalog_path = Path(catalog).expanduser()
            if not catalog_path.is_absolute():
                catalog_path = (base_dir / catalog_path).resolve()
            else:
                catalog_path = catalog_path.resolve()
            if not catalog_path.is_file():
                raise DeploymentConfigError(f"driver.model_catalog_file does not exist: {catalog_path}")
        driver = DriverConfig(
            codex_base_url=_optional_string(driver_raw, "codex_base_url", DriverConfig.codex_base_url),
            workspace_path=_optional_string(driver_raw, "workspace_path", DriverConfig.workspace_path),
            model_catalog_file=catalog_path,
            codex_version=_optional_string(driver_raw, "codex_version", DriverConfig.codex_version),
            codex_model=_optional_string(driver_raw, "codex_model", DriverConfig.codex_model),
            codex_provider=_optional_string(driver_raw, "codex_provider", DriverConfig.codex_provider),
            codex_wire_api=_optional_string(driver_raw, "codex_wire_api", DriverConfig.codex_wire_api),
        )
        if not driver.workspace_path.startswith("/"):
            raise DeploymentConfigError("driver.workspace_path must be absolute")
        return cls(
            config_path=config_path,
            name=name,
            remote_dir=remote_dir,
            workspace_id=workspace_id,
            internal_api_secret=internal_secret,
            postgres_password=postgres_password,
            minio_access_key=minio_access_key.strip(),
            minio_secret_key=minio_secret,
            machines=machines,
            driver=driver,
            secret_paths=(internal_secret_path, postgres_password_path, minio_secret_path),
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


def _required_string(table: dict[str, Any], field: str) -> str:
    value = table.get(field)
    if not isinstance(value, str) or not value.strip():
        raise DeploymentConfigError(f"{field} is required and must be non-empty")
    return value.strip()


def _optional_string(table: dict[str, Any], field: str, default: str) -> str:
    value = table.get(field, default)
    if not isinstance(value, str) or not value.strip():
        raise DeploymentConfigError(f"driver.{field} must be non-empty")
    return value.strip()


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


def _parse_machine(raw: Any) -> MachineConfig:
    if not isinstance(raw, dict):
        raise DeploymentConfigError("each machines entry must be a table")
    name = _required_string(raw, "name")
    if not _NAME_RE.fullmatch(name):
        raise DeploymentConfigError(f"machines.{name}.name contains unsupported characters")
    host = _required_string(raw, "ssh_host")
    user = _required_string(raw, "ssh_user")
    if any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in host):
        raise DeploymentConfigError(f"machines.{name}.ssh_host must not contain whitespace or control characters")
    if any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in user):
        raise DeploymentConfigError(f"machines.{name}.ssh_user must not contain whitespace or control characters")
    role = _required_string(raw, "role").lower()
    if role not in _ROLES:
        raise DeploymentConfigError(f"machines.{name}.role must be one of {sorted(_ROLES)}")
    ssh_port = _port(raw.get("ssh_port", 22), f"machines.{name}.ssh_port")
    defaults = {"minio": 9000, "observer": 8080, "driver": 8090}
    service_id = raw.get("service_id")
    if role == "slave":
        if not isinstance(service_id, str) or not service_id.strip():
            raise DeploymentConfigError(f"machines.{name}.service_id is required for a slave")
        service_id = service_id.strip()
        if service_id not in _SLAVE_IDS:
            raise DeploymentConfigError(f"machines.{name}.service_id must be slave-a or slave-b")
        default_port = 8081 if service_id == "slave-a" else 8082
    else:
        if service_id is not None:
            raise DeploymentConfigError(f"machines.{name}.service_id is only valid for a slave")
        default_port = defaults[role]
    service_port = _port(raw.get("service_port", default_port), f"machines.{name}.service_port")
    console_port: int | None = None
    if role == "minio":
        console_port = _port(raw.get("console_port", 9001), f"machines.{name}.console_port")
    elif raw.get("console_port") is not None:
        raise DeploymentConfigError(f"machines.{name}.console_port is only valid for minio")
    key = raw.get("ssh_key")
    ssh_key: Path | None = None
    if key is not None:
        if not isinstance(key, str) or not key.strip():
            raise DeploymentConfigError(f"machines.{name}.ssh_key must be a path")
        ssh_key = Path(key).expanduser()
    return MachineConfig(name, host, ssh_port, user, role, service_port, console_port, service_id, ssh_key)


def _port(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 65535:
        raise DeploymentConfigError(f"{field} must be an integer between 1 and 65535")
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
