"""SSH transport and remote Compose lifecycle for deployment projects."""

from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
import json
import shlex
from pathlib import Path
import subprocess
import tarfile
import time
from typing import Callable, Protocol, Sequence
from urllib.request import urlopen

from .compose import RenderedProject, render_project
from .config import DeploymentConfig, MachineConfig


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str = ""
    stderr: str = ""


@dataclass(frozen=True)
class RecordedCall:
    command: str
    input_bytes: bytes | None


class CommandRunner(Protocol):
    def run(self, argv: Sequence[str], *, input_bytes: bytes | None = None, timeout: float | None = None) -> CommandResult:
        ...


class SubprocessRunner:
    def run(self, argv: Sequence[str], *, input_bytes: bytes | None = None, timeout: float | None = None) -> CommandResult:
        try:
            result = subprocess.run(
                list(argv),
                input=input_bytes,
                capture_output=True,
                timeout=timeout,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            return CommandResult(124, exc.stdout.decode(errors="replace") if isinstance(exc.stdout, bytes) else str(exc.stdout or ""), "command timed out")
        except OSError as exc:
            return CommandResult(127, stderr=str(exc))
        return CommandResult(
            result.returncode,
            result.stdout.decode(errors="replace"),
            result.stderr.decode(errors="replace"),
        )


class RecordingRunner:
    """A deterministic command runner used by unit tests and dry-run callers."""

    def __init__(self, *, fail_at: int | None = None, stderr: str = "") -> None:
        self.fail_at = fail_at
        self.failure_stderr = stderr
        self.calls: list[RecordedCall] = []

    def run(self, argv: Sequence[str], *, input_bytes: bytes | None = None, timeout: float | None = None) -> CommandResult:
        command = " ".join(shlex.quote(str(item)) for item in argv)
        self.calls.append(RecordedCall(command, input_bytes))
        if self.fail_at is not None and len(self.calls) == self.fail_at:
            return CommandResult(1, stderr=self.failure_stderr)
        return CommandResult(0)


@dataclass(frozen=True)
class DeploymentPlan:
    machine: str
    role: str
    project_name: str
    preview: str
    commands: tuple[str, ...]


class DeploymentFailure(RuntimeError):
    def __init__(
        self,
        *,
        machine: str,
        role: str,
        phase: str,
        command: str,
        returncode: int | None = None,
        stderr: str = "",
        diagnostics: str = "",
    ) -> None:
        self.machine = machine
        self.role = role
        self.phase = phase
        self.command = command
        self.returncode = returncode
        self.stderr = stderr
        self.diagnostics = diagnostics
        self.args = (self._message(),)

    def _message(self) -> str:
        status = f" (exit {self.returncode})" if self.returncode is not None else ""
        detail = f": {self.stderr.strip()}" if self.stderr.strip() else ""
        command = f" command={self.command}" if self.command else ""
        diagnostics = f" diagnostics={self.diagnostics}" if self.diagnostics else ""
        return f"{self.machine} ({self.role}) {self.phase} failed{status}{command}{detail}{diagnostics}"

    def __str__(self) -> str:
        return self._message()


Healthcheck = Callable[[str], bool]


class RemoteDeployer:
    def __init__(
        self,
        config: DeploymentConfig,
        *,
        source_root: str | Path,
        runner: CommandRunner | None = None,
        healthcheck: Healthcheck | None = None,
        command_timeout: float = 60.0,
        health_timeout: float = 120.0,
        health_interval: float = 2.0,
    ) -> None:
        self.config = config
        self.source_root = Path(source_root).resolve()
        self.runner = runner or SubprocessRunner()
        self.healthcheck = healthcheck or _http_healthcheck
        self.command_timeout = command_timeout
        self.health_timeout = health_timeout
        self.health_interval = health_interval

    def deploy(self, machine_name: str, *, dry_run: bool = False) -> DeploymentPlan:
        machine = self.config.machine(machine_name)
        project = render_project(self.config, machine.name)
        remote_dir = f"{self.config.remote_dir.rstrip('/')}/{machine.name}"
        commands = self._commands(machine, project, remote_dir)
        plan = DeploymentPlan(machine.name, machine.role, project.project_name, project.redacted_preview, tuple(commands))
        if dry_run:
            return DeploymentPlan(
                plan.machine,
                plan.role,
                plan.project_name,
                plan.preview + "\n" + "\n".join(commands),
                plan.commands,
            )

        archive = _make_archive(self.source_root, project, self.config)
        compose = _compose_command(remote_dir, project.project_name)
        self._run_checked(machine, "prepare", commands[0], [*self._ssh_args(machine), _mkdir_command(remote_dir)], input_bytes=None)
        self._run_checked(machine, "upload", commands[1], [*self._ssh_args(machine), _extract_command(remote_dir)], input_bytes=archive)
        self._run_checked(machine, "build", commands[2], [*self._ssh_args(machine), compose + " build"], input_bytes=None)
        start_command = compose + (" up -d --remove-orphans minio" if machine.role == "minio" else " up -d --remove-orphans")
        self._run_checked(machine, "start", commands[3], [*self._ssh_args(machine), start_command], input_bytes=None)
        if machine.role == "minio":
            self._run_checked(machine, "minio-init", commands[4], [*self._ssh_args(machine), compose + " run --rm minio-init"], input_bytes=None)
        try:
            self._wait_for_health(machine)
        except DeploymentFailure as failure:
            failure.diagnostics = self._collect_diagnostics(machine, "health")
            raise
        return plan

    def _commands(self, machine: MachineConfig, project: RenderedProject, remote_dir: str) -> list[str]:
        ssh = self._ssh_args(machine)
        target = " ".join(shlex.quote(item) for item in ssh)
        mkdir = _mkdir_command(remote_dir)
        extract = _extract_command(remote_dir)
        compose = _compose_command(remote_dir, project.project_name)
        start = compose + (" up -d --remove-orphans minio" if machine.role == "minio" else " up -d --remove-orphans")
        commands = [
            f"{target} {shlex.quote(mkdir)}",
            f"{target} {shlex.quote(extract)}",
            f"{target} {shlex.quote(compose + ' build')}",
            f"{target} {shlex.quote(start)}",
        ]
        if machine.role == "minio":
            commands.append(f"{target} {shlex.quote(compose + ' run --rm minio-init')}")
        return commands

    def _ssh_args(self, machine: MachineConfig) -> list[str]:
        args = ["ssh", "-p", str(machine.ssh_port), "-o", "BatchMode=yes", "-o", f"ConnectTimeout={int(self.command_timeout)}"]
        if machine.ssh_key is not None:
            args.extend(["-i", str(machine.ssh_key)])
        args.append(f"{machine.ssh_user}@{machine.ssh_host}")
        return args

    def _run_checked(
        self,
        machine: MachineConfig,
        phase: str,
        display_command: str,
        argv: Sequence[str],
        *,
        input_bytes: bytes | None,
    ) -> CommandResult:
        attempts = 2 if argv and argv[0] == "ssh" else 1
        result = CommandResult(1, stderr="not executed")
        for _ in range(attempts):
            result = self.runner.run(argv, input_bytes=input_bytes, timeout=self.command_timeout)
            if result.returncode == 0:
                return result
            if argv[0] != "ssh" or result.returncode != 255:
                break
        if result.returncode != 0:
            stderr = _redact(result.stderr, self.config)
            diagnostics = self._collect_diagnostics(machine, phase) if phase in {"prepare", "upload", "build", "start", "minio-init"} else ""
            raise DeploymentFailure(
                machine=machine.name,
                role=machine.role,
                phase=phase,
                command=_redact(display_command, self.config),
                returncode=result.returncode,
                stderr=stderr,
                diagnostics=diagnostics,
            )
        return result

    def _collect_diagnostics(self, machine: MachineConfig, phase: str) -> str:
        try:
            remote_dir = f"{self.config.remote_dir.rstrip('/')}/{machine.name}"
            project = render_project(self.config, machine.name)
            compose = _compose_command(remote_dir, project.project_name)
            records: list[str] = []
            for suffix in (" ps", " logs --tail=80"):
                command = compose + suffix
                result = self.runner.run([*self._ssh_args(machine), command], timeout=self.command_timeout)
                output = _redact((result.stdout + "\n" + result.stderr).strip(), self.config)
                safe_command = _redact(command, self.config)
                records.append(f"$ {safe_command} (exit {result.returncode}){': ' + output if output else ''}")
            return "; ".join(records)
        except Exception:
            return f"diagnostic collection unavailable during {phase}"

    def _wait_for_health(self, machine: MachineConfig) -> None:
        path = "/minio/health/live" if machine.role == "minio" else "/healthz"
        url = f"{machine.endpoint_url}{path}"
        deadline = time.monotonic() + self.health_timeout
        while True:
            try:
                if self.healthcheck(url):
                    return
            except Exception:
                pass
            if time.monotonic() >= deadline:
                raise DeploymentFailure(
                    machine=machine.name,
                    role=machine.role,
                    phase="health",
                    command=f"GET {url}",
                    stderr="health check timed out",
                )
            time.sleep(self.health_interval)


def _make_archive(source_root: Path, project: RenderedProject, config: DeploymentConfig) -> bytes:
    payload = BytesIO()
    with tarfile.open(fileobj=payload, mode="w:gz") as archive:
        if source_root.exists():
            for path in sorted(source_root.rglob("*")):
                relative = path.relative_to(source_root)
                if _excluded(relative, path, config):
                    continue
                if path.is_file():
                    archive.add(path, arcname=Path("source") / relative, recursive=False)
        _add_bytes(archive, "docker-compose.yml", project.compose_text.encode("utf-8"), 0o644)
        _add_bytes(archive, ".env", project.env_text.encode("utf-8"), 0o600)
        allowed = {
            "minio": {"minio_secret_key"},
            "driver": {"internal_api_secret", "minio_secret_key"},
            "observer": {"internal_api_secret", "postgres_password", "minio_secret_key"},
            "slave": {"internal_api_secret", "postgres_password", "minio_secret_key"},
        }[project.role]
        for filename, content in project.secret_files.items():
            if filename not in allowed:
                continue
            _add_bytes(archive, Path("secrets") / filename, content.encode("utf-8"), 0o600)
        if project.model_catalog_file is not None:
            _add_bytes(archive, "model_catalog.json", project.model_catalog_file.read_bytes(), 0o644)
    return payload.getvalue()


def _excluded(relative: Path, path: Path, config: DeploymentConfig) -> bool:
    if any(part in {".git", ".venv", "__pycache__", ".pytest_cache", "secrets"} for part in relative.parts):
        return True
    try:
        if path.resolve() in config.secret_paths or path.resolve() == config.config_path:
            return True
    except OSError:
        return True
    return (
        path.name == ".env"
        or path.name.startswith(".env.")
        or path.name == "deployment.toml"
        or path.name.endswith((".secret", ".key", ".pem"))
    )


def _add_bytes(archive: tarfile.TarFile, name: str | Path, content: bytes, mode: int) -> None:
    info = tarfile.TarInfo(str(name))
    info.size = len(content)
    info.mode = mode
    archive.addfile(info, BytesIO(content))


def _mkdir_command(remote_dir: str) -> str:
    quoted = shlex.quote(remote_dir)
    return f"mkdir -p {quoted}/source {quoted}/secrets"


def _extract_command(remote_dir: str) -> str:
    quoted = shlex.quote(remote_dir)
    return f"tar -xzf - -C {quoted} && chmod 600 {quoted}/.env {quoted}/secrets/*"


def _compose_command(remote_dir: str, project_name: str) -> str:
    return f"cd {shlex.quote(remote_dir)} && docker compose --project-name {shlex.quote(project_name)} --file docker-compose.yml"


def _http_healthcheck(url: str) -> bool:
    with urlopen(url, timeout=5) as response:
        if not 200 <= response.status < 300:
            return False
        # Loom role health endpoints return {"ok": bool}.  A successful HTTP
        # status alone is insufficient while Driver/Slave are still waiting
        # for their Observer lease or registration.  Keep status-only checks
        # for MinIO's plain-text health endpoint.
        body = response.read()
        try:
            payload = json.loads(body)
        except (TypeError, ValueError):
            return True
        if isinstance(payload, dict) and isinstance(payload.get("ok"), bool):
            return payload["ok"]
        return True


def _redact(value: str, config: DeploymentConfig) -> str:
    from urllib.parse import quote

    secrets = (
        config.internal_api_secret,
        config.postgres_password,
        config.minio_secret_key,
        quote(config.postgres_password, safe=""),
        quote(config.minio_secret_key, safe=""),
    )
    for secret in secrets:
        if secret:
            value = value.replace(secret, "<redacted>")
    return value
