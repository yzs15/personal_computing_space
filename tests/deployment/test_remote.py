from pathlib import Path
import tarfile
from io import BytesIO

import pytest

from loom_v2.deployment.remote import DeploymentFailure, RemoteDeployer, RecordingRunner, SubprocessRunner
from loom_v2.deployment import remote as remote_module
from .test_compose import make_config


def test_deploy_uploads_then_builds_then_starts(tmp_path: Path):
    config = make_config(tmp_path)
    runner = RecordingRunner()
    deployer = RemoteDeployer(config, source_root=tmp_path, runner=runner, healthcheck=lambda _: True)

    deployer.deploy("observer")

    commands = [item.command for item in runner.calls]
    assert commands[0].startswith("ssh ") and "mkdir -p" in commands[0]
    assert "tar -xzf -" in commands[1]
    assert "docker compose" in commands[2] and " build" in commands[2]
    assert "docker compose" in commands[3] and " up -d" in commands[3]
    assert all("internal-token" not in item.command for item in runner.calls)


def test_dry_run_does_not_call_runner(tmp_path: Path):
    config = make_config(tmp_path)
    runner = RecordingRunner()
    deployer = RemoteDeployer(config, source_root=tmp_path, runner=runner, healthcheck=lambda _: True)

    plan = deployer.deploy("observer", dry_run=True)

    assert plan.machine == "observer"
    assert runner.calls == []
    assert "internal-token" not in plan.preview


def test_command_failure_contains_phase_without_secret(tmp_path: Path):
    config = make_config(tmp_path)
    runner = RecordingRunner(fail_at=2, stderr="permission denied internal-token")
    deployer = RemoteDeployer(config, source_root=tmp_path, runner=runner, healthcheck=lambda _: True)

    with pytest.raises(DeploymentFailure) as error:
        deployer.deploy("observer")

    assert error.value.phase == "upload"
    assert "internal-token" not in str(error.value)


def test_uploaded_archive_contains_only_role_relevant_secrets(tmp_path: Path):
    config = make_config(tmp_path)
    (tmp_path / ".env").write_text("LEAK=should-not-upload", encoding="utf-8")
    runner = RecordingRunner()
    RemoteDeployer(config, source_root=tmp_path, runner=runner, healthcheck=lambda _: True).deploy("storage")

    archive = tarfile.open(fileobj=BytesIO(runner.calls[1].input_bytes), mode="r:gz")
    names = set(archive.getnames())
    assert "secrets/minio_secret_key" in names
    assert "secrets/internal_api_secret" not in names
    assert "secrets/postgres_password" not in names
    assert "source/.env" not in names


def test_uploaded_archive_excludes_configured_secret_with_non_secret_extension(tmp_path: Path):
    config = make_config(tmp_path)
    custom_secret = tmp_path / "credentials"
    custom_secret.write_text("do-not-upload", encoding="utf-8")
    config_path = tmp_path / "deployment.toml"
    config_path.write_text(config_path.read_text().replace("postgres.secret", "credentials"), encoding="utf-8")
    config = config.__class__.from_file(config_path)
    runner = RecordingRunner()

    RemoteDeployer(config, source_root=tmp_path, runner=runner, healthcheck=lambda _: True).deploy("observer")

    archive = tarfile.open(fileobj=BytesIO(runner.calls[1].input_bytes), mode="r:gz")
    assert "source/credentials" not in set(archive.getnames())


def test_build_failure_includes_command_and_diagnostic_commands(tmp_path: Path):
    config = make_config(tmp_path)
    runner = RecordingRunner(fail_at=3, stderr="build failed")

    with pytest.raises(DeploymentFailure) as error:
        RemoteDeployer(config, source_root=tmp_path, runner=runner, healthcheck=lambda _: True).deploy("observer")

    message = str(error.value)
    assert "command=" in message
    assert "docker compose" in message
    assert "diagnostics" in message


def test_subprocess_runner_reports_missing_binary_as_command_failure():
    result = SubprocessRunner().run(["/definitely/missing/loom-command"])

    assert result.returncode == 127
    assert "No such file" in result.stderr


def test_minio_deployment_waits_for_bucket_initializer(tmp_path: Path):
    config = make_config(tmp_path)
    runner = RecordingRunner()

    RemoteDeployer(config, source_root=tmp_path, runner=runner, healthcheck=lambda _: True).deploy("storage")

    assert " wait minio-init" in runner.calls[4].command


class _HealthResponse:
    def __init__(self, body: bytes, status: int = 200) -> None:
        self.body = body
        self.status = status

    def __enter__(self) -> "_HealthResponse":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self) -> bytes:
        return self.body


def test_http_healthcheck_requires_json_ok_when_present(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(remote_module, "urlopen", lambda *_args, **_kwargs: _HealthResponse(b'{"ok": false}'))
    assert remote_module._http_healthcheck("http://driver:8090/healthz") is False

    monkeypatch.setattr(remote_module, "urlopen", lambda *_args, **_kwargs: _HealthResponse(b'{"ok": true}'))
    assert remote_module._http_healthcheck("http://driver:8090/healthz") is True


def test_http_healthcheck_accepts_status_only_payloads(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(remote_module, "urlopen", lambda *_args, **_kwargs: _HealthResponse(b"ok"))
    assert remote_module._http_healthcheck("http://minio:9000/minio/health/live") is True
