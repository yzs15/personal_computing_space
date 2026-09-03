from pathlib import Path

import pytest

from loom_v2.deployment.remote import DeploymentFailure, RemoteDeployer, RecordingRunner
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
