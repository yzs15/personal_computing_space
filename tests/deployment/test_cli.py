import subprocess
import sys
from pathlib import Path

from .test_config import config_text, write_secret


def test_cluster_dry_run_prints_dependency_order_without_ssh(tmp_path: Path):
    for filename, value in {
        "internal.secret": "internal-token",
        "postgres.secret": "pg-password",
        "minio.secret": "minio-password",
    }.items():
        write_secret(tmp_path / filename, value)
    config = tmp_path / "deployment.toml"
    config.write_text(config_text(), encoding="utf-8")

    result = subprocess.run(
        [sys.executable, "scripts/deploy_cluster.py", "--config", str(config), "--dry-run"],
        check=True,
        capture_output=True,
        text=True,
    )

    assert result.stdout.index("storage (minio)") < result.stdout.index("observer (observer)")
    assert result.stdout.index("observer (observer)") < result.stdout.index("driver (driver)")
    assert "ssh " in result.stdout
    assert "internal-token" not in result.stdout


def test_slave_script_requires_known_slave_id(tmp_path: Path):
    for filename, value in {
        "internal.secret": "internal-token",
        "postgres.secret": "pg-password",
        "minio.secret": "minio-password",
    }.items():
        write_secret(tmp_path / filename, value)
    config = tmp_path / "deployment.toml"
    config.write_text(config_text(), encoding="utf-8")

    result = subprocess.run(
        [sys.executable, "scripts/deploy_slave.py", "--config", str(config), "--id", "slave-c", "--dry-run"],
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "slave-c" in result.stderr
