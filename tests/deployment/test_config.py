from pathlib import Path

import pytest

from loom_v2.deployment.config import DeploymentConfig, DeploymentConfigError


def write_secret(path: Path, value: str) -> None:
    path.write_text(value, encoding="utf-8")


def config_text() -> str:
    return """
[cluster]
name = "loom-prod"
remote_dir = "/opt/loom-v2"
workspace_id = "workspace-default"
internal_secret_file = "internal.secret"
postgres_password_file = "postgres.secret"
minio_secret_key_file = "minio.secret"

[driver]
codex_base_url = "http://codex.internal:8787"

[[machines]]
name = "storage"
ssh_host = "10.0.0.10"
ssh_user = "ubuntu"
role = "minio"

[[machines]]
name = "observer"
ssh_host = "10.0.0.11"
ssh_user = "ubuntu"
role = "observer"

[[machines]]
name = "driver"
ssh_host = "10.0.0.12"
ssh_user = "ubuntu"
role = "driver"

[[machines]]
name = "worker-a"
ssh_host = "10.0.0.13"
ssh_user = "ubuntu"
role = "slave"
service_id = "slave-a"
"""


def test_loads_defaults_and_derives_cross_host_urls(tmp_path: Path):
    for filename, value in {
        "internal.secret": "internal-token",
        "postgres.secret": "pg-password",
        "minio.secret": "minio-password",
    }.items():
        write_secret(tmp_path / filename, value)
    path = tmp_path / "deployment.toml"
    path.write_text(config_text(), encoding="utf-8")

    config = DeploymentConfig.from_file(path)

    assert config.machine("observer").service_port == 8080
    assert config.machine("worker-a").service_port == 8081
    assert config.urls.observer == "http://10.0.0.11:8080"
    assert config.urls.slaves == {"slave-a": "http://10.0.0.13:8081"}
    assert config.postgres_password == "pg-password"


def test_rejects_missing_driver_or_duplicate_slave_id(tmp_path: Path):
    for filename in ("internal.secret", "postgres.secret", "minio.secret"):
        write_secret(tmp_path / filename, "secret")
    text = config_text().replace('role = "driver"', 'role = "observer"', 1)
    path = tmp_path / "deployment.toml"
    path.write_text(text, encoding="utf-8")

    with pytest.raises(DeploymentConfigError, match="exactly one driver"):
        DeploymentConfig.from_file(path)


def test_rejects_unreadable_or_empty_secret(tmp_path: Path):
    (tmp_path / "internal.secret").write_text("\n", encoding="utf-8")
    (tmp_path / "postgres.secret").write_text("pg", encoding="utf-8")
    (tmp_path / "minio.secret").write_text("minio", encoding="utf-8")
    path = tmp_path / "deployment.toml"
    path.write_text(config_text(), encoding="utf-8")

    with pytest.raises(DeploymentConfigError, match="internal_secret_file"):
        DeploymentConfig.from_file(path)


def test_all_test_roles_can_share_one_ssh_host_when_ports_are_distinct(tmp_path: Path):
    for filename in ("internal.secret", "postgres.secret", "minio.secret"):
        write_secret(tmp_path / filename, "secret")
    text = config_text()
    text += '''

[[machines]]
name = "worker-b"
ssh_host = "9.0.3.9"
ssh_user = "root"
ssh_port = 22
role = "slave"
service_id = "slave-b"
service_port = 18082
'''
    text = text.replace('ssh_host = "10.0.0.10"', 'ssh_host = "9.0.3.9"').replace('ssh_host = "10.0.0.11"', 'ssh_host = "9.0.3.9"').replace('ssh_host = "10.0.0.12"', 'ssh_host = "9.0.3.9"').replace('ssh_host = "10.0.0.13"', 'ssh_host = "9.0.3.9"')
    path = tmp_path / "deployment.toml"
    path.write_text(text, encoding="utf-8")

    config = DeploymentConfig.from_file(path)

    assert {machine.ssh_host for machine in config.machines} == {"9.0.3.9"}
    assert {machine.ssh_port for machine in config.machines} == {22}
    assert config.urls.slaves["slave-b"] == "http://9.0.3.9:18082"
