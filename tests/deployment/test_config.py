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


def test_rejects_relative_driver_workspace_path(tmp_path: Path):
    for filename in ("internal.secret", "postgres.secret", "minio.secret"):
        write_secret(tmp_path / filename, "secret")
    path = tmp_path / "deployment.toml"
    path.write_text(config_text().replace('codex_base_url = "http://codex.internal:8787"', 'workspace_path = "relative/workspace"'), encoding="utf-8")

    with pytest.raises(DeploymentConfigError, match="workspace_path must be absolute"):
        DeploymentConfig.from_file(path)


def test_rejects_cluster_name_that_cannot_form_compose_project(tmp_path: Path):
    for filename in ("internal.secret", "postgres.secret", "minio.secret"):
        write_secret(tmp_path / filename, "secret")
    path = tmp_path / "deployment.toml"
    path.write_text(config_text().replace('name = "loom-prod"', 'name = "bad name"'), encoding="utf-8")

    with pytest.raises(DeploymentConfigError, match="cluster.name contains unsupported characters"):
        DeploymentConfig.from_file(path)


def test_ipv6_minio_console_url_is_bracketed(tmp_path: Path):
    for filename in ("internal.secret", "postgres.secret", "minio.secret"):
        write_secret(tmp_path / filename, "secret")
    path = tmp_path / "deployment.toml"
    path.write_text(config_text().replace('10.0.0.10', '2001:db8::10'), encoding="utf-8")

    config = DeploymentConfig.from_file(path)

    assert config.urls.minio_console == "http://[2001:db8::10]:9001"


def test_rejects_machine_names_that_collide_after_compose_normalization(tmp_path: Path):
    for filename in ("internal.secret", "postgres.secret", "minio.secret"):
        write_secret(tmp_path / filename, "secret")
    text = config_text().replace('name = "worker-a"', 'name = "Worker-A"') + '''

[[machines]]
name = "worker-a-copy"
ssh_host = "10.0.0.13"
ssh_user = "ubuntu"
role = "slave"
service_id = "slave-b"
'''
    text = text.replace('name = "worker-a-copy"', 'name = "worker-a"')
    path = tmp_path / "deployment.toml"
    path.write_text(text, encoding="utf-8")

    with pytest.raises(DeploymentConfigError, match="project names must be unique"):
        DeploymentConfig.from_file(path)


def test_rejects_invalid_secret_encoding_and_ssh_control_characters(tmp_path: Path):
    (tmp_path / "internal.secret").write_bytes(b"\xff")
    (tmp_path / "postgres.secret").write_text("secret", encoding="utf-8")
    (tmp_path / "minio.secret").write_text("secret", encoding="utf-8")
    path = tmp_path / "deployment.toml"
    path.write_text(config_text(), encoding="utf-8")

    with pytest.raises(DeploymentConfigError, match="internal_secret_file cannot be read"):
        DeploymentConfig.from_file(path)

    (tmp_path / "internal.secret").write_text("secret", encoding="utf-8")
    path.write_text(config_text().replace('ssh_host = "10.0.0.10"', 'ssh_host = "10.0.0.10\\nmalicious"'), encoding="utf-8")
    with pytest.raises(DeploymentConfigError, match="ssh_host must not contain"):
        DeploymentConfig.from_file(path)


def test_rejects_unknown_build_network(tmp_path: Path):
    for filename in ("internal.secret", "postgres.secret", "minio.secret"):
        write_secret(tmp_path / filename, "secret")
    path = tmp_path / "deployment.toml"
    path.write_text(config_text().replace('[cluster]', '[cluster]\nbuild_network = "bridge"'), encoding="utf-8")

    with pytest.raises(DeploymentConfigError, match="cluster.build_network"):
        DeploymentConfig.from_file(path)
