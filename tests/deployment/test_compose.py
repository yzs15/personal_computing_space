import json
from pathlib import Path

from loom_v2.deployment.compose import render_project
from .test_config import config_text, write_secret
from loom_v2.deployment.config import DeploymentConfig


def make_config(tmp_path: Path) -> DeploymentConfig:
    for filename, value in {
        "internal.secret": "internal-token",
        "postgres.secret": "pg-password",
        "minio.secret": "minio-password",
    }.items():
        write_secret(tmp_path / filename, value)
    path = tmp_path / "deployment.toml"
    path.write_text(config_text(), encoding="utf-8")
    return DeploymentConfig.from_file(path)


def test_observer_project_contains_private_postgres_and_public_port(tmp_path: Path):
    project = render_project(make_config(tmp_path), "observer")
    document = json.loads(project.compose_text)

    assert set(document["services"]) == {"observer", "postgres"}
    assert document["services"]["observer"]["ports"] == ["8080:8080"]
    assert document["services"]["observer"]["environment"]["LOOM_DATABASE_URL"].startswith(
        "postgresql+asyncpg://loom:"
    )
    assert document["services"]["postgres"]["volumes"] == ["observer-postgres-data:/var/lib/postgresql/data"]
    assert project.secret_files["internal_api_secret"] == "internal-token"


def test_slave_project_uses_observer_url_and_isolated_database(tmp_path: Path):
    project = render_project(make_config(tmp_path), "worker-a")
    document = json.loads(project.compose_text)

    assert set(document["services"]) == {"slave", "postgres"}
    assert document["services"]["slave"]["environment"]["LOOM_OBSERVER_URL"] == "http://10.0.0.11:8080"
    assert document["services"]["postgres"]["volumes"] == ["worker-a-postgres-data:/var/lib/postgresql/data"]


def test_driver_project_wires_all_remote_services_and_docker_socket(tmp_path: Path):
    project = render_project(make_config(tmp_path), "driver")
    document = json.loads(project.compose_text)
    environment = document["services"]["driver"]["environment"]

    assert environment["LOOM_OBSERVER_URL"] == "http://10.0.0.11:8080"
    assert environment["LOOM_DRIVER_URL"] == "http://10.0.0.12:8090"
    assert environment["LOOM_SLAVE_A_URL"] == "http://10.0.0.13:8081"
    assert environment["LOOM_S3_ENDPOINT_URL"] == "${LOOM_S3_ENDPOINT_URL}"
    assert document["services"]["driver"]["volumes"][-1] == "/var/run/docker.sock:/var/run/docker.sock"
    assert "pg-password" not in project.redacted_preview
    assert "internal-token" not in project.redacted_preview


def test_minio_project_contains_bucket_initializer(tmp_path: Path):
    project = render_project(make_config(tmp_path), "storage")
    document = json.loads(project.compose_text)

    assert set(document["services"]) == {"minio", "minio-init"}
    assert document["services"]["minio"]["ports"] == ["9000:9000", "9001:9001"]
    assert "mc mb --ignore-existing" in document["services"]["minio-init"]["command"]


def test_slave_advertises_cross_host_endpoint_to_observer(tmp_path: Path):
    project = render_project(make_config(tmp_path), "worker-a")
    environment = json.loads(project.compose_text)["services"]["slave"]["environment"]

    assert environment["LOOM_SLAVE_ENDPOINT_URL"] == "http://10.0.0.13:8081"
