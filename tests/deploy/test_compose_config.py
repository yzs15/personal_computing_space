from pathlib import Path
import subprocess


def test_compose_declares_observer_and_two_slave_postgres_instances():
    compose = Path("deploy/docker-compose.yml")
    result = subprocess.run(
        ["/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe", "compose", "-f", str(compose), "config", "--services"],
        check=True,
        capture_output=True,
        text=True,
    )
    services = set(result.stdout.split())
    assert {"observer-db", "slave-a-db", "slave-b-db"} <= services
    assert "driver-db" not in services


def test_compose_uses_host_proxy_and_only_requires_internal_secret():
    compose = Path("deploy/docker-compose.yml").read_text(encoding="utf-8")

    assert "codex_api_key" not in compose
    assert "http://host.docker.internal:8787" in compose
    assert "host.docker.internal:host-gateway" in compose
    assert "LOOM_SLAVE_A_URL: http://slave-a:8081" in compose
    assert "LOOM_SLAVE_B_URL: http://slave-b:8082" in compose
