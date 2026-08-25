from pathlib import Path
import subprocess


def test_compose_declares_four_isolated_postgres_instances():
    compose = Path("deploy/docker-compose.yml")
    result = subprocess.run(
        ["/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe", "compose", "-f", str(compose), "config", "--services"],
        check=True,
        capture_output=True,
        text=True,
    )
    services = set(result.stdout.split())
    assert {"observer-db", "driver-db", "slave-a-db", "slave-b-db"} <= services
