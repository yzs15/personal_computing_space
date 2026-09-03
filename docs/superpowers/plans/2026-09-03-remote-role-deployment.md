# Remote Role Deployment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (recommended for this session) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Python tooling that reads a TOML machine inventory, uploads this repository over SSH, builds role-specific Docker images on remote hosts, and starts independently deployable Observer, Driver, Slave, and MinIO Compose projects with automatically derived cross-host addresses.

**Architecture:** A dependency-free deployment package has three focused layers: immutable TOML/topology parsing, deterministic per-role Compose rendering, and an injectable SSH/Docker executor. Thin role scripts call the shared CLI; cluster deployment runs MinIO, Observer, Slaves, then Driver. Every Observer and Slave Compose project contains its own PostgreSQL service and named data volume.

**Tech Stack:** Python 3.12 standard library (`tomllib`, `dataclasses`, `subprocess`, `tarfile`, `urllib.request`), existing Dockerfiles and Docker Compose plugin, pytest/pytest-asyncio already in the repository.

---

### Task 1: Add validated deployment configuration and topology model

**Files:**
- Create: `loom_v2/deployment/__init__.py`
- Create: `loom_v2/deployment/config.py`
- Create: `tests/deployment/__init__.py`
- Create: `tests/deployment/test_config.py`

- [ ] **Step 1: Write the failing tests for TOML loading, defaults, URL derivation, and validation**

```python
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
    text = config_text().replace('service_port = 8080', 'service_port = 18080')
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
```

- [ ] **Step 2: Run the focused tests to verify they fail for the missing package**

Run: `.venv/bin/pytest -q tests/deployment/test_config.py`

Expected: FAIL with `ModuleNotFoundError: No module named 'loom_v2.deployment'`.

- [ ] **Step 3: Implement the minimal immutable configuration API**

Implement `MachineConfig`, `DriverConfig`, `EndpointMap`, and `DeploymentConfig` as frozen dataclasses. `DeploymentConfig.from_file()` must parse `tomllib`, resolve relative secret paths from the TOML parent, read non-empty single-line secrets, apply ports 9000/9001 (MinIO), 8080 (Observer), 8090 (Driver), and 8081/8082 (Slave IDs), then validate one MinIO, one Observer, one Driver, one or two unique Slave IDs, unique machine names, valid port ranges, and required SSH fields. Expose `machine(name_or_role)`, `slaves`, and `urls` properties. Raise `DeploymentConfigError` with a stable message naming the invalid field.

- [ ] **Step 4: Run the focused tests to verify they pass**

Run: `.venv/bin/pytest -q tests/deployment/test_config.py`

Expected: PASS.

- [ ] **Step 5: Commit the configuration layer**

```bash
git add loom_v2/deployment tests/deployment/test_config.py
git commit -m "feat: add remote deployment topology config"
```

### Task 2: Render deterministic Compose projects for each role

**Files:**
- Create: `loom_v2/deployment/compose.py`
- Create: `tests/deployment/test_compose.py`

- [ ] **Step 1: Write failing renderer tests for every role and secret redaction**

```python
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
    assert environment["LOOM_SLAVE_A_URL"] == "http://10.0.0.13:8081"
    assert document["services"]["driver"]["volumes"][-1] == "/var/run/docker.sock:/var/run/docker.sock"
    assert "pg-password" not in project.redacted_preview
    assert "internal-token" not in project.redacted_preview


def test_minio_project_contains_bucket_initializer(tmp_path: Path):
    project = render_project(make_config(tmp_path), "storage")
    document = json.loads(project.compose_text)

    assert set(document["services"]) == {"minio", "minio-init"}
    assert document["services"]["minio"]["ports"] == ["9000:9000", "9001:9001"]
    assert "mc mb --ignore-existing" in document["services"]["minio-init"]["command"]
```

- [ ] **Step 2: Run the renderer tests to verify they fail**

Run: `.venv/bin/pytest -q tests/deployment/test_compose.py`

Expected: FAIL with `ModuleNotFoundError: No module named 'loom_v2.deployment.compose'`.

- [ ] **Step 3: Implement deterministic JSON-as-Compose rendering**

Define frozen `RenderedProject` with `compose_text`, `env_text`, `secret_files`, and `redacted_preview`. Build plain dictionaries and serialize `compose_text` with `json.dumps(..., indent=2, sort_keys=True)`, which Docker Compose accepts as YAML/JSON. Render role-specific services with existing image names and Dockerfiles, healthchecks, named volumes, internal secret mounts, and published ports. Use JSON-quoted values in the generated `.env` text, and use environment references only for credentials that must be shared with PostgreSQL/MinIO. Observer and Slave `LOOM_DATABASE_URL` values must use the local `postgres` service; Driver must set Observer/Slave/S3 URLs, Codex settings, and its Docker socket/workspace mounts. Never place secret contents in `compose_text` or `redacted_preview`.

- [ ] **Step 4: Run the renderer tests and a Compose parser check**

Run: `.venv/bin/pytest -q tests/deployment/test_compose.py`

Expected: PASS.

Run: `python -c 'import json; from pathlib import Path; json.loads(Path("/tmp/generated-compose.json").read_text())'` after rendering a fixture in the test, or use `docker compose -f <generated-file> config` when Docker is available.

- [ ] **Step 5: Commit the renderer**

```bash
git add loom_v2/deployment/compose.py tests/deployment/test_compose.py
git commit -m "feat: render per-role deployment compose projects"
```

### Task 3: Implement injectable SSH upload, Compose execution, and health checks

**Files:**
- Create: `loom_v2/deployment/remote.py`
- Create: `tests/deployment/test_remote.py`

- [ ] **Step 1: Write failing tests for command order, archive filtering, dry-run, and redacted failures**

```python
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
```

- [ ] **Step 2: Run the remote tests to verify they fail**

Run: `.venv/bin/pytest -q tests/deployment/test_remote.py`

Expected: FAIL with `ModuleNotFoundError: No module named 'loom_v2.deployment.remote'`.

- [ ] **Step 3: Implement archive creation and safe command execution**

Add `CommandResult`, `CommandRunner`, `SubprocessRunner`, `RecordingRunner`, `DeploymentPlan`, `DeploymentFailure`, and `RemoteDeployer`. Build SSH argument lists with `-p`, optional `-i`, `BatchMode=yes`, and `ConnectTimeout`; run remote shell commands only after quoting paths with `shlex.quote`. Create an in-memory gzip tar containing the source tree (excluding `.git`, `.venv`, `__pycache__`, `.pytest_cache`, `secrets`) plus generated Compose, `.env`, and secret files; set secret member modes to `0600`. Upload it by piping bytes to remote `tar -xzf -`, then run `docker compose -p <stable-project> -f docker-compose.yml build` and `up -d --remove-orphans` from the remote project directory. Apply bounded retries only to SSH failures. Redact configured secret values from diagnostics and dry-run previews.

- [ ] **Step 4: Implement health polling and failure diagnostics**

Poll `http://<advertised-host>:<port>/healthz` for Observer, Driver, and Slave; poll MinIO's `/minio/health/live`. Use an injectable `healthcheck(url)` callable and a deadline/interval on `RemoteDeployer`. On timeout or nonzero command exit, raise `DeploymentFailure(machine, role, phase, command, returncode, stderr)` after collecting `docker compose ps` and `docker compose logs --tail=80` through the runner. Never run `docker compose down -v` or remove named volumes.

- [ ] **Step 5: Run remote tests and refactor while green**

Run: `.venv/bin/pytest -q tests/deployment/test_remote.py`

Expected: PASS with no warnings or errors from the new tests.

- [ ] **Step 6: Commit the remote executor**

```bash
git add loom_v2/deployment/remote.py tests/deployment/test_remote.py
git commit -m "feat: deploy compose projects over ssh"
```

### Task 4: Add cluster and independent role CLI entry points

**Files:**
- Create: `loom_v2/deployment/cli.py`
- Create: `scripts/deploy_cluster.py`
- Create: `scripts/deploy_observer.py`
- Create: `scripts/deploy_driver.py`
- Create: `scripts/deploy_slave.py`
- Create: `scripts/deploy_minio.py`
- Create: `deploy/deployment.example.toml`
- Create: `deploy/deployment.test.toml`
- Create: `tests/deployment/test_cli.py`

- [ ] **Step 1: Write failing dry-run CLI tests**

```python
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
```

- [ ] **Step 2: Run the CLI tests to verify they fail**

Run: `.venv/bin/pytest -q tests/deployment/test_cli.py`

Expected: FAIL because the script entry points do not exist.

- [ ] **Step 3: Implement shared CLI orchestration**

Implement `loom_v2.deployment.cli.main(argv, forced_target=None)` with `--config`, `--source-root` (default repository root), `--dry-run`, and optional `--id` for the Slave script. In cluster mode deploy machine names in MinIO → Observer → Slaves sorted by service ID → Driver order. In role mode select exactly the requested machine and still load the full topology for URL derivation. Print a redacted plan in dry-run mode; catch `DeploymentConfigError` and `DeploymentFailure`, print their stable diagnostic text to stderr, and return 2. Keep wrappers executable and add a small `sys.path` bootstrap so `python scripts/deploy_*.py` works from a source checkout.

- [ ] **Step 4: Add the example TOML and CLI help text**

Copy the approved schema into `deploy/deployment.example.toml`, including Observer, Driver, MinIO, and both Slave examples, comments for defaults, and secret-file paths. Add module docstrings/help text that state remote prerequisites (SSH, Docker Engine, Compose plugin, passwordless Docker).

Add `deploy/deployment.test.toml` with all five projects on `9.0.3.9`, SSH user `root`, port `22`, and distinct published ports (`18080` Observer, `18090` Driver, `18081`/`18082` Slaves, `19000`/`19001` MinIO). It must use the same secret-file keys as the example and never contain secret values.

- [ ] **Step 5: Run CLI tests and manual help/dry-run checks**

Run: `.venv/bin/pytest -q tests/deployment/test_cli.py`

Expected: PASS.

Run: `python scripts/deploy_cluster.py --help` and confirm it exits 0.

Run: `python scripts/deploy_cluster.py --config deploy/deployment.example.toml --dry-run` only after creating local example secret files; confirm output contains no secret values.

- [ ] **Step 6: Commit the CLI layer and example**

```bash
git add loom_v2/deployment/cli.py scripts/deploy_*.py deploy/deployment.example.toml tests/deployment/test_cli.py
git commit -m "feat: add cluster and role deployment commands"
```

### Task 5: Document operation and perform complete verification

**Files:**
- Modify: `README.md`
- Create: `tests/deployment/test_generated_compose.py`

- [ ] **Step 1: Write a generated Compose validation test**

```python
import json
from pathlib import Path

from .test_compose import make_config
from loom_v2.deployment.compose import render_project


def test_every_role_renderer_produces_parseable_compose(tmp_path: Path):
    config = make_config(tmp_path)
    for machine in ("storage", "observer", "driver", "worker-a"):
        document = json.loads(render_project(config, machine).compose_text)
        assert document["services"]
        assert all(name.replace("_", "").replace("-", "").isalnum() for name in document["services"])
```

- [ ] **Step 2: Run it to confirm the complete renderer path**

Run: `.venv/bin/pytest -q tests/deployment/test_generated_compose.py`

Expected: FAIL only if a renderer role is missing or emits invalid JSON.

- [ ] **Step 3: Add concise README deployment instructions**

Document prerequisites, secret-file creation, the example TOML, full-cluster command, independent role commands, dry-run behavior, expected public ports, and the fact that each Slave/Observer has a private PostgreSQL volume. Explain that the Driver needs the Docker socket and that generated URLs use each machine's `ssh_host`.

- [ ] **Step 4: Run focused and repository verification commands**

Run: `.venv/bin/pytest -q tests/deployment`

Expected: all new deployment tests pass.

Run: `.venv/bin/pytest -q tests/deploy/test_compose_config.py tests/test_cli.py`

Expected: existing deployment/CLI tests pass (apart from the acknowledged pre-existing baseline failures outside this feature).

Run: `git diff --check`

Expected: no whitespace errors.

If Docker is available, render one project per role from a temporary fixture and run `docker compose -f <file> config`; otherwise record that this external verification was unavailable.

- [ ] **Step 5: Review requirements against the approved spec and commit documentation**

Check that every spec requirement has a corresponding test or implementation: TOML topology validation, per-Slave PostgreSQL isolation, generated cross-host URLs, source upload, role-specific Dockerfile selection, ordered startup, health diagnostics, idempotent volumes, redaction, dry-run, and independent scripts. Then commit:

```bash
git add README.md tests/deployment/test_generated_compose.py
git commit -m "docs: document remote deployment workflow"
```
