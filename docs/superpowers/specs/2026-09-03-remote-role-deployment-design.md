# Remote Role Deployment Design

**Date:** 2026-09-03
**Status:** Approved for implementation

## Goal

Provide Python deployment tooling that can start a complete Loom v2 installation
across mutually reachable machines. The operator supplies a TOML machine
inventory containing SSH connection details and the role assigned to each
machine. The tooling uploads the current source, builds the existing Docker
images remotely, generates cross-host connection settings, and starts each
role's Compose project. Observer, Driver, and each Slave remain independently
deployable.

The design does not preserve an older deployment-script interface; the new TOML
interface is the supported interface for this feature.

## Scope and constraints

- The control machine has the repository source and runs Python 3.12+.
- Target machines are reachable by SSH and have Docker Engine plus the Docker
  Compose plugin installed. The SSH user can run Docker without interactive
  privilege escalation.
- Multiple machine entries may intentionally point at the same SSH host for a
  single-host test deployment; their published ports must be distinct.
- Target machines can reach one another using the configured advertised host
  addresses and published ports. No overlay network or firewall automation is
  required.
- Exactly one Observer, one Driver, and one MinIO host are required. One or two
  Slaves are supported because the current Driver settings expose
  `LOOM_SLAVE_A_URL` and `LOOM_SLAVE_B_URL`.
- Observer and every Slave own a separate PostgreSQL container and persistent
  volume. Driver has no database. MinIO owns its persistent data volume.
- PostgreSQL and MinIO credentials, plus the shared internal API secret, are
  read from local files on the control machine and copied as mode `0600` files
  to the relevant target machines. Secrets are not committed or printed.
- The existing `Dockerfile` is used for Observer and Slave images; the existing
  `Dockerfile.driver` is used for Driver. No application runtime behavior is
  changed by this feature.
- The implementation uses Python standard-library modules (`argparse`,
  `subprocess`, `tarfile`, `tomllib`, `urllib`) and existing repository
  dependencies only. SSH and Docker are invoked through their installed command
  line clients.

## Configuration

The supported file format is TOML:

```toml
[cluster]
name = "loom-prod"
remote_dir = "/opt/loom-v2"
workspace_id = "workspace-default"
build_network = "default" # optional: default, host, or none for Dockerfile build steps; host only when bridge DNS is unavailable
internal_secret_file = "secrets/internal_api_secret"
postgres_password_file = "secrets/postgres_password"
minio_access_key = "loom"
minio_secret_key_file = "secrets/minio_secret_key"

[driver]
codex_base_url = "http://10.0.0.20:8787"
workspace_path = "/srv/loom/workspace"
model_catalog_file = "/root/.codex/model_catalog.json"

[[machines]]
name = "storage"
ssh_host = "10.0.0.10"
ssh_port = 22
ssh_user = "ubuntu"
ssh_key = "/home/operator/.ssh/id_ed25519"
role = "minio"
service_port = 9000
console_port = 9001

[[machines]]
name = "observer"
ssh_host = "10.0.0.11"
ssh_port = 22
ssh_user = "ubuntu"
role = "observer"
service_port = 8080

[[machines]]
name = "driver"
ssh_host = "10.0.0.12"
ssh_port = 22
ssh_user = "ubuntu"
role = "driver"
service_port = 8090

[[machines]]
name = "slave-a"
ssh_host = "10.0.0.13"
ssh_port = 22
ssh_user = "ubuntu"
role = "slave"
service_id = "slave-a"
service_port = 8081
```

`role` is one of `minio`, `observer`, `driver`, or `slave`. A slave must set
`service_id` to `slave-a` or `slave-b`; service ports default to 9000/9001 for
MinIO, 8080 for Observer, 8090 for Driver, and 8081/8082 for those two Slave
IDs. `ssh_host` is also the advertised address used in generated URLs; a
future explicit advertised-address field is outside this feature.

The checked-in test topology places MinIO, Observer, Driver, `slave-a`, and
`slave-b` on `9.0.3.9` using SSH user `root` and port `22`; it assigns distinct
published ports so the five Compose projects can run concurrently. Production
configurations may place each entry on a different host.

The loader validates required cluster fields, readable non-empty secret files,
unique machine names, unique roles, valid ports, and the one/two-Slave topology
before opening an SSH connection. Relative secret paths are resolved from the
configuration file's directory. The model catalog is optional; when present it
is uploaded to the Driver host and mounted read-only.

## Components and boundaries

### `loom_v2/deployment/config.py`

Loads TOML into small immutable configuration records, resolves secret files,
validates topology and ports, and derives the public URLs used by all roles.
It has no SSH or Docker dependency.

### `loom_v2/deployment/compose.py`

Renders one deterministic Compose document and `.env`/secret-file set for a
single machine. It contains only role-specific templates and address mapping;
it does not execute commands. Compose projects use a stable name derived from
`cluster.name` and machine name. Database service names are always `postgres`.

Generated projects have these shapes:

- Observer: `postgres` (healthcheck, named data volume) and `observer`, with
  `LOOM_DATABASE_URL` pointing to `postgres` and public port binding.
- Slave: `postgres` and `slave`, with the same local database pattern and
  `LOOM_OBSERVER_URL` pointing to the Observer advertised URL.
- Driver: `driver`, with Observer and each Slave advertised URLs, the internal
  secret, the optional Codex model catalog, the configured workspace bind, and
  `/var/run/docker.sock`.
- MinIO: `minio` and one-shot `minio-init`, with a named data volume and S3 /
  console port bindings.

### `loom_v2/deployment/remote.py`

Runs validated SSH commands using `subprocess` with argument lists and
`shlex.quote` only for the remote shell script portions. It streams a tar
archive of the selected source files to a temporary remote directory, writes
generated deployment files, runs Compose, polls HTTP health endpoints, and
collects diagnostic output. A small injectable command runner makes these
operations unit-testable without a remote machine.

### CLI entry points

The independent scripts are thin wrappers over the shared deployment library:

- `scripts/deploy_cluster.py --config deployment.toml` deploys all machines in
  dependency order.
- `scripts/deploy_observer.py --config deployment.toml` deploys only the
  Observer project.
- `scripts/deploy_driver.py --config deployment.toml` deploys only Driver.
- `scripts/deploy_slave.py --config deployment.toml --id slave-a` deploys one
  Slave project.
- `scripts/deploy_minio.py --config deployment.toml` deploys MinIO only.

Every entry point supports `--dry-run` and returns a non-zero exit status on
validation, SSH, build, Compose, or health-check failure.

## Deployment flow

The cluster command validates the full topology, then performs these phases:

1. MinIO and its bucket initializer.
2. Observer and its PostgreSQL.
3. All configured Slaves and their PostgreSQL instances.
4. Driver.

For each machine, the tool creates `<remote_dir>/<machine-name>`, uploads the
source archive (excluding `.git`, virtual environments, caches, and local
secret files), writes Compose and mode-`0600` environment/secret files, runs
`docker compose build`, and then runs `docker compose up -d --remove-orphans`.
Health checks wait for PostgreSQL/MinIO readiness before role startup and then
poll the role's `/healthz` endpoint. The cluster command stops at the first
failure; already completed projects and named volumes remain intact so a retry
is safe.

Role-specific commands use the same rendering and deployment path, but only
touch the selected project's machine. They still derive all remote URLs from
the complete configuration, so a Driver or Slave can be upgraded independently
without hand-editing endpoint variables.

## Idempotency and failure handling

- Stable Compose project names, service names, and volume names make repeated
  runs converge on the configured state.
- Existing containers are recreated only when Compose detects changed image or
  environment configuration. Volumes are never removed by the tooling.
- SSH commands have a configurable connect/command timeout and a bounded retry
  for transient connection failures. Health polling has a separate deadline.
- Errors are reported as structured records containing machine, role, phase,
  command, exit status, and redacted stderr. Secret values are never included.
- `--dry-run` renders and prints Compose plus the command plan without opening
  SSH or changing remote state.

## Testing and verification

- Unit tests cover TOML parsing, default ports, topology/secret validation, URL
  derivation, and deterministic Compose output for every role.
- Remote executor tests use a recording fake runner to assert archive transfer,
  file permissions, build/up ordering, health polling, and redacted failure
  diagnostics.
- CLI dry-run tests exercise a representative four-machine/two-Slave config
  without Docker or SSH.
- Existing local application tests remain unchanged. Docker Compose config
  validation is run on each generated document when the Docker CLI is
  available; remote E2E execution remains an operator-owned verification step.

## Out of scope

- Provisioning operating systems, Docker, SSH users, firewalls, DNS, TLS, or
  external Codex/S3/PostgreSQL services.
- Registry push/pull, Kubernetes, overlay networking, or automatic rollback.
- More than two Slaves without a corresponding runtime settings redesign.
