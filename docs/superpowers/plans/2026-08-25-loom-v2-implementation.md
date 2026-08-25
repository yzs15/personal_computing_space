# Loom v2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and deploy the Python single-user/single-Workspace vertical slice described by the v2 specification, including extensible `TypedTerm` contracts, continuous plan patching, deterministic execution, isolated PostgreSQL instances, Fake/Codex agent providers, and a Conversation-first UI.

**Architecture:** `loom_v2/contracts` is the language-neutral Layer 0 encoded with Pydantic models, canonical JSON digests, a versioned vocabulary registry, typed holes, constraints, events, and deterministic refinement validation. Observer is the sole state authority and persists its state in observer-db; Driver owns one active conversation lane and talks to either Fake or Codex app-server; Slave A/B expose WorkerService and persist only local attempt/replica ledgers. The first deployment uses one Python image with role-specific entrypoints plus four PostgreSQL containers, while the host Driver profile can launch the local Codex CLI without copying `~/.codex/config.toml`.

**Tech Stack:** Python 3.12, FastAPI/Starlette, Pydantic v2, SQLAlchemy 2 + asyncpg, Alembic-compatible SQL migrations, pytest/pytest-asyncio/httpx, vanilla HTML/CSS/JS with SSE, Docker Compose, local Codex app-server over stdio JSONL.

**Execution status (2026-08-25):** Tasks 1–7 are implemented and verified; Task 8 Compose files/configuration are implemented and validated, but image build/deployment is blocked in this environment because Docker cannot resolve `registry-1.docker.io`. The local venv experience and Fake vertical slice are runnable; the next deployment attempt should rerun the unchanged Compose commands after registry DNS/network is restored.

---

### Task 1: Bootstrap the Python project and deterministic test harness

**Files:**
- Create: `pyproject.toml`
- Create: `loom_v2/__init__.py`
- Create: `loom_v2/settings.py`
- Create: `tests/conftest.py`
- Create: `tests/test_health.py`
- Create: `.env.example`
- Create: `README.md`

- [ ] **Step 1: Write the failing health test**

```python
# tests/test_health.py
from fastapi.testclient import TestClient
from loom_v2.observer.app import create_app


def test_health_endpoint_reports_role():
    response = TestClient(create_app()).get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"ok": True, "service": "observer"}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pytest -q tests/test_health.py`

Expected: collection fails with `ModuleNotFoundError: No module named 'loom_v2.observer'`.

- [ ] **Step 3: Add package metadata, settings, and the minimal app factory**

```toml
# pyproject.toml
[project]
name = "loom-v2"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
  "fastapi>=0.115,<1",
  "uvicorn[standard]>=0.30,<1",
  "pydantic>=2.8,<3",
  "pydantic-settings>=2.4,<3",
  "sqlalchemy[asyncio]>=2.0,<3",
  "asyncpg>=0.29,<1",
  "httpx>=0.27,<1",
]

[project.optional-dependencies]
test = ["pytest>=8,<9", "pytest-asyncio>=0.23,<1"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
```

```python
# loom_v2/settings.py
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    service_name: str = "observer"
    database_url: str = "sqlite+aiosqlite:///:memory:"
    coding_agent_backend: str = "fake"
    codex_model: str = "deepseek-v4-flash"

    model_config = SettingsConfigDict(env_prefix="LOOM_", extra="ignore")
```

```python
# loom_v2/observer/app.py
from fastapi import FastAPI


def create_app() -> FastAPI:
    app = FastAPI(title="Loom v2 Observer")

    @app.get("/healthz")
    async def health() -> dict[str, object]:
        return {"ok": True, "service": "observer"}

    return app
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `pip install -e '.[test]' && pytest -q tests/test_health.py`

Expected: `1 passed`.

- [ ] **Step 5: Commit the bootstrap**

```bash
git add pyproject.toml loom_v2 tests .env.example README.md
git commit -m "build: bootstrap loom v2 python project"
```

### Task 2: Implement Layer 0 contracts, vocabulary registry, and decorators

**Files:**
- Create: `loom_v2/contracts/types.py`
- Create: `loom_v2/contracts/terms.py`
- Create: `loom_v2/contracts/constraints.py`
- Create: `loom_v2/contracts/refinement.py`
- Create: `loom_v2/contracts/decorators.py`
- Create: `loom_v2/contracts/errors.py`
- Create: `loom_v2/contracts/events.py`
- Create: `loom_v2/contracts/__init__.py`
- Create: `tests/contracts/test_terms.py`
- Create: `tests/contracts/test_closure.py`
- Create: `tests/contracts/test_decorators.py`

- [ ] **Step 1: Write failing tests for extensible terms and required-term support**

```python
# tests/contracts/test_terms.py
import pytest
from loom_v2.contracts.terms import (
    TermSupport, UnknownRequiredTerm, VocabularyRegistry, builtin_registry,
)
from loom_v2.contracts.types import TypedTerm


def test_builtin_registry_validates_precision_term():
    registry = builtin_registry()
    term = TypedTerm(kind="loom.compute.precision.v1", schema_ref="loom.compute.precision/1", value={"epsilon": 0.01}, criticality="required")
    assert registry.validate(term).kind == term.kind


def test_unknown_required_term_is_rejected_but_advisory_round_trips():
    registry = builtin_registry()
    required = TypedTerm(kind="vendor.new.v1", schema_ref="vendor.new/1", value={"x": 1}, criticality="required")
    advisory = required.model_copy(update={"criticality": "advisory"})
    with pytest.raises(UnknownRequiredTerm):
        registry.validate(required)
    assert registry.round_trip(advisory) == advisory


def test_slave_support_is_stage_specific():
    support = TermSupport(kind="loom.compute.precision.v1", schema_ref="loom.compute.precision/1", support={"parse", "preserve", "validate"}, execution_stages={"commit"})
    assert support.can("validate", "commit")
    assert not support.can("enforce", "execute")
```

```python
# tests/contracts/test_closure.py
from loom_v2.contracts.constraints import Constraint
from loom_v2.contracts.types import ComputeRequirement, ComputeSpec, TaskClosure, TypedHole


def test_compute_requirement_uses_constraint_ref_as_single_source():
    constraint = Constraint(subject=["ComputeSpec"], predicate={"op": "le", "field": "cpu_seconds", "value": 60}, source="Requester", fate="preserve")
    requirement = ComputeRequirement(key="loom.compute.budget.v1", value={"cpu_seconds": 60}, view="systems", constraint_ref=constraint.ref())
    closure = TaskClosure.minimal(compute=ComputeSpec(requirements=[requirement], typed_holes=[TypedHole(hole_id="h_compute")]), constraints=[constraint])
    assert closure.compute.requirements[0].constraint_ref.constraint_id == constraint.constraint_id
```

```python
# tests/contracts/test_decorators.py
from loom_v2.contracts.decorators import attach_constraint, task_closure
from loom_v2.contracts.constraints import ConstraintSpec


def test_decorators_materialize_explicit_metadata_without_calling_function():
    calls = []

    @task_closure(goal="echo", operation_ref="loom://echo")
    @attach_constraint(ConstraintSpec(subject=["ComputeSpec"], predicate={"op": "le", "field": "cpu_seconds", "value": 1}, source="Requester", fate="preserve"))
    def task():
        calls.append("executed")

    contract = task.materialize_contract()
    assert contract.goal == "echo"
    assert calls == []
    assert len(contract.declared_constraints) == 1
```

- [ ] **Step 2: Run the focused tests and verify they fail**

Run: `pytest -q tests/contracts`

Expected: import failures for `loom_v2.contracts`.

- [ ] **Step 3: Implement Pydantic models and canonical digest helpers**

`types.py` defines `OpaqueId`, `ResourceRef`, `DataApplication`, `DataSystems`, `ProgramApplication`, `ProgramSystems`, `ComputeApplication`, `ComputeSystems`, `ComputeRequirement`, `TypedHole`, `ComputeSpec`, `ComputeBinding`, `SpecPart`, `TaskClosure`, `ClosureContract`, `ClosureVersion`, and `Execution`. Every model uses `ConfigDict(extra="forbid")` except `StructuredValue`, which is JSON-compatible and retained by digest. `TaskClosure.canonical_digest()` serializes `model_dump(mode="json", exclude_none=True)` with sorted keys and compact separators.

`terms.py` defines `VocabularyTerm`, `SchemaRef`, `TypedTerm`, `TermSupport`, `VocabularyRegistryEntry`, and `VocabularyRegistry`. Register built-ins for `loom.compute.capability.v1`, `loom.compute.precision.v1`, `loom.compute.parallelism.v1`, `loom.compute.budget.v1`, `loom.data.locality.v1`, `loom.compute.network.v1`, and `loom.compute.deadline.v1`. `VocabularyRegistry.validate()` checks schema/value shape and raises `UnknownRequiredTerm` for an unregistered required term; `round_trip()` preserves unknown advisory terms.

`constraints.py` defines the five orthogonal tags and `Constraint.ref()`. `refinement.py` implements `is_monotonic_tightening(parent, child)` for numeric `le`/`ge`, allow-list subset, effect subset, and target narrowing; all other relation kinds return a typed `validation_failed` error instead of guessing. `errors.py` defines `DomainErrorEnvelope`; `events.py` defines `RunEvent`, `ResourceEvent`, and provenance links.

- [ ] **Step 4: Implement decorator compilation and run the tests**

`task_closure()` and `attach_constraint()` store only validated Pydantic metadata on the function. `materialize_contract()` constructs a `ClosureContract` without invoking the decorated callable and rejects callable predicates/lambdas. Run:

```bash
pytest -q tests/contracts
```

Expected: all contract tests pass.

- [ ] **Step 5: Commit Layer 0**

```bash
git add loom_v2/contracts tests/contracts
git commit -m "feat: add extensible layer zero contracts"
```

### Task 3: Add PostgreSQL persistence, migrations, and repository boundaries

**Files:**
- Create: `loom_v2/db/base.py`
- Create: `loom_v2/db/models.py`
- Create: `loom_v2/db/session.py`
- Create: `migrations/001_initial.sql`
- Create: `tests/integration/test_repositories.py`
- Create: `scripts/wait-for-db.py`

- [ ] **Step 1: Write repository tests against a disposable PostgreSQL URL**

```python
# tests/integration/test_repositories.py
import os
import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from loom_v2.observer.repository import ObserverRepository


@pytest.mark.integration
async def test_patch_is_cas_and_idempotent():
    engine = create_async_engine(os.environ["LOOM_TEST_OBSERVER_DATABASE_URL"])
    repo = ObserverRepository(engine)
    run = await repo.open_run("run-1", "task-1")
    first = await repo.apply_patch(run.run_id, run.draft_version, run.draft_digest, "op-1", [{"kind": "set_result_expectation", "value": {"kind": "content"}}])
    replay = await repo.apply_patch(run.run_id, run.draft_version, run.draft_digest, "op-1", [{"kind": "set_result_expectation", "value": {"kind": "content"}}])
    assert replay.receipt == first.receipt
    with pytest.raises(Exception, match="version_conflict"):
        await repo.apply_patch(run.run_id, run.draft_version, run.draft_digest, "op-2", [])
```

- [ ] **Step 2: Run the integration test before implementation**

Run: `pytest -q -m integration tests/integration/test_repositories.py`

Expected: fixture/import failure because the repository and migration do not exist.

- [ ] **Step 3: Implement isolated database schemas and transaction repository**

`migrations/001_initial.sql` creates only role-local tables: Observer stores workspaces, conversations, runs, closure contracts/versions/patches, nodes, constraints, typed holes/bindings, executions/attempts/dispatches, events, outbox, and idempotency; Driver stores conversations/messages/turns/draft snapshots; Slave stores replica bindings, attempt ledger, receipts, staging and evidence. JSONB canonical snapshots are retained alongside indexed IDs/digests.

`db/session.py` creates an async engine from `LOOM_DATABASE_URL`; `db/base.py` exposes metadata; `db/models.py` maps the tables. `observer/repository.py` implements `open_run`, `apply_patch`, `inspect_readiness`, `commit_plan`, `start_run`, `append_event`, `close_run`, and idempotency receipts in one transaction. CAS checks both expected draft version and digest. The repository never lets a draft enter the dispatch table.

- [ ] **Step 4: Run unit and integration tests with a local PostgreSQL service**

Run: `docker compose -f deploy/docker-compose.test.yml up -d observer-db` then `pytest -q tests/contracts tests/integration/test_repositories.py`.

Expected: contract tests and repository tests pass; each test database is reset by `migrations/001_initial.sql`.

- [ ] **Step 5: Commit persistence**

```bash
git add loom_v2/db loom_v2/observer/repository.py migrations tests/integration scripts/wait-for-db.py
git commit -m "feat: add isolated postgres persistence and cas repository"
```

### Task 4: Implement Observer control plane and WorkerService

**Files:**
- Create: `loom_v2/observer/app.py`
- Create: `loom_v2/observer/service.py`
- Create: `loom_v2/observer/repository.py`
- Create: `loom_v2/observer/worker_api.py`
- Create: `tests/api/test_observer_run.py`
- Create: `tests/api/test_worker_fencing.py`

- [ ] **Step 1: Write failing API tests for the M0 lifecycle**

```python
# tests/api/test_observer_run.py
from fastapi.testclient import TestClient
from loom_v2.observer.app import create_app


def test_draft_patch_commit_and_start_are_explicit():
    client = TestClient(create_app())
    run = client.post("/api/v1/runs", json={"task_ref": "task-1", "goal": "echo"}).json()
    patch = client.post(f"/api/v1/runs/{run['run_id']}/patches", json={"operation_id": "op-1", "base_draft_version": run["draft_version"], "base_snapshot_digest": run["draft_digest"], "ops": [{"kind": "set_result_expectation", "value": {"kind": "content"}}]}).json()
    assert patch["kind"] == "draft"
    assert client.post(f"/api/v1/runs/{run['run_id']}/start", json={"closure_version": "draft"}).status_code == 409
    assert client.post(f"/api/v1/runs/{run['run_id']}/commit", json={"draft_version": patch["draft_version"], "draft_digest": patch["draft_digest"]}).status_code == 200
```

```python
# tests/api/test_worker_fencing.py
def test_stale_epoch_terminal_report_is_rejected():
    client = TestClient(create_app())
    response = client.post("/worker/v1/terminal", json={"attempt_id": "a-1", "execution_epoch": 1, "session_generation": 1, "operation_id": "terminal-1", "outcome": {"status": "completed"}})
    assert response.status_code == 409
    assert response.json()["code"] == "stale_execution_epoch"
```

- [ ] **Step 2: Run API tests to verify they fail**

Run: `pytest -q tests/api`

Expected: routes are missing and tests fail.

- [ ] **Step 3: Implement Observer routes and service methods**

Implement `POST /api/v1/runs`, `POST /api/v1/runs/{id}/patches`, `GET /api/v1/runs/{id}/readiness`, `POST /commit`, `POST /start`, `POST /close`, `GET /events`, `GET /capabilities`, and SSE `/api/v1/conversations/{id}/stream`. Implement `/worker/v1/register`, `/heartbeat`, `/poll`, `/ack`, `/progress`, `/terminal`, `/resource-events`, and `/capability-health`. Every mutation accepts `operation_id`, verifies workspace/session/epoch, writes an event and outbox record, and returns a receipt. `/terminal` rejects stale session generations and epochs with `DomainErrorEnvelope`.

- [ ] **Step 4: Run API tests and verify state transitions**

Run: `pytest -q tests/api`

Expected: lifecycle and fencing tests pass, including “draft cannot start” and duplicate operation replay.

- [ ] **Step 5: Commit Observer**

```bash
git add loom_v2/observer tests/api
git commit -m "feat: add observer lifecycle and worker service"
```

### Task 5: Implement Driver providers, continuous patching, and Codex app-server protocol

**Files:**
- Create: `loom_v2/driver/service.py`
- Create: `loom_v2/driver/tools.py`
- Create: `loom_v2/coding_agents/base.py`
- Create: `loom_v2/coding_agents/fake.py`
- Create: `loom_v2/coding_agents/codex.py`
- Create: `tests/driver/test_fake_flow.py`
- Create: `tests/driver/test_codex_protocol.py`

- [ ] **Step 1: Write failing provider tests**

```python
# tests/driver/test_fake_flow.py
import pytest
from loom_v2.coding_agents.fake import FakeCodingAgentProvider


@pytest.mark.asyncio
async def test_fake_provider_emits_multiple_patches_then_commit():
    provider = FakeCodingAgentProvider()
    events = [event async for event in provider.send_turn("echo")]
    assert [event.kind for event in events].count("apply_plan_patch") >= 2
    assert events[-1].kind == "commit_plan"
```

```python
# tests/driver/test_codex_protocol.py
def test_codex_provider_defaults_to_requested_model(monkeypatch):
    from loom_v2.coding_agents.codex import CodexAppServerProvider
    assert CodexAppServerProvider().model == "deepseek-v4-flash"
```

- [ ] **Step 2: Run provider tests to verify they fail**

Run: `pytest -q tests/driver`

Expected: missing provider classes.

- [ ] **Step 3: Implement the provider interface and Fake provider**

`base.py` defines `CodingAgentProvider.start/send_turn/interrupt/close`. `fake.py` deterministically emits: data/program patch, compute spec + typed hole patch, compute binding + constraint patch, readiness inspection, commit, start, and close. It uses the same Driver tool facade as Codex and never bypasses Observer.

- [ ] **Step 4: Implement Codex stdio JSONL handshake**

`codex.py` launches `codex app-server --listen stdio://` with `asyncio.create_subprocess_exec`, sends `initialize`, waits for the response, writes `initialized`, then sends `thread/start` with `model=os.getenv("LOOM_CODEX_MODEL", "deepseek-v4-flash")` and `turn/start`. It maps `item/*` and `turn/*` notifications to `AgentEvent`, treats process exit/protocol errors as `coding_agent_unavailable`, and never reads or writes the Codex config file. The Driver service selects Fake only when `LOOM_CODING_AGENT_BACKEND=fake` is explicit.

- [ ] **Step 5: Run provider tests and commit**

Run: `pytest -q tests/driver`

Expected: Fake flow and Codex handshake/model tests pass.

```bash
git add loom_v2/driver loom_v2/coding_agents tests/driver
git commit -m "feat: add fake and codex driver providers"
```

### Task 6: Implement Slave A/B execution, Replica state, and reassignment

**Files:**
- Create: `loom_v2/slave/service.py`
- Create: `loom_v2/slave/executor.py`
- Create: `loom_v2/slave/app.py`
- Create: `tests/slave/test_execution.py`
- Create: `tests/e2e/test_reassignment.py`

- [ ] **Step 1: Write failing execution and reassignment tests**

```python
# tests/slave/test_execution.py
import pytest
from loom_v2.slave.executor import execute_operation


@pytest.mark.asyncio
async def test_echo_execution_returns_resource_ref():
    result = await execute_operation("echo", {"text": "hello"})
    assert result.resource_ref.resource_id
    assert result.value == {"text": "hello"}
```

```python
# tests/e2e/test_reassignment.py
from fastapi.testclient import TestClient
from loom_v2.observer.app import create_app


def test_slave_a_loss_creates_new_attempt_without_new_execution():
    client = TestClient(create_app())
    run = client.post("/api/v1/runs", json={"task_ref": "task-1", "goal": "echo", "allow_reassignment": True}).json()
    committed = client.post(f"/api/v1/runs/{run['run_id']}/commit", json={"draft_version": run["draft_version"], "draft_digest": run["draft_digest"]}).json()
    started = client.post(f"/api/v1/runs/{run['run_id']}/start", json={"closure_version": committed["closure_version"]}).json()
    client.post("/api/v1/slaves/slave-a/availability", json={"available": False})
    client.post(f"/api/v1/runs/{run['run_id']}/reconcile", json={})
    state = client.get(f"/api/v1/runs/{run['run_id']}").json()
    assert state["execution_id"] == started["execution_id"]
    assert [attempt["target"] for attempt in state["attempts"]] == ["slave-a", "slave-b"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest -q tests/slave tests/e2e/test_reassignment.py`

Expected: missing slave executor/service.

- [ ] **Step 3: Implement executor and local ledger**

`executor.py` supports deterministic `echo`, `hash`, and `sort`; it writes result bytes only to a managed staging directory, returns a `ResourceRef` and digest, and records cleanup evidence. `service.py` polls Observer, fences stale execution epochs, ACKs exactly once, emits progress/terminal/resource events, and reports static built-in `TermSupport` for current demo terms. `WorkspaceReplica` states are `ready|dirty|unavailable`; raw paths never leave the Slave.

- [ ] **Step 4: Implement reassignment policy**

The Observer reconciliation service creates a second `Attempt` only when `allow_reassignment=true`, input refs are target-independent, executor effect is replay-safe, Slave B has a ready replica, and budget remains. It preserves `TaskRef`, `ClosureVersion`, and constraints, fences A’s old session/epoch, and appends provenance with target, reason, and policy decision.

- [ ] **Step 5: Run tests and commit**

Run: `pytest -q tests/slave tests/e2e/test_reassignment.py`

Expected: executor, fencing, replica, and reassignment tests pass.

```bash
git add loom_v2/slave loom_v2/observer tests/slave tests/e2e
git commit -m "feat: add slave execution and reassignment"
```

### Task 7: Build Conversation-first Web UI and SSE projection

**Files:**
- Create: `loom_v2/web/static/index.html`
- Create: `loom_v2/web/static/app.js`
- Create: `loom_v2/web/static/styles.css`
- Modify: `loom_v2/observer/app.py`
- Create: `tests/web/test_static_ui.py`

- [ ] **Step 1: Write failing UI smoke test**

```python
from fastapi.testclient import TestClient
from loom_v2.observer.app import create_app


def test_homepage_contains_conversation_and_run_drawer():
    client = TestClient(create_app())
    response = client.get("/")
    assert response.status_code == 200
    assert "Run drawer" in response.text
    assert "Event cursor" in response.text
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest -q tests/web/test_static_ui.py`

Expected: `404 Not Found`.

- [ ] **Step 3: Implement static UI and SSE client**

`index.html` contains the prompt timeline, backend/model indicator, prompt form, Run drawer (draft/committed version, blockers, nodes, attempts, provenance, resources), and Slave A/B status cards. `app.js` posts messages to Observer, opens `EventSource` for SSE, renders only allowlisted opaque IDs/status/error fields, and provides development Fake/Codex and Slave availability controls. It never stores or displays agent tokens. `styles.css` keeps the UI usable without a build tool.

- [ ] **Step 4: Run UI test and commit**

Run: `pytest -q tests/web/test_static_ui.py`

Expected: homepage smoke test passes.

```bash
git add loom_v2/web loom_v2/observer/app.py tests/web
git commit -m "feat: add conversation-first web experience"
```

### Task 8: Compose deployment, host Codex profile, and full verification

**Files:**
- Create: `Dockerfile`
- Create: `deploy/docker-compose.yml`
- Create: `deploy/docker-compose.test.yml`
- Create: `scripts/dev-up.sh`
- Create: `scripts/test-e2e.sh`
- Create: `scripts/migrate.sh`
- Modify: `README.md`

- [ ] **Step 1: Write deployment smoke checks**

```bash
#!/usr/bin/env bash
set -euo pipefail
curl -fsS http://localhost:8080/healthz
curl -fsS http://localhost:8080/ | grep -q "Run drawer"
docker compose -f deploy/docker-compose.test.yml ps --status running | grep -q observer-db
```

- [ ] **Step 2: Run checks before deployment**

Run: `bash scripts/test-e2e.sh`

Expected: fails because Dockerfiles/Compose/services are not yet present.

- [ ] **Step 3: Implement isolated Compose services**

`deploy/docker-compose.yml` defines `observer-db`, `driver-db`, `slave-a-db`, and `slave-b-db` with separate named volumes and credentials; Observer, Slave A, Slave B, and Web use only their own `DATABASE_URL`. The test profile adds a containerized Driver with `LOOM_CODING_AGENT_BACKEND=fake`. The default `scripts/dev-up.sh` starts Compose, applies migrations, then launches the host Driver with `LOOM_CODING_AGENT_BACKEND=codex`; it does not copy `~/.codex/config.toml` or credentials. Healthchecks wait for database readiness and `/healthz`.

- [ ] **Step 4: Run full tests, build, and deploy**

Run:

```bash
docker compose -f deploy/docker-compose.test.yml up --build --abort-on-container-exit --exit-code-from driver
pytest -q
bash scripts/dev-up.sh
bash scripts/test-e2e.sh
```

Expected: all unit/integration/E2E tests pass; four PostgreSQL containers, Observer, both Slaves and Web are healthy; browser at `http://localhost:8080` shows the Conversation-first page; Fake profile completes M0/M1; host Driver reports Codex model `deepseek-v4-flash` when local app-server is available and returns `coding_agent_unavailable` otherwise without Fake fallback.

- [ ] **Step 5: Commit deployment and documentation**

```bash
git add Dockerfile deploy scripts README.md
git commit -m "feat: add compose deployment and verification scripts"
```

### Task 9: Final self-review and handoff

**Files:**
- Modify: `docs/superpowers/specs/2026-08-24-single-user-workspace-v2-design.md`
- Modify: `README.md`

- [ ] **Step 1: Run the complete verification matrix**

Run: `git diff --check && pytest -q && docker compose -f deploy/docker-compose.test.yml config && bash scripts/test-e2e.sh`

Expected: no whitespace errors, all tests pass, Compose config validates, and the script prints service health plus the M0/M1 event/provenance summary.

- [ ] **Step 2: Check spec coverage**

Confirm the implementation has tests for continuous `apply_plan_patch`, closure/compute binding, required/advisory terms, unknown required failure, `TermSupport`, stale worker fencing, reassignment, policy denial, evidence, SSE, isolated databases, Fake/Codex selection, and no-secret logging. Record any deliberately deferred capability-package behavior under §11 roadmap rather than silently omitting it.

- [ ] **Step 3: Commit final verification notes**

```bash
git add docs/superpowers/specs/2026-08-24-single-user-workspace-v2-design.md README.md
git commit -m "docs: record loom v2 verification and roadmap"
```
