# Dynamic Orchestration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement the approved programmatic distributed-task design: a content-addressed `orchestrator_python_v1` package runs in a Docker sandbox, emits deterministic `NodeIntent`s, and executes authoritative `DynamicNode`s through the existing Observer/Worker boundaries.

**Architecture:** Extend the existing capability-package and closure models with orchestration policy and dynamic-node records. A Driver-side Docker executor speaks a JSON-lines protocol with a restricted Python context; the Observer validates and persists each intent, while a runtime schedules node attempts, stores canonical outputs, and completes the parent execution.

**Tech Stack:** Python 3.12+, Pydantic v2, asyncio subprocess/JSON-lines, Docker CLI, SQLAlchemy JSON persistence, ContentStore, pytest.

---

### Task 1: Add orchestration contracts

**Files:** `loom_v2/contracts/types.py`, `loom_v2/contracts/__init__.py`, `tests/contracts/test_dynamic_orchestration.py`

- [x] Add `allowed_node_package_refs`, `max_nodes`, and `max_live_nodes` to `CapabilityPackageVersion`; validate these fields only for `orchestrator_python_v1` packages and include them in `package_digest`.
- [x] Add strict `NodeIntent` and `DynamicNode` models and export them.
- [x] Run `pytest tests/contracts/test_dynamic_orchestration.py -q` and require all contract validation and digest assertions to pass.

### Task 2: Implement the Docker executor

**Files:** `loom_v2/driver/orchestrator.py`, `loom_v2/settings.py`, `tests/driver/test_orchestrator.py`

- [x] Validate UTF-8 Python source, syntax, `async def orchestrate`, and bounded source/message sizes before launch.
- [x] Launch one container with `--network none`, `--read-only`, non-root UID, dropped capabilities, fixed image, and resource limits; use stdin/stdout JSON-lines and redirect program stdout to stderr.
- [x] Implement `read_json`, `emit_node`, `result`, structured `OrchestrationFailure`, and final-ref validation.
- [x] Run unit tests and the Docker-marked integration tests when the configured image is available.

### Task 3: Persist and admit dynamic nodes

**Files:** `loom_v2/observer/repository.py`, `loom_v2/db/models.py`, `loom_v2/observer/app.py`, `tests/api/test_dynamic_nodes.py`

- [x] Persist `dynamic_nodes` in in-memory and SQL projections and expose them in run/conversation state.
- [x] Implement deterministic intent IDs, allowlist/digest/input-schema/target/limit checks, `node_requested` and `node_accepted` events, and authoritative `DynamicNode` materialization.
- [x] Add node dispatch/result methods with attempt/epoch fencing and canonical ContentStore outputs.
- [x] Run the dynamic-node API tests.

### Task 4: Wire Driver orchestration runtime

**Files:** `loom_v2/driver/orchestration_runtime.py`, `loom_v2/driver/service.py`, `tests/driver/test_dynamic_orchestration_runtime.py`

- [x] Start the Docker program after `loom_start_run`, bridge context callbacks to Observer and WorkerSession, and return result refs.
- [x] Select targets deterministically using availability, executor capability, activation preference, locality/permission constraints, and stable tie-breaking.
- [x] Enforce `max_live_nodes`, propagate node failures, and complete the parent only after final-ref, output-schema, validator, budget, and lineage checks.
- [x] Run runtime tests with a mocked executor and real Docker/Slave integration where available.

### Task 5: Add scenario B stress coverage

**Files:** `tests/e2e/test_dynamic_orchestration_stress.py`

- [x] Build schemas, contracts, node package, orchestration package, and input exclusively through MCP/content APIs.
- [x] Assert one dynamic summarize node, target selection, terminal validation, content-addressed output, reassignment fencing, invalid-input rejection, and event replay.

### Task 6: Add scenario A distributed analysis

**Files:** `tests/e2e/test_dynamic_distributed_analysis.py`

- [x] Emit two summarize nodes from a partition document, execute them with bounded parallelism, emit merge, and return the merge output.
- [x] Assert intermediate/final ContentStore refs, two target decisions, provenance back to packages/attempts/Slaves, and runtime fan-out without static closure nodes.

### Task 7: Recovery, safety, and regression verification

**Files:** dynamic orchestration modules and adjacent tests as needed

- [x] Replay deterministic orchestration from persisted events after Driver restart and reuse completed replay-safe nodes.
- [x] Reject non-replayable programs and side effects (filesystem, network, environment, direct Observer/Slave access) with structured failures.
- [x] Run targeted tests, `python -m compileall loom_v2`, then the full `pytest -q` suite; report any unrelated baseline failures without changing them.
