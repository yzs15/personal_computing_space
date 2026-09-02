# Run Lifecycle Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement the Run lifecycle state machine, structured execution outcomes, repair/attestation decisions, epoch fencing, and single-Driver receipt recovery defined in the redesign spec.

**Architecture:** Observer remains the authoritative Run state machine. DriverMCP invokes one Driver-local `execute_and_wait` callable and returns a structured business outcome to the coding agent; protocol failures remain MCP errors. Receipt ownership uses an opaque `claim_token` and Driver-epoch fencing without time expiry or heartbeat.

**Tech Stack:** Python 3.12, asyncio, FastAPI, SQLAlchemy, PostgreSQL/SQLite test repositories, pytest, existing MCP and Worker protocols.

---

### Task 1: Define failing lifecycle and outcome tests

**Files:**
- Create: `tests/api/test_run_lifecycle.py`
- Modify: `tests/api/test_observer_run.py`
- Modify: `tests/driver/test_mcp.py`
- Modify: `tests/driver/test_service.py`
- Modify: `tests/observer/test_message_receipts.py`

- [ ] **Step 1: Add state-transition tests first**

  Cover `thinking → committed → running`, readiness failure staying `thinking`, execution/schema failure becoming `awaiting_decision(repair)`, attestation becoming `awaiting_decision(attestation)`, cancellation returning `cancelled`, and illegal transitions raising `illegal_state_transition`.

- [ ] **Step 2: Add outcome and decision tests first**

  Assert `resolve(accept)` is limited to attestation, preserves `terminal_state=decision_required` and `resource_ref`, clears `decision`, and changes `disposition` to `completed`; assert repair patch reopens to `thinking` and clears the current outcome.

- [ ] **Step 3: Add rerun and fencing tests first**

  Assert first start uses epoch 1, rerun creates a new execution id and increments the epoch, and a result with an old execution/epoch/attempt is rejected.

- [ ] **Step 4: Add DriverMCP and service tests first**

  Assert `loom_start_run` waits for `completed`, `failed`, `awaiting_decision`, or `cancelled`; business execution failure is returned with `success=true` and structured outcome; local, remote, and fake start paths dispatch exactly once.

- [ ] **Step 5: Add receipt fencing tests first**

  Assert receipts have no expiry field, duplicate requests remain deduplicated, old claim tokens are rejected after a Driver epoch change, and previous-epoch in-flight receipts become retryable during registration recovery.

- [ ] **Step 6: Run the new tests and confirm expected failures**

  Run: `pytest tests/api/test_run_lifecycle.py tests/api/test_observer_run.py tests/driver/test_mcp.py tests/driver/test_service.py tests/observer/test_message_receipts.py -q`

  Expected: failures identify the missing state machine, outcome, MCP, and receipt behavior rather than test collection errors.

### Task 2: Implement Observer lifecycle authority

**Files:**
- Modify: `loom_v2/observer/repository.py`
- Modify: `loom_v2/observer/app.py`
- Modify: `loom_v2/driver/mcp.py`
- Modify: `loom_v2/web/static/app.js`

- [ ] **Step 1: Add the complete `RUN_TRANSITIONS` table and `_transition()`**

  Route every `RunRecord.state` write through the helper, emit one compact transition event, and enforce decision guards (`repair` for reopen, `attestation` for accept). Keep Worker/Slave `terminal_state` values unchanged.

- [ ] **Step 2: Migrate every Run state writer**

  Update refinement, commit, start, record-result, dynamic-node completion/failure, orchestration completion, fail, cancel, close, and startup recovery paths. Readiness guard failures must persist the existing `thinking` state without transitioning.

- [ ] **Step 3: Normalize outcomes and decisions**

  Add `disposition`/`decision` invariants, preserve raw execution fields, retain attestation `resource_ref`, clear outcome on repair reopen, and add `resolve_run()` with idempotent accept/abandon behavior.

- [ ] **Step 4: Implement epoch-safe start and result handling**

  Use epoch 1 for the first execution, increment atomically for reruns, preserve historical attempts, and reject mismatched execution id/epoch/attempt reports.

- [ ] **Step 5: Update projections and MCP tools**

  Replace Run-level `decision_required` projections with `awaiting_decision`, expose `loom_resolve_run`, return complete outcome from status/start tools, and add the UI label.

- [ ] **Step 6: Run focused Observer tests and make them pass**

  Run: `pytest tests/api/test_run_lifecycle.py tests/api/test_observer_run.py tests/api/test_readiness.py tests/api/test_dynamic_nodes.py -q`

  Expected: all lifecycle, readiness, epoch, validation, and projection tests pass.

### Task 3: Implement blocking execution feedback in Driver

**Files:**
- Modify: `loom_v2/driver/mcp.py`
- Modify: `loom_v2/driver/service.py`
- Modify: `loom_v2/coding_agents/fake.py`
- Modify: `loom_v2/coding_agents/codex.py`

- [ ] **Step 1: Add one `execute_and_wait` binding**

  Inject one private callable into `DriverMCP`; bind local and remote Driver implementations to existing dispatch logic, and make fake `start_run` events use the same helper.

- [ ] **Step 2: Move dispatch into the `loom_start_run` handler**

  Start the execution, await the executor, read the final Run record, and return the structured outcome. Remove post-tool-call dispatch from both Driver event loops so one tool call cannot execute twice.

- [ ] **Step 3: Separate business outcomes from MCP failures**

  Return `success=true` for `awaiting_decision(repair)` and attestation outcomes. Reserve `success=false` for invalid arguments, illegal transitions, unavailable transport, or provider protocol failures; preserve structured error payloads.

- [ ] **Step 4: Handle cancellation and Driver errors**

  Wake blocked start calls on Run cancellation, return `cancelled`, and map deadline/coding-agent failures through the centralized Observer transition without overwriting a completed execution.

- [ ] **Step 5: Run focused Driver tests and make them pass**

  Run: `pytest tests/driver/test_mcp.py tests/driver/test_service.py tests/driver/test_fake_flow.py tests/driver/test_interrupt.py -q`

  Expected: MCP feedback, single-dispatch, cancellation, structured failure, and existing provider tests pass.

### Task 4: Remove receipt expiry and add migration 008

**Files:**
- Modify: `loom_v2/contracts/messages.py`
- Modify: `loom_v2/db/models.py`
- Modify: `loom_v2/observer/repository.py`
- Modify: `loom_v2/observer/dispatcher.py`
- Modify: `scripts/migrate.sh`
- Create: `migrations/008_run_lifecycle_state_machine.sql`

- [ ] **Step 1: Remove expiry fields and renewal code**

  Delete `claim_expires_at` from the contract/model/row mapping and remove TTL checks, claim TTL arguments, and renewal writes. Keep claim-token validation and request-id idempotency.

- [ ] **Step 2: Fence and recover receipts by Driver epoch**

  On a new Driver registration epoch, atomically clear claims and move previous-epoch `in_flight` receipts to `retryable`; reject late updates carrying an old token. Do not add a heartbeat task.

- [ ] **Step 3: Add idempotent migration SQL**

  Map old Run rows by their raw `outcome.terminal_state`, update Run state/outcome fields once, and drop `message_receipts.claim_expires_at` with `IF EXISTS`.

- [ ] **Step 4: Execute migration from the existing deployment script**

  Make `scripts/migrate.sh` initialize tables, then execute 008 through the `observer-db` PostgreSQL container. Do not introduce a new migration framework.

- [ ] **Step 5: Run database and receipt tests and make them pass**

  Run: `pytest tests/db/test_message_receipt_db.py tests/db/test_persistence.py tests/observer/test_message_receipts.py -q`

  Expected: receipt persistence, epoch recovery, migration, and existing database tests pass.

### Task 5: Align timeout/projection regressions and run the suite

**Files:**
- Modify: `loom_v2/settings.py`
- Modify: `.env.example`
- Modify: `README.md`
- Modify: `tests/api/test_message_receipt_api.py`
- Modify: `tests/deploy/test_compose_config.py`

- [ ] **Step 1: Make Driver deadline authoritative**

  Keep `LOOM_CODING_AGENT_DEADLINE_SECONDS` as the only execution deadline; configure Observer forward timeout as transport-only and never shorter than the Driver deadline.

- [ ] **Step 2: Update API and UI projections**

  Ensure 202 responses remain in-flight while the Driver turn runs and expose normalized `awaiting_decision` status/outcome through conversation and run APIs.

- [ ] **Step 3: Run regression tests**

  Run: `pytest -q`

  Expected: the full non-integration suite passes with lifecycle expectations updated and no remaining Run-level `decision_required` assertions.

- [ ] **Step 4: Run integration/deployment checks**

  Run: `pytest tests/integration -q && pytest tests/deploy/test_compose_config.py -q`

  Expected: integration persistence, IO validation, distributed orchestration, and Compose checks pass.
