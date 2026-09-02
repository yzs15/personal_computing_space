# Message Turn Reliability Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace in-memory message forwarding and shared Codex turn state with a durable Observer receipt state machine, a single recoverable dispatcher, per-conversation FIFO scheduling, and owner-scoped provider turn contexts.

**Architecture:** Observer creates the durable `(workspace_id, request_id)` receipt before returning `202`. One Observer dispatcher delivers queued receipts to the active Driver. Driver claims receipts through fenced Observer commands, serializes conversation turns through a FIFO plus one global Codex lane, and runs each turn in an owner-scoped `TurnContext`. Observer receipts and existing Run/thread tables remain authoritative; process-local maps are only scheduling aids.

**Tech Stack:** FastAPI, SQLAlchemy async sessions, Pydantic v2 contracts, asyncio locks/queues, httpx, Codex app-server JSON-RPC, pytest/pytest-asyncio.

---

## File map

- Create `loom_v2/contracts/messages.py` for receipt DTOs and explicit state values.
- Create `loom_v2/observer/dispatcher.py` for the single Observer delivery loop.
- Create `loom_v2/driver/coordinator.py` for per-conversation FIFO and the global Codex lane.
- Create `loom_v2/coding_agents/turn.py` for the provider-independent `TurnContext` data object.
- Modify `loom_v2/db/models.py` with the `message_receipts` table and indexes.
- Create `migrations/007_message_turn_reliability.sql` with the PostgreSQL migration.
- Modify `loom_v2/observer/repository.py` with receipt persistence, CAS transitions, command handlers, and receipt-backed conversation projections.
- Modify `loom_v2/observer/app.py` to create receipts, start/stop the dispatcher, and remove `forward_tasks`/embedded production Driver behavior.
- Modify `loom_v2/observer/gateway.py` only where dispatcher lifecycle injection requires it; keep the existing timeout override.
- Modify `loom_v2/driver/app.py` and `loom_v2/driver/service.py` to submit messages to the coordinator and report receipt transitions.
- Modify `loom_v2/coding_agents/base.py`, `loom_v2/coding_agents/codex.py`, and `loom_v2/coding_agents/fake.py` to use context-scoped turns.
- Modify `loom_v2/driver/control_client.py` and `loom_v2/driver/remote_repository.py` for receipt claim/update/release commands.
- Modify `loom_v2/contracts/agents.py` to remove duplicate epoch aliases in newly introduced receipt commands while retaining the existing registry storage mapping.
- Modify `loom_v2/settings.py`, `.env.example`, and `README.md` only for the dispatcher retry interval if the implementation needs a configurable value; use a fixed bounded backoff otherwise.
- Create/update focused tests under `tests/observer`, `tests/driver`, `tests/coding_agents`, `tests/api`, `tests/db`, and `tests/e2e`.

## Task 1: Add receipt contract, database model, and migration

**Files:**
- Create: `loom_v2/contracts/messages.py`
- Modify: `loom_v2/db/models.py`
- Create: `migrations/007_message_turn_reliability.sql`
- Test: `tests/contracts/test_messages.py`
- Test: `tests/db/test_message_receipts.py`

- [ ] **Step 1: Write failing contract tests**

Add tests for a receipt payload and state validation:

```python
def test_message_receipt_requires_workspace_request_and_digest():
    receipt = MessageReceipt(
        workspace_id="workspace-default",
        request_id="request-1",
        conversation_ref="conversation-1",
        prompt="hello",
        payload_digest="a" * 64,
        state="accepted",
        attempt_count=0,
    )
    assert (receipt.workspace_id, receipt.request_id) == ("workspace-default", "request-1")


def test_message_receipt_rejects_unknown_state():
    with pytest.raises(ValidationError):
        MessageReceipt(
            workspace_id="workspace-default",
            request_id="request-1",
            conversation_ref="conversation-1",
            prompt="hello",
            payload_digest="a" * 64,
            state="unknown",
            attempt_count=0,
        )
```

Run: `pytest tests/contracts/test_messages.py -q`

Expected: FAIL because `MessageReceipt` and its state type do not exist.

- [ ] **Step 2: Implement the receipt DTO**

Define a literal state type containing exactly `accepted`, `queued`, `in_flight`, `completed`, `retryable`, `failed`, and `interrupted`. Define `MessageReceipt` with the reduced schema from the spec: composite identity, conversation/prompt/digest, state, optional `run_id`, `assistant_text`, `outcome`, `claim_token`, `claim_expires_at`, `attempt_count`, `next_attempt_at`, and timestamps. Model `outcome` as a JSON-compatible dictionary and require a 64-character lowercase digest at the boundary.

- [ ] **Step 3: Write the database model and migration**

Add `MessageReceiptRow` to `loom_v2/db/models.py` with composite primary key `(workspace_id, request_id)`, a conversation/created index, and a state/next-attempt index. Use `Text` for prompt and assistant text, `JSON` for outcome, timezone-aware `DateTime` for timestamps, and `Integer` for attempt count.

Create `migrations/007_message_turn_reliability.sql`:

```sql
CREATE TABLE IF NOT EXISTS message_receipts (
    workspace_id VARCHAR(128) NOT NULL,
    request_id VARCHAR(256) NOT NULL,
    conversation_ref VARCHAR(256) NOT NULL,
    prompt TEXT NOT NULL,
    payload_digest VARCHAR(64) NOT NULL,
    state VARCHAR(32) NOT NULL,
    run_id VARCHAR(128),
    assistant_text TEXT,
    outcome JSONB,
    claim_token VARCHAR(128),
    claim_expires_at TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (workspace_id, request_id)
);
CREATE INDEX IF NOT EXISTS idx_message_receipts_conversation
    ON message_receipts (workspace_id, conversation_ref, created_at);
CREATE INDEX IF NOT EXISTS idx_message_receipts_dispatch
    ON message_receipts (state, next_attempt_at);
```

- [ ] **Step 4: Add persistence tests and run them**

Use the existing in-memory SQLite repository fixture to create, reload, and list a receipt. Assert the composite key rejects a duplicate with a different digest and that the table is included by `ObserverRepository.init_db()`.

Run: `pytest tests/contracts/test_messages.py tests/db/test_message_receipts.py -q`

Expected: PASS.

## Task 2: Implement Observer receipt state machine and command handlers

**Files:**
- Modify: `loom_v2/observer/repository.py`
- Modify: `loom_v2/contracts/agents.py`
- Modify: `loom_v2/driver/control_client.py`
- Modify: `loom_v2/driver/remote_repository.py`
- Test: `tests/observer/test_message_receipts.py`
- Test: `tests/driver/test_control_client.py`

- [ ] **Step 1: Write failing receipt transition tests**

Cover these exact transitions:

```python
receipt = await repo.create_or_get_message_receipt(
    "workspace-default", "request-1", "conversation-1", "hello"
)
assert receipt.state == "accepted"

queued = await repo.queue_message_receipt(receipt.workspace_id, receipt.request_id)
assert queued.state == "queued"

claimed = await repo.claim_message_receipt(
    receipt.workspace_id,
    receipt.request_id,
    payload_digest=receipt.payload_digest,
    claim_token="claim-1",
    claim_ttl_seconds=30,
)
assert claimed.state == "in_flight"

duplicate = await repo.claim_message_receipt(
    receipt.workspace_id,
    receipt.request_id,
    payload_digest=receipt.payload_digest,
    claim_token="claim-2",
    claim_ttl_seconds=30,
)
assert duplicate.state == "in_flight"
assert duplicate.claim_token == "claim-1"
```

Also test digest mismatch (`request_id_reused`), stale claim token rejection, terminal state immutability, lease expiry re-claim, and interrupt of a queued receipt.

Run: `pytest tests/observer/test_message_receipts.py -q`

Expected: FAIL because the repository methods do not exist.

- [ ] **Step 2: Implement in-memory and SQL receipt access**

Add repository methods with identical semantics for the no-session test repository and SQLAlchemy-backed repository:

```python
async def create_or_get_message_receipt(
    self, workspace_id: str, request_id: str,
    conversation_ref: str, prompt: str,
) -> MessageReceipt: ...

async def get_message_receipt(self, workspace_id: str, request_id: str) -> MessageReceipt | None: ...

async def list_dispatchable_message_receipts(self, workspace_id: str) -> list[MessageReceipt]: ...

async def queue_message_receipt(self, workspace_id: str, request_id: str) -> MessageReceipt: ...

async def claim_message_receipt(
    self, workspace_id: str, request_id: str,
    *, payload_digest: str, claim_token: str, claim_ttl_seconds: float,
) -> MessageReceipt: ...

async def update_message_receipt(
    self, workspace_id: str, request_id: str,
    *, claim_token: str, state: str | None = None,
    assistant_text: str | None = None, run_id: str | None = None,
    outcome: dict[str, Any] | None = None,
) -> MessageReceipt: ...

async def release_message_receipt(self, workspace_id: str, request_id: str, *, claim_token: str) -> MessageReceipt: ...
```

Use conditional SQL updates for claim/update/release. A stale token or terminal receipt raises a structured `ValueError`; never overwrite a completed receipt. On new Driver registration, convert all current `in_flight` claims to `retryable` and clear their claim tokens because only one Driver lease is active in this deployment.

- [ ] **Step 3: Add explicit Driver commands**

Extend the command allowlist with `message.claim`, `message.update`, and `message.release`. Dispatch each command to the repository methods after the existing Driver lease, workspace, and epoch checks. Return a serialized `MessageReceipt` plus `claim_token` for a successful claim. Do not reuse `message.append`, which requires a `run_id`.

Add `ObserverControlClient.claim_message()`, `update_message()`, and `release_message()` methods. Add matching `RemoteObserverRepository` adapters only if DriverService needs repository-shaped access; all methods must carry the original request id and payload digest.

- [ ] **Step 4: Run focused command tests**

Run: `pytest tests/observer/test_message_receipts.py tests/driver/test_control_client.py tests/driver/test_remote_repository.py -q`

Expected: PASS.

## Task 3: Make conversation projection receipt-aware

**Files:**
- Modify: `loom_v2/observer/repository.py`
- Test: `tests/api/test_conversation.py`
- Test: `tests/observer/test_message_receipts.py`

- [ ] **Step 1: Write the no-Run projection regression test**

Create a completed receipt with no `run_id` and `assistant_text="plain reply"`, then assert `get_conversation("conversation-1")` returns user and assistant messages and `status == "completed"` instead of raising `KeyError`.

Run: `pytest tests/api/test_conversation.py::test_receipt_only_conversation_is_queryable -q`

Expected: FAIL with `conversation_not_found`.

- [ ] **Step 2: Merge receipts into conversation reads**

Update `list_conversations()` and `get_conversation()` to include receipts ordered by `created_at`. For receipts with no Run, synthesize the user message from `prompt` and assistant message from `assistant_text`. For Run-backed receipts, use the receipt as the canonical conversation message and keep Run events for execution details; do not append duplicate user/assistant messages from Run events in the projection.

Emit receipt state changes as Observer events consumed by the existing stream endpoint.

- [ ] **Step 3: Run projection tests**

Run: `pytest tests/api/test_conversation.py tests/observer/test_message_receipts.py -q`

Expected: PASS, with existing Run-backed conversation tests updated to the receipt-backed message shape.

## Task 4: Replace `forward_tasks` with a single recoverable Observer dispatcher

**Files:**
- Create: `loom_v2/observer/dispatcher.py`
- Modify: `loom_v2/observer/app.py`
- Modify: `loom_v2/settings.py` only if a dispatcher interval is required
- Test: `tests/observer/test_dispatcher.py`
- Test: `tests/api/test_driver_gateway.py`

- [ ] **Step 1: Write dispatcher failure and restart tests**

Test that a dispatcher sends one receipt at a time, marks a pre-claim transport failure `retryable`, leaves a claimed receipt `in_flight` after timeout, and re-scans a queued receipt after the dispatcher is recreated. Assert no exception is swallowed without a receipt state change.

Run: `pytest tests/observer/test_dispatcher.py -q`

Expected: FAIL because the dispatcher module and receipt-backed app lifecycle do not exist.

- [ ] **Step 2: Implement the single dispatcher**

Implement `ObserverMessageDispatcher.start()`, `stop()`, `wake()`, and a loop that:

1. Lists due `accepted`/`retryable` receipts for the configured workspace.
2. Changes one receipt to `queued`.
3. Calls `ObserverDriverGateway.forward("/driver/v1/messages", payload, timeout=settings.observer_forward_timeout_seconds)`.
4. On a response, leaves terminal/in-flight state to Driver receipt commands.
5. On a transport failure, reloads the receipt; changes it to `retryable` only if it has no claim, with bounded exponential `next_attempt_at` backoff.
6. Sleeps or waits on `wake()` before scanning again.

The loop is the only dispatcher instance in the Observer process. It processes one delivery at a time and does not maintain a correctness-critical in-memory dedup map.

- [ ] **Step 3: Make the public message endpoint receipt-first**

In `loom_v2/observer/app.py`, validate `request_id`, `conversation_ref`, and text, call `create_or_get_message_receipt()` before returning, wake the dispatcher, and return `202` with the existing receipt state. Remove `_forward_background`, `forward_tasks`, and the catch-all exception path. Duplicate payloads return the stored state; digest mismatches return `409 request_id_reused`.

Keep Observer as the only public entry point. The endpoint never waits for Driver or Codex.

- [ ] **Step 4: Wire startup/shutdown and interrupt**

Create and start one dispatcher during Observer startup and stop it during shutdown. Interrupt first checks the receipt/coordinator state: queued receipts become `interrupted`; in-flight receipts are forwarded to Driver for context-scoped interruption.

- [ ] **Step 5: Run gateway and dispatcher tests**

Run: `pytest tests/observer/test_dispatcher.py tests/api/test_driver_gateway.py tests/api/test_conversation.py -q`

Expected: PASS. Update the old test that expected a production `503` before Driver registration to assert a persisted accepted receipt when the dispatcher has no active Driver.

## Task 5: Introduce provider-independent `TurnContext` and Driver coordinator

**Files:**
- Create: `loom_v2/coding_agents/turn.py`
- Create: `loom_v2/driver/coordinator.py`
- Modify: `loom_v2/driver/service.py`
- Modify: `loom_v2/driver/app.py`
- Test: `tests/driver/test_coordinator.py`

- [ ] **Step 1: Write coordinator tests**

Use a fake provider that records context identities. Assert that two requests in one conversation execute FIFO, two different conversations never overlap the global lane, a duplicate request id never creates a second context, and interrupt selects the active context for the requested conversation.

Run: `pytest tests/driver/test_coordinator.py -q`

Expected: FAIL because `TurnContext` and `DriverTurnCoordinator` do not exist.

- [ ] **Step 2: Define `TurnContext`**

Create a dataclass containing the approved fields:

```python
@dataclass
class TurnContext:
    workspace_id: str
    conversation_ref: str
    request_id: str
    claim_token: str
    driver_epoch: int
    owner_generation: str
    thread_id: str | None = None
    turn_id: str | None = None
    dynamic_tools: list[dict[str, Any]] = field(default_factory=list)
    mcp_handler: Callable[[str, dict[str, Any]], Awaitable[dict[str, Any]]] | None = None
    process: Any | None = None
    read_buffer: bytearray = field(default_factory=bytearray)
    read_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    write_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    state: str = "starting"
    interrupt_requested: bool = False
```

The context is ephemeral. Receipt, Run, and thread binding remain the persisted sources of truth.

- [ ] **Step 3: Implement the coordinator**

Implement `submit(receipt, prompt)`, `interrupt(conversation_ref)`, and `shutdown()`. Keep a FIFO deque per conversation and one global `asyncio.Lock`. The coordinator calls `message.claim` before enqueuing, stores only active/waiting Futures in memory, creates a context after acquiring the global lane, invokes the existing Driver turn execution callback, and removes the context in a `finally` block. A claim result of `in_flight`, `completed`, or `failed` must not create a second context.

- [ ] **Step 4: Connect Driver HTTP and service**

Make `/driver/v1/messages` require the receipt fields (`request_id`, `conversation_ref`, `text`, `payload_digest`), then submit to the coordinator. Replace `DriverService.active_turns` and `_request_results` with coordinator state. `interrupt()` delegates to the coordinator and never looks up a mutable shared provider turn.

- [ ] **Step 5: Run coordinator tests**

Run: `pytest tests/driver/test_coordinator.py tests/driver/test_driver_app.py tests/driver/test_remote_service.py -q`

Expected: PASS after updating fake controls to implement the explicit receipt commands.

## Task 6: Refactor Codex and fake providers to context ownership

**Files:**
- Modify: `loom_v2/coding_agents/base.py`
- Modify: `loom_v2/coding_agents/codex.py`
- Modify: `loom_v2/coding_agents/fake.py`
- Modify: `loom_v2/driver/service.py`
- Test: `tests/driver/test_codex_protocol.py`
- Test: `tests/driver/test_provider_context.py`

- [ ] **Step 1: Add failing isolation and owner tests**

Add tests that two contexts retain separate handlers, a stale context cannot close a newer process, interrupt uses the context thread/turn, and start failures release only their own owner. Keep the existing read/write serialization tests and add a concurrent dynamic-tool test whose expected handlers are `A` and `B` respectively.

Run: `pytest tests/driver/test_provider_context.py tests/driver/test_codex_protocol.py -q`

Expected: FAIL with shared-handler or unowned-close behavior.

- [ ] **Step 2: Update the provider protocol**

Replace the old start/send/close lifecycle with context methods:

```python
class CodingAgentProvider(Protocol):
    async def begin_turn(
        self, conversation_ref: str, request_id: str,
        workspace_root: str, existing_thread_id: str | None,
        tools: list[dict[str, Any]],
        handler: Callable[[str, dict[str, Any]], Awaitable[dict[str, Any]]],
    ) -> TurnContext: ...
    def send_turn(self, context: TurnContext, user_message: str) -> AsyncIterator[AgentEvent]: ...
    async def interrupt(self, context: TurnContext) -> None: ...
    async def end_turn(self, context: TurnContext) -> None: ...
    async def force_shutdown(self) -> None: ...
```

- [ ] **Step 3: Implement Codex context lifecycle**

Move process/thread/turn state from provider globals into the context. Bind dynamic tools and the handler only after the coordinator has acquired the global lane. Keep `_read_lock` and `_write_lock` around the context’s stdio operations. Make `end_turn(context)` verify `owner_generation` before terminating or releasing anything; stale end calls are ignored or rejected without touching the active context. `force_shutdown()` is only used by Driver shutdown.

- [ ] **Step 4: Update Fake provider and DriverService**

Make the fake provider create contexts with deterministic thread ids and emit the same events. Adapt DriverService’s existing remote execution loop to pass context and call `message.update` for every assistant fragment and terminal state. Remove all calls to `set_tool_handler`, `provider.close()`, and the old two-argument `start()` API from production paths.

- [ ] **Step 5: Run provider and service tests**

Run: `pytest tests/driver/test_provider_context.py tests/driver/test_codex_protocol.py tests/driver/test_remote_service.py tests/driver/test_service.py -q`

Expected: PASS with no shared-handler or lock-ownership failures.

## Task 7: Remove the obsolete Observer embedded production path

**Files:**
- Modify: `loom_v2/observer/app.py`
- Modify: `loom_v2/driver/app.py`
- Modify: `tests/api/test_conversation.py`
- Modify: `tests/api/test_runtime.py`
- Modify: `tests/driver/test_driver_app.py`
- Modify: `tests/driver/test_fake_flow.py`

- [ ] **Step 1: Add explicit dependency-injection test setup**

Construct Observer tests with an injected gateway/repository and Driver tests with an injected fake provider/control client. Assert Observer startup does not instantiate `CodexAppServerProvider`, `SlaveService`, `WorkerSession`, or `DriverMCPServer`.

- [ ] **Step 2: Delete the implicit `legacy_embedded` branch**

Remove the `legacy_embedded` conditional and all Observer-side provider/slave/worker construction. Keep only public Observer routes, repository, gateway, receipt dispatcher, and Driver command handlers. The fake provider remains available only through explicit test injection.

- [ ] **Step 3: Update runtime and API expectations**

When no active Driver exists, `/api/v1/runtime` returns the structured `503 driver_unavailable` response. `/api/v1/messages` still creates a receipt and returns `202`; it does not synchronously fall back to an embedded Driver.

- [ ] **Step 4: Run boundary tests**

Run: `pytest tests/api/test_runtime.py tests/api/test_conversation.py tests/driver/test_driver_app.py tests/driver/test_fake_flow.py -q`

Expected: PASS with no production embedded Driver objects.

## Task 8: End-to-end validation and documentation

**Files:**
- Modify: `tests/e2e/test_driver_restart_thread_resume.py`
- Modify: `tests/e2e/test_dynamic_distributed_analysis.py`
- Modify: `tests/e2e/test_dynamic_orchestration_stress.py`
- Modify: `tests/api/test_driver_gateway.py`
- Modify: `README.md` only where runtime status/receipt polling changed
- Modify: `.env.example` only if new dispatcher setting is introduced

- [ ] **Step 1: Add receipt-driven E2E tests**

Cover:

```text
POST /api/v1/messages -> 202 accepted
GET /api/v1/conversations/{conversation_ref} -> queued/in_flight
Driver completes -> receipt completed
GET conversation -> user + assistant/result
```

Repeat the POST concurrently with the same request id and assert one Codex user turn. Repeat after Observer restart and Driver restart; assert the same thread id and no stale-epoch writes.

- [ ] **Step 2: Run focused E2E tests**

Run: `pytest tests/e2e/test_driver_restart_thread_resume.py tests/e2e/test_dynamic_distributed_analysis.py tests/api/test_driver_gateway.py -q`

Expected: PASS.

- [ ] **Step 3: Run the complete non-integration suite**

Run: `PYTHONPATH=. pytest -q -m 'not integration'`

Expected: all non-integration tests pass, with no receipt/turn regressions.

- [ ] **Step 4: Validate deployment configuration**

Run: `pytest tests/deploy/test_compose_config.py -q` and `docker compose -f deploy/docker-compose.yml config`.

Expected: Compose still exposes only Observer publicly, mounts Codex/Docker resources only in Driver, and starts one Observer dispatcher.

- [ ] **Step 5: Update runtime documentation**

Document that `202` is receipt acceptance, conversation polling exposes terminal state, and the current deployment intentionally runs one Observer dispatcher and one global Codex lane. Keep the host proxy base URL and optional Codex API key behavior unchanged.

## Self-review checklist

- [ ] Every spec requirement maps to Tasks 1–8.
- [ ] No task relies on `forward_tasks`, `_request_results`, shared `tool_handler`, or an unowned provider close.
- [ ] Receipt fields are not duplicated with `driver_threads`; `outcome` is used instead of separate result/error columns.
- [ ] Single-dispatcher scope is explicit; no multi-worker lease protocol is introduced.
- [ ] All tests use explicit dependency injection instead of an implicit compatibility path.
