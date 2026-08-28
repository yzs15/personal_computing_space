# Codex Protocol Health Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task with verification checkpoints.

**Goal:** Prevent healthy long-running Codex app-server turns from being misclassified as stalled while still reporting genuine protocol failures and enforcing the 24-hour absolute deadline.

**Architecture:** Separate turn lifecycle from protocol health. `send_turn` terminates only on explicit turn/system/goal errors, interruption, process/protocol failure, or the Driver-owned absolute deadline. A bounded poll scheduler emits `thread/read` and `thread/goal/get` only when the previous request of that method is complete. Every successful RPC response and every valid app-server notification refreshes a protocol heartbeat; unchanged snapshots are never treated as failure. A consecutive RPC failure window is the only provider-level stall signal.

**Tech Stack:** Python 3.11+, asyncio, pytest/pytest-asyncio, Pydantic settings, Codex app-server JSON-RPC over stdout JSONL.

---

### Task 1: Replace stale stall tests with protocol-health behavior tests

**Files:**
- Modify: `tests/driver/test_codex_protocol.py`

- [x] **Step 1: Replace the old snapshot-stall test.** Rename it to `test_codex_provider_does_not_stall_on_unchanged_healthy_snapshots`, remove `stall_seconds`, feed repeated successful `thread/read` and `thread/goal/get` responses with identical `updatedAt` and item ids, then complete the turn and assert no `agent_stalled` event.
- [x] **Step 2: Add a failing test for consecutive poll failures.** Configure `protocol_failure_seconds=0.05`, return JSON-RPC errors for both poll methods, advance the injected clock, and assert one `agent_stalled` event with code `coding_agent_stalled`, source `protocol_health`, and failure metadata.
- [x] **Step 3: Add a failing recovery test.** Return one failed poll followed by a successful poll and a completed turn; assert no stall event and that a successful response clears the failure window.
- [x] **Step 4: Add a failing bounded-poll test.** Keep poll responses unresolved, let several intervals elapse, and assert at most one in-flight request per method is present.
- [x] **Step 5: Add heartbeat coverage.** Feed `thread/tokenUsage/updated`, `item/agentMessage/delta`, and `thread/status/changed` notifications during a quiet turn; assert they are surfaced and do not produce a stall.
- [x] **Step 6: Run the focused tests and verify they fail for the expected missing/new behavior.**

### Task 2: Rewrite CodexAppServerProvider around protocol health

**Files:**
- Modify: `loom_v2/coding_agents/codex.py`

- [x] **Step 1: Remove the old idle-stall API and state.** Delete the `stall_seconds` constructor argument, `LOOM_CODING_AGENT_STALL_SECONDS` lookup, `last_progress_at`, `last_snapshot`, and all elapsed-time checks based on content/snapshot changes.
- [x] **Step 2: Add protocol-health state.** Read `LOOM_CODING_AGENT_PROTOCOL_FAILURE_SECONDS` (default 60), track `last_protocol_heartbeat_at`, `poll_failure_started_at`, and a per-method `_poll_requests` map. Keep all protocol reads in the foreground reader.
- [x] **Step 3: Implement bounded polling.** `_poll_loop` sends each poll method only if that method has no in-flight request; it catches send failures and records protocol failure without spawning unbounded requests. Cancel it in `finally`.
- [x] **Step 4: Centralize health transitions.** Successful poll responses and normal notifications call `_mark_protocol_heartbeat`; poll RPC errors call `_mark_protocol_failure`; emit `agent_stalled` and end the stream only when failure duration reaches the configured window.
- [x] **Step 5: Preserve lifecycle error handling.** Keep explicit handling for `turn/completed` failures/interruption, `thread/status/changed` system errors, goal blocked/usage/budget limits, `error`/`warning`, process EOF and protocol decode errors. These are not converted into idle-stall events.
- [x] **Step 6: Treat all valid app-server notifications as liveness.** Explicitly include deltas, `item/*`, `turn/*`, `thread/status/changed`, `thread/tokenUsage/updated`, and goal notifications; do not require active-item or snapshot changes.
- [x] **Step 7: Run focused protocol tests and then the full test suite; refactor only after green.**

### Task 3: Remove obsolete configuration and align runtime wiring

**Files:**
- Modify: `loom_v2/settings.py`
- Modify: `loom_v2/observer/app.py`
- Modify: `deploy/docker-compose.yml`
- Modify: `README.md`

- [x] **Step 1: Replace `coding_agent_stall_seconds` with `coding_agent_protocol_failure_seconds=60.0`.**
- [x] **Step 2: Pass the new setting to `CodexAppServerProvider`; remove all old environment wiring.**
- [x] **Step 3: Update deployment and README descriptions to state that quiet generation is allowed, protocol failures use a 60-second window, and the Driver keeps a 24-hour absolute deadline.**
- [x] **Step 4: Render both Compose files and run settings/import checks.**

### Task 4: Unify design and implementation plans

**Files:**
- Modify: `docs/superpowers/specs/2026-08-26-capability-gap-resolution-design.md`
- Modify: `docs/superpowers/plans/2026-08-27-capability-gap-resolution.md`

- [x] **Step 1: Delete the contradictory rule that unchanged items/snapshots for 300 seconds imply a stall.**
- [x] **Step 2: Document `TurnLifecycle`, `ProtocolHealthMonitor` responsibilities, bounded polling, heartbeat signals, protocol failure window, and 24-hour absolute deadline.**
- [x] **Step 3: Ensure start/dispatch remains an execution boundary whose completed result is preserved if a later turn error occurs.**

### Task 5: Restart and verify the deployed Observer

**Files:**
- Runtime only; no source files.

- [x] **Step 1: Restart the Observer on port 18080 with the new environment and confirm no `LOOM_CODING_AGENT_STALL_SECONDS` remains.**
- [x] **Step 2: Run `pytest -q`, `python -m compileall -q loom_v2`, both `docker compose ... config` commands, and health/runtime curl checks.**
- [x] **Step 3: Run a real Codex smoke turn and verify a quiet/long generation is not failed by an idle timeout; report any explicit protocol or turn error with its structured reason.**
