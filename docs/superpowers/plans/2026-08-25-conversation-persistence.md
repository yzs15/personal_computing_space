# Conversation Persistence and Agent Replies Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist conversation messages, surface real Codex agent replies, and restore a conversation list/history after browser refresh.

**Architecture:** Keep the current Observer PostgreSQL schema stable by storing allowlisted message events in the existing `runs.events` JSON column. Normalize Codex `item/completed` agent messages in the provider, have Driver record user/assistant events and return assistant text, expose conversation summaries/history from Observer, and let the browser hydrate its timeline from those APIs. Fake and Codex share the same normalized event path.

**Tech Stack:** Python 3.12+, FastAPI, SQLAlchemy async, PostgreSQL/SQLite tests, pytest/pytest-asyncio, vanilla HTML/CSS/JavaScript, SSE.

---

### Task 1: Normalize Codex agent messages

**Files:**
- Modify: `loom_v2/coding_agents/codex.py:43-59`
- Test: `tests/driver/test_codex_protocol.py`

- [ ] **Step 1: Write the failing protocol test**

Add a provider message fixture test that feeds an `item/completed` notification with `item.type == "agentMessage"` and asserts `send_turn` yields one `AgentEvent` with kind `assistant_text` and the item text. Keep the existing default-model assertion unchanged.

```python
@pytest.mark.asyncio
async def test_codex_agent_message_is_normalized(monkeypatch):
    provider = CodexAppServerProvider()
    provider.process = FakeProcess([
        {"id": 1, "result": {"thread": {"id": "thread-1"}}},
        {"id": 2, "result": {"turn": {"id": "turn-1"}}},
        {"method": "item/completed", "params": {"item": {"type": "agentMessage", "text": "答案"}}},
        {"method": "turn/completed", "params": {}},
    ])
    provider.thread_id = "thread-1"
    events = [event async for event in provider.send_turn("问题")]
    assert [(event.kind, event.payload) for event in events] == [("assistant_text", {"text": "答案"})]
```

Use the existing provider test helpers or a small async fake process matching `_send`/`_read_message`; do not start a live Codex process in this unit test.

- [ ] **Step 2: Run the focused test and verify the expected failure**

Run: `.venv/bin/pytest -q tests/driver/test_codex_protocol.py::test_codex_agent_message_is_normalized`

Expected: FAIL because the provider currently emits the raw `item/completed` method instead of `assistant_text`.

- [ ] **Step 3: Implement the smallest normalization**

In `CodexAppServerProvider.send_turn`, handle `item/completed` before the generic notification branch:

```python
if method == "item/completed":
    item = message.get("params", {}).get("item", {})
    if item.get("type") == "agentMessage" and item.get("text"):
        yield AgentEvent("assistant_text", {"text": item["text"]})
    continue
```

- [ ] **Step 4: Run protocol tests**

Run: `.venv/bin/pytest -q tests/driver/test_codex_protocol.py`

Expected: all Codex protocol tests pass.

- [ ] **Step 5: Commit**

```bash
git add loom_v2/coding_agents/codex.py tests/driver/test_codex_protocol.py
git commit -m "feat: normalize codex agent messages"
```

### Task 2: Persist and query conversation message events

**Files:**
- Modify: `loom_v2/observer/repository.py`
- Modify: `loom_v2/observer/app.py`
- Modify: `tests/db/test_role_metadata.py`
- Test: `tests/integration/test_conversations.py`
- Test: `tests/api/test_conversation.py`

- [ ] **Step 1: Write repository/API failing tests**

Add tests covering message persistence across a fresh repository instance and the two browser APIs:

```python
@pytest.mark.asyncio
async def test_message_events_survive_repository_reload():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    repo = ObserverRepository(engine)
    await repo.init_db()
    run = await repo.open_run("run-message", "conversation-1", "能力问题")
    await repo.append_message(run.run_id, "user", "能力问题")
    await repo.append_message(run.run_id, "assistant", "两者能力相同")
    restored = ObserverRepository(engine)
    await restored.init_db()
    conversation = await restored.get_conversation("conversation-1")
    assert [message["content"] for message in conversation["messages"]] == ["能力问题", "两者能力相同"]
```

Add an API test with the Fake backend that posts one message, asserts the response contains `conversation_ref` and `assistant_text`, then asserts `GET /api/v1/conversations` lists the reference and `GET /api/v1/conversations/{ref}` returns both roles. Add a stream assertion after constructing a fresh repository-backed app that persisted `message` events are present.

Update the role metadata test's expected Observer tables to include only the tables actually added; do not place Slave tables in `Base.metadata`.

- [ ] **Step 2: Run the new focused tests to verify failure**

Run: `.venv/bin/pytest -q tests/integration/test_conversations.py tests/api/test_conversation.py`

Expected: FAIL with missing repository/API methods or missing response fields.

- [ ] **Step 3: Add repository message and conversation projections**

Implement these methods in `ObserverRepository`:

```python
async def append_message(self, run_id: str, role: str, content: str) -> dict[str, Any]:
    record = await self._load(run_id)
    message = {
        "phase": "message",
        "message_id": f"message-{uuid4().hex[:12]}",
        "role": role,
        "content": content,
        "run_id": run_id,
    }
    record.events.append(message)
    await self._persist(record)
    return message

```

`list_conversations` must deduplicate by `task_ref`, include `conversation_ref`, a safe title from the latest goal, `run_count`, and `latest_run_id`. For a SQL-backed repository query `RunRow` rows and reconstruct records; for an in-memory repository use `self.runs.values()`. `get_conversation` must order rows by the repository's returned order, extract only allowlisted `phase == "message"` entries, and fall back to the run goal as a user message for older runs that predate message events. Include run summaries with `run_id`, `state`, `committed_version`, `execution_id`, and `outcome`.

The concrete method signatures are `async def list_conversations(self) -> list[dict[str, Any]]` and `async def get_conversation(self, conversation_ref: str) -> dict[str, Any]`.

- [ ] **Step 4: Expose the APIs and make SSE use persisted records**

Add to `create_app`:

```python
@app.get("/api/v1/conversations")
async def conversations() -> list[dict[str, Any]]:
    return await app.state.repo.list_conversations()

@app.get("/api/v1/conversations/{conversation_ref}")
async def conversation(conversation_ref: str) -> dict[str, Any]:
    try:
        return await app.state.repo.get_conversation(conversation_ref)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="conversation_not_found") from exc
```

Update the existing stream generator to await `get_conversation` and yield the persisted `events` for all runs in the conversation. Preserve the existing `text/event-stream` response and 404 behavior for unknown conversations.

- [ ] **Step 5: Run repository/API tests**

Run: `.venv/bin/pytest -q tests/integration/test_conversations.py tests/api/test_conversation.py tests/db/test_role_metadata.py`

Expected: all focused tests pass.

- [ ] **Step 6: Commit**

```bash
git add loom_v2/observer/repository.py loom_v2/observer/app.py tests/integration/test_conversations.py tests/api/test_conversation.py tests/db/test_role_metadata.py
git commit -m "feat: persist conversation message history"
```

### Task 3: Connect Driver replies to persisted messages

**Files:**
- Modify: `loom_v2/driver/service.py:30-50`
- Modify: `tests/driver/test_service.py`
- Modify: `tests/api/test_conversation.py`

- [ ] **Step 1: Write the failing Driver test**

Extend the Fake Driver test to assert the returned `assistant_text` and inspect the repository conversation messages for both user and assistant roles.

```python
assert result["conversation_ref"] == "conversation-1"
assert result["assistant_text"] == "I will refine the closure in multiple patches."
conversation = await repo.get_conversation("conversation-1")
assert [message["role"] for message in conversation["messages"]] == ["user", "assistant"]
```

- [ ] **Step 2: Run the test and verify failure**

Run: `.venv/bin/pytest -q tests/driver/test_service.py::test_driver_applies_fake_patches_continuously_and_starts_execution`

Expected: FAIL because Driver does not record messages or return assistant text.

- [ ] **Step 3: Implement Driver message handling**

Immediately after `open_run`, call `append_message(run.run_id, "user", prompt)`. During the provider event loop, handle normalized `assistant_text` events by appending each non-empty text and collecting it. Return `conversation_ref` and `assistant_text` (joined with two newlines) along with the existing run/execution fields. Keep provider close in `finally` and preserve the existing timeout/error behavior.

- [ ] **Step 4: Run Driver/API tests**

Run: `.venv/bin/pytest -q tests/driver/test_service.py tests/api/test_conversation.py`

Expected: all selected tests pass and the response contains the Fake assistant text.

- [ ] **Step 5: Commit**

```bash
git add loom_v2/driver/service.py tests/driver/test_service.py tests/api/test_conversation.py
git commit -m "feat: return and persist assistant replies"
```

### Task 4: Restore conversations in the browser

**Files:**
- Modify: `loom_v2/web/static/index.html`
- Modify: `loom_v2/web/static/app.js`
- Modify: `loom_v2/web/static/styles.css`
- Modify: `tests/web/test_static_ui.py`

- [ ] **Step 1: Write failing static UI tests**

Assert the homepage includes a conversation list and New conversation control. Assert `app.js` references both conversation endpoints, sends `conversation_ref`, renders `assistant_text`, and handles a non-OK response body.

```python
assert 'id="conversation-list"' in response.text
assert 'id="new-conversation"' in response.text
script = client.get("/static/app.js").text
assert "/api/v1/conversations" in script
assert "assistant_text" in script
assert "conversation_ref" in script
```

- [ ] **Step 2: Run static tests and verify failure**

Run: `.venv/bin/pytest -q tests/web/test_static_ui.py`

Expected: FAIL because the current static page has no conversation list and the script does not hydrate history.

- [ ] **Step 3: Add the UI controls and history loader**

Add a compact conversation list and New conversation button to the drawer. In `app.js`, maintain only the selected reference in memory, create a fresh opaque reference for New conversation, load `/api/v1/conversations` on startup, select the latest existing reference, and load `/api/v1/conversations/{ref}` into the timeline. Render message roles/content with `textContent` and never interpolate HTML.

- [ ] **Step 4: Render real replies and errors**

Post `{conversation_ref, text}` from the form. Parse the JSON response; on success render the persisted conversation (including `assistant_text`) and refresh the list. On failure render `Assistant: request failed (${payload.code || payload.detail || response.status})`. Keep runtime status loading and Slave status sections intact.

- [ ] **Step 5: Run static tests**

Run: `.venv/bin/pytest -q tests/web/test_static_ui.py`

Expected: all static UI tests pass.

- [ ] **Step 6: Commit**

```bash
git add loom_v2/web/static/index.html loom_v2/web/static/app.js loom_v2/web/static/styles.css tests/web/test_static_ui.py
git commit -m "feat: restore conversations in workspace UI"
```

### Task 5: Full verification and live rollout

**Files:**
- Modify: `README.md` to document the new conversation/history endpoints.

- [ ] **Step 1: Run the complete verification suite**

Run:

```bash
.venv/bin/pytest -q
git diff --check
bash -n scripts/dev-up.sh
docker compose -f deploy/docker-compose.yml config
docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.test.yml --profile test config
```

Expected: all tests pass, no diff whitespace errors, shell syntax is valid, and both Compose configurations validate.

- [ ] **Step 2: Restart the host Observer**

Stop the current host `uvicorn` process, start it with the existing environment (`LOOM_DATABASE_URL` pointing to `127.0.0.1:15432`, backend `codex`, model `deepseek-v4-flash`, workspace `.`), and confirm `/healthz` and `/api/v1/runtime` return 200.

- [ ] **Step 3: Verify persisted history and real Codex**

Send one real Chinese capability question to `/api/v1/messages`, assert the response contains non-empty `assistant_text`, then fetch `/api/v1/conversations` and the selected conversation endpoint and assert the assistant message is present. Refreshing `/` must still expose the conversation list/history APIs and the running Docker services must remain healthy.

- [ ] **Step 4: Commit any documentation update and report evidence**

```bash
git status --short
git log -5 --oneline
```

Report the live URL, backend/model, test count, and the real run ID.
