import asyncio
import json

import pytest

from loom_v2.coding_agents.codex import CodexAppServerProvider
from loom_v2.contracts.errors import DomainError, DomainErrorEnvelope


def test_codex_provider_defaults_to_requested_model():
    assert CodexAppServerProvider().model == "deepseek-v4-flash"


@pytest.mark.asyncio
async def test_codex_provider_resumes_existing_thread(monkeypatch):
    provider = CodexAppServerProvider()
    sent: list[dict] = []

    class Proc:
        stdin = None
        stdout = None

    async def spawn(*_args, **_kwargs):
        return Proc()

    async def send(message):
        sent.append(message)

    async def response(_request_id):
        return {"result": {"thread": {"id": "thread-7"}}}

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    provider._send = send
    provider._read_response = response
    thread_id = await provider.start("conversation-1", "/workspace", existing_thread_id="thread-7")
    assert thread_id == "thread-7"
    assert [item["method"] for item in sent] == ["initialize", "thread/resume"]
    assert sent[-1]["params"]["threadId"] == "thread-7"


@pytest.mark.asyncio
async def test_codex_provider_rejects_thread_start_error(monkeypatch):
    provider = CodexAppServerProvider()

    class Proc:
        stdin = None
        stdout = None

    async def spawn(*_args, **_kwargs):
        return Proc()

    responses = iter([
        {"result": {"ok": True}},
        {"error": {"code": -32000, "message": "provider unavailable"}},
    ])
    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    provider._send = lambda _message: _noop()
    provider._read_response = lambda _request_id: _next_response(responses)
    with pytest.raises(RuntimeError, match="coding_agent_unavailable"):
        await provider.start("conversation-1", "/workspace")


@pytest.mark.asyncio
async def test_codex_provider_reads_resumed_thread_history(monkeypatch):
    provider = CodexAppServerProvider()
    provider.process = object()
    provider.thread_id = "thread-7"
    sent = []

    async def send(message):
        sent.append(message)

    async def response(_request_id):
        return {"result": {"thread": {"id": "thread-7", "turns": []}}}

    provider._send = send
    provider._read_response = response
    result = await provider.read_thread()
    assert result["thread"]["id"] == "thread-7"
    assert sent[0]["method"] == "thread/read"


async def _noop():
    return None


async def _next_response(responses):
    return next(responses)


@pytest.mark.asyncio
async def test_codex_provider_reads_large_jsonl_message_in_chunks():
    """A single JSONL app-server event may exceed asyncio's 64 KiB default."""

    class ChunkedStdout:
        def __init__(self, payload: bytes):
            self.payload = payload
            self.read_calls = 0

        async def read(self, size: int = -1) -> bytes:
            self.read_calls += 1
            if not self.payload:
                return b""
            chunk, self.payload = self.payload[: max(size, 1)], self.payload[max(size, 1) :]
            return chunk

        async def readline(self):  # pragma: no cover - proves readline is not used
            raise AssertionError("the protocol reader must consume chunks, not readline")

    provider = CodexAppServerProvider()
    payload = {"method": "item/completed", "params": {"item": {"text": "x" * 100_000}}}
    stdout = ChunkedStdout((json.dumps(payload) + "\n").encode())
    provider.process = type("Process", (), {"stdout": stdout})()

    message = await provider._read_message()

    assert message == payload
    assert stdout.read_calls > 1


@pytest.mark.asyncio
async def test_provider_releases_turn_lock_when_start_fails():
    provider = CodexAppServerProvider(executable="loom-no-codex")
    with pytest.raises(RuntimeError, match="coding_agent_unavailable"):
        await provider.start("conversation", "/workspace")
    assert not provider._turn_lock.locked()


@pytest.mark.asyncio
async def test_provider_write_lock_serializes_stdin_writes():
    class FakeStdin:
        def __init__(self):
            self.active = 0
            self.max_active = 0

        def write(self, data: bytes) -> None:
            self.active += 1
            self.max_active = max(self.max_active, self.active)

        async def drain(self) -> None:
            await asyncio.sleep(0.005)
            self.active -= 1

    provider = CodexAppServerProvider()
    provider.process = type("Process", (), {"stdin": FakeStdin()})()

    async def send_one(index: int) -> None:
        await provider._send({"jsonrpc": "2.0", "id": index, "method": "thread/read"})

    await asyncio.gather(*(send_one(index) for index in range(6)))
    assert provider.process.stdin.max_active == 1


@pytest.mark.asyncio
async def test_provider_turn_lock_serializes_concurrent_starts(monkeypatch):
    provider = CodexAppServerProvider()

    class Proc:
        stdin = None
        stdout = None

        def terminate(self) -> None:
            return None

        async def wait(self) -> int:
            return 0

    async def spawn(*_args, **_kwargs):
        return Proc()

    async def send(_message: dict) -> None:
        return None

    async def response(_request_id: int) -> dict:
        return {"result": {"thread": {"id": "thread-lock"}}}

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    provider._send = send
    provider._read_response = response

    first = asyncio.create_task(provider.start("conversation-a", "/workspace"))
    await asyncio.sleep(0.01)
    second = asyncio.create_task(provider.start("conversation-b", "/workspace"))
    await asyncio.sleep(0.05)
    assert not second.done()

    await provider.close()
    assert await asyncio.wait_for(second, timeout=1) == "thread-lock"
    # The second turn now owns the turn lock until its own close().
    assert provider._turn_lock.locked()
    await provider.close()
    assert not provider._turn_lock.locked()


@pytest.mark.asyncio
async def test_codex_agent_message_is_normalized():
    provider = CodexAppServerProvider()
    provider.process = object()
    messages = iter(
        [
            {"method": "item/completed", "params": {"item": {"type": "agentMessage", "text": "答案"}}},
            {"method": "turn/completed", "params": {}},
        ]
    )

    async def fake_send(_message):
        return None

    async def fake_read_message():
        return next(messages)

    provider._send = fake_send
    provider._read_message = fake_read_message
    provider.thread_id = "thread-1"

    events = [event async for event in provider.send_turn("问题")]

    assert [(event.kind, event.payload) for event in events] == [("assistant_text", {"text": "答案"})]


@pytest.mark.asyncio
async def test_codex_turn_started_and_interrupted_are_normalized():
    provider = CodexAppServerProvider()
    provider.process = object()
    messages = iter(
        [
            {"method": "turn/started", "params": {"turn": {"id": "turn-42"}}},
            {"method": "turn/completed", "params": {"turn": {"id": "turn-42", "status": "interrupted"}}},
        ]
    )

    async def fake_send(_message):
        return None

    async def fake_read_message():
        return next(messages)

    provider._send = fake_send
    provider._read_message = fake_read_message
    provider.thread_id = "thread-1"

    events = [event async for event in provider.send_turn("问题")]

    assert [(event.kind, event.payload) for event in events] == [
        ("turn_started", {"turn_id": "turn-42"}),
        ("turn_interrupted", {"turn_id": "turn-42"}),
    ]
    assert provider.current_turn_id == "turn-42"


@pytest.mark.asyncio
async def test_codex_interrupt_includes_thread_and_turn_ids():
    provider = CodexAppServerProvider()
    provider.process = object()
    provider.thread_id = "thread-1"
    provider.current_turn_id = "turn-42"
    sent: list[dict] = []

    async def fake_send(message):
        sent.append(message)

    provider._send = fake_send

    await provider.interrupt()

    assert sent == [
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "turn/interrupt",
            "params": {"threadId": "thread-1", "turnId": "turn-42"},
        }
    ]


@pytest.mark.asyncio
async def test_codex_provider_registers_and_answers_dynamic_tools():
    provider = CodexAppServerProvider()
    provider.process = object()
    provider.thread_id = "thread-1"
    provider.set_tool_handler(
        [{"type": "function", "name": "loom_open_run", "description": "open", "inputSchema": {"type": "object"}}],
        lambda name, args: _tool_result(name, args),
    )
    sent: list[dict] = []
    messages = iter(
        [
            {"id": 7, "method": "item/tool/call", "params": {"tool": "loom_open_run", "arguments": {"goal": "sort"}}},
            {"method": "turn/completed", "params": {}},
        ]
    )

    async def fake_send(message):
        sent.append(message)

    async def fake_read_message():
        return next(messages)

    provider._send = fake_send
    provider._read_message = fake_read_message

    events = [event async for event in provider.send_turn("sort")]

    assert events[0].kind == "tool_call"
    assert sent[-1] == {
        "jsonrpc": "2.0",
        "id": 7,
        "result": {"contentItems": [{"type": "inputText", "text": '{"tool": "loom_open_run", "goal": "sort"}'}], "success": True},
    }


@pytest.mark.asyncio
async def test_codex_provider_returns_structured_dynamic_tool_error():
    provider = CodexAppServerProvider()
    provider.process = object()
    provider.thread_id = "thread-1"

    async def handler(_name, _args):
        raise DomainError(
            DomainErrorEnvelope(
                code="readiness_blocked",
                details={"blockers": [{"code": "orchestration_program_unresolved_name", "diagnostics": []}]},
            )
        )

    provider.set_tool_handler([], handler)
    sent: list[dict] = []

    async def send(message):
        sent.append(message)

    provider._send = send
    await provider._handle_dynamic_tool_call({"id": 8, "params": {"tool": "loom_commit_plan", "arguments": {}}})
    assert sent[0]["result"]["success"] is False
    assert sent[0]["result"]["error"]["code"] == "readiness_blocked"
    content = json.loads(sent[0]["result"]["contentItems"][0]["text"])
    assert content["error"]["blockers"][0]["code"] == "orchestration_program_unresolved_name"


@pytest.mark.asyncio
async def test_codex_provider_routes_dynamic_tool_when_id_collides_with_turn_request():
    """Dynamic tool request ids share the app-server numeric id space.

    Codex starts dynamic-call ids at zero, while Loom's ``turn/start``
    request is also a small integer.  A later tool call can therefore have
    the same id as the turn request; routing must use the JSON-RPC method,
    not only the id.
    """
    provider = CodexAppServerProvider(poll_interval_seconds=10)
    provider.process = object()
    provider.thread_id = "thread-1"
    provider.request_id = 2  # the next turn/start request gets id=3
    provider.set_tool_handler(
        [{"type": "function", "name": "loom_apply_plan_patch", "inputSchema": {"type": "object"}}],
        lambda name, args: _tool_result(name, args),
    )
    sent: list[dict] = []
    messages = iter(
        [
            # This is a dynamic request, not the response to turn/start,
            # despite sharing its numeric id.
            {"id": 3, "method": "item/tool/call", "params": {"tool": "loom_apply_plan_patch", "arguments": {"ops": []}}},
            {"method": "turn/completed", "params": {}},
        ]
    )

    async def fake_send(message):
        sent.append(message)

    async def fake_read_message():
        return next(messages)

    provider._send = fake_send
    provider._read_message = fake_read_message

    events = [event async for event in provider.send_turn("patch")]

    assert events[0].kind == "tool_call"
    assert sent[-1]["id"] == 3
    assert sent[-1]["result"]["success"] is True


@pytest.mark.asyncio
async def test_codex_provider_does_not_stall_on_unchanged_healthy_snapshots():
    provider = CodexAppServerProvider(poll_interval_seconds=0.001, protocol_failure_seconds=0.05)
    provider.process = object()
    provider.thread_id = "thread-quiet"
    clock = [0.0]
    provider.clock = lambda: clock[0]
    queue: asyncio.Queue[dict] = asyncio.Queue()
    sent: list[dict] = []
    polls = 0

    async def fake_send(message):
        nonlocal polls
        sent.append(message)
        method = message.get("method")
        if method == "turn/start":
            await queue.put({"method": "turn/started", "params": {"turn": {"id": "turn-quiet", "status": "inProgress"}}})
        elif method == "thread/read":
            polls += 1
            clock[0] += 0.02
            await queue.put(
                {
                    "id": message["id"],
                    "result": {
                        "thread": {
                            "id": "thread-quiet",
                            "status": {"type": "active", "activeFlags": []},
                            "updatedAt": 1,
                            "turns": [{"id": "turn-quiet", "status": "inProgress", "items": []}],
                        }
                    },
                }
            )
            if polls >= 4:
                await queue.put({"method": "turn/completed", "params": {"turn": {"id": "turn-quiet", "status": "completed"}}})
        elif method == "thread/goal/get":
            await queue.put(
                {
                    "id": message["id"],
                    "result": {"goal": {"status": "active", "updatedAt": 1, "tokensUsed": 1, "timeUsedSeconds": 1}},
                }
            )

    async def fake_read_message():
        return await queue.get()

    provider._send = fake_send
    provider._read_message = fake_read_message

    events = [event async for event in provider.send_turn("quiet generation")]

    assert any(message["method"] == "thread/read" and message["params"]["includeTurns"] for message in sent)
    assert any(message["method"] == "thread/goal/get" for message in sent)
    assert not any(event.kind == "agent_stalled" for event in events)
    assert polls >= 4


@pytest.mark.asyncio
async def test_codex_provider_reports_stall_after_consecutive_poll_failures():
    provider = CodexAppServerProvider(poll_interval_seconds=0.001, protocol_failure_seconds=0.05)
    provider.process = object()
    provider.thread_id = "thread-unhealthy"
    clock = [0.0]
    provider.clock = lambda: clock[0]
    queue: asyncio.Queue[dict] = asyncio.Queue()

    async def fake_send(message):
        method = message.get("method")
        if method == "turn/start":
            await queue.put({"method": "turn/started", "params": {"turn": {"id": "turn-unhealthy", "status": "inProgress"}}})
        elif method in {"thread/read", "thread/goal/get"}:
            clock[0] += 0.03
            await queue.put({"id": message["id"], "error": {"code": -32000, "message": "app-server unavailable"}})

    async def fake_read_message():
        return await queue.get()

    provider._send = fake_send
    provider._read_message = fake_read_message

    events = [event async for event in provider.send_turn("unhealthy")]

    stalled = next(event for event in events if event.kind == "agent_stalled")
    assert stalled.payload["code"] == "coding_agent_stalled"
    assert stalled.payload["source"] == "protocol_health"
    assert stalled.payload["failure_age_seconds"] >= 0.05


@pytest.mark.asyncio
async def test_codex_provider_clears_protocol_failure_after_healthy_poll():
    provider = CodexAppServerProvider(poll_interval_seconds=0.001, protocol_failure_seconds=0.05)
    provider.process = object()
    provider.thread_id = "thread-recovered"
    clock = [0.0]
    provider.clock = lambda: clock[0]
    queue: asyncio.Queue[dict] = asyncio.Queue()
    read_calls = 0

    async def fake_send(message):
        nonlocal read_calls
        method = message.get("method")
        if method == "turn/start":
            await queue.put({"method": "turn/started", "params": {"turn": {"id": "turn-recovered", "status": "inProgress"}}})
        elif method == "thread/read":
            read_calls += 1
            if read_calls == 1:
                clock[0] += 0.03
                await queue.put({"id": message["id"], "error": {"code": -32000, "message": "temporary"}})
            else:
                clock[0] += 0.01
                await queue.put(
                    {
                        "id": message["id"],
                        "result": {
                            "thread": {
                                "id": "thread-recovered",
                                "status": {"type": "active", "activeFlags": []},
                                "updatedAt": 1,
                                "turns": [{"id": "turn-recovered", "status": "inProgress", "items": []}],
                            }
                        },
                    }
                )
                if read_calls >= 2:
                    await queue.put({"method": "turn/completed", "params": {"turn": {"id": "turn-recovered", "status": "completed"}}})
        elif method == "thread/goal/get":
            await queue.put({"id": message["id"], "result": {"goal": {"status": "active"}}})

    async def fake_read_message():
        return await queue.get()

    provider._send = fake_send
    provider._read_message = fake_read_message

    events = [event async for event in provider.send_turn("recover")]

    assert read_calls >= 2
    assert not any(event.kind == "agent_stalled" for event in events)


@pytest.mark.asyncio
async def test_codex_provider_bounds_in_flight_poll_requests():
    provider = CodexAppServerProvider(poll_interval_seconds=0.001, protocol_failure_seconds=1)
    provider.process = object()
    provider.thread_id = "thread-bounded"
    sent: list[dict] = []

    async def fake_send(message):
        sent.append(message)

    provider._send = fake_send
    poll_task = asyncio.create_task(provider._poll_loop())
    try:
        await asyncio.sleep(0.02)
    finally:
        poll_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await poll_task

    methods = [message["method"] for message in sent]
    assert methods.count("thread/read") == 1
    assert methods.count("thread/goal/get") == 1
    assert len(provider._poll_requests) == 2


@pytest.mark.asyncio
async def test_codex_provider_reports_stall_when_poll_response_is_missing():
    provider = CodexAppServerProvider(poll_interval_seconds=0.001, protocol_failure_seconds=0.02)
    provider.process = object()
    provider.thread_id = "thread-no-response"
    queue: asyncio.Queue[dict] = asyncio.Queue()
    sent: list[dict] = []

    async def fake_send(message):
        sent.append(message)
        if message.get("method") == "turn/start":
            await queue.put({"method": "turn/started", "params": {"turn": {"id": "turn-no-response", "status": "inProgress"}}})

    async def fake_read_message():
        return await queue.get()

    provider._send = fake_send
    provider._read_message = fake_read_message

    events = [event async for event in provider.send_turn("no response")]

    assert any(message["method"] == "thread/read" for message in sent)
    assert any(event.kind == "agent_stalled" and event.payload["source"] == "protocol_health" for event in events)


@pytest.mark.asyncio
async def test_codex_provider_treats_notifications_as_protocol_heartbeat():
    provider = CodexAppServerProvider(poll_interval_seconds=10, protocol_failure_seconds=0.01)
    provider.process = object()
    provider.thread_id = "thread-heartbeat"
    messages = iter(
        [
            {"method": "turn/started", "params": {"turn": {"id": "turn-heartbeat", "status": "inProgress"}}},
            {"method": "thread/tokenUsage/updated", "params": {"threadId": "thread-heartbeat", "tokensUsed": 4}},
            {"method": "item/agentMessage/delta", "params": {"delta": "still thinking"}},
            {"method": "thread/status/changed", "params": {"threadId": "thread-heartbeat", "status": {"type": "active"}}},
            {"method": "turn/completed", "params": {"turn": {"id": "turn-heartbeat", "status": "completed"}}},
        ]
    )

    async def fake_send(_message):
        return None

    async def fake_read_message():
        return next(messages)

    provider._send = fake_send
    provider._read_message = fake_read_message

    events = [event async for event in provider.send_turn("heartbeat")]

    assert not any(event.kind == "agent_stalled" for event in events)
    assert any(event.kind == "thread/tokenUsage/updated" for event in events)
    assert any(event.kind == "item/agentMessage/delta" for event in events)


@pytest.mark.asyncio
async def test_codex_provider_does_not_stall_while_waiting_for_user_input():
    provider = CodexAppServerProvider(poll_interval_seconds=0.001, protocol_failure_seconds=0.05)
    provider.process = object()
    provider.thread_id = "thread-question"
    clock = [0.0]
    provider.clock = lambda: clock[0]
    queue: asyncio.Queue[dict] = asyncio.Queue()
    sent: list[dict] = []
    polls = 0

    async def fake_send(message):
        nonlocal polls
        sent.append(message)
        method = message.get("method")
        if method == "turn/start":
            await queue.put({"method": "turn/started", "params": {"turn": {"id": "turn-question", "status": "inProgress"}}})
        elif method == "thread/read":
            polls += 1
            clock[0] += 0.02
            await queue.put(
                {
                    "id": message["id"],
                    "result": {
                        "thread": {
                            "id": "thread-question",
                            "status": {"type": "active", "activeFlags": ["waitingOnUserInput"]},
                            "updatedAt": 1,
                            "turns": [{"id": "turn-question", "status": "inProgress", "items": []}],
                        }
                    },
                }
            )
            if polls >= 4:
                await queue.put({"method": "turn/completed", "params": {"turn": {"id": "turn-question", "status": "completed"}}})
        elif method == "thread/goal/get":
            await queue.put({"id": message["id"], "result": {"goal": {"status": "active"}}})

    async def fake_read_message():
        return await queue.get()

    provider._send = fake_send
    provider._read_message = fake_read_message

    events = [event async for event in provider.send_turn("need clarification")]

    assert not any(event.kind == "agent_stalled" for event in events)
    assert any(event.kind == "thread_status" and "waitingOnUserInput" in event.payload["active_flags"] for event in events)


@pytest.mark.asyncio
async def test_codex_provider_treats_changing_thread_snapshot_as_progress():
    provider = CodexAppServerProvider(poll_interval_seconds=0.001, protocol_failure_seconds=0.05)
    provider.process = object()
    provider.thread_id = "thread-progress"
    clock = [0.0]
    provider.clock = lambda: clock[0]
    queue: asyncio.Queue[dict] = asyncio.Queue()
    snapshots = 0

    async def fake_send(message):
        nonlocal snapshots
        method = message.get("method")
        if method == "turn/start":
            await queue.put({"method": "turn/started", "params": {"turn": {"id": "turn-progress", "status": "inProgress"}}})
        elif method == "thread/read":
            snapshots += 1
            clock[0] += 0.02
            await queue.put(
                {
                    "id": message["id"],
                    "result": {
                        "thread": {
                            "id": "thread-progress",
                            "status": {"type": "active", "activeFlags": []},
                            "updatedAt": snapshots,
                            "turns": [{"id": "turn-progress", "status": "inProgress", "items": [{"id": f"item-{snapshots}"}]}],
                        }
                    },
                }
            )
            if snapshots >= 4:
                await queue.put({"method": "turn/completed", "params": {"turn": {"id": "turn-progress", "status": "completed"}}})
        elif method == "thread/goal/get":
            await queue.put({"id": message["id"], "result": {"goal": {"status": "active"}}})

    async def fake_read_message():
        return await queue.get()

    provider._send = fake_send
    provider._read_message = fake_read_message

    events = [event async for event in provider.send_turn("progress")]

    assert snapshots >= 4
    assert not any(event.kind == "agent_stalled" for event in events)


@pytest.mark.asyncio
async def test_codex_provider_normalizes_turn_error_and_codex_error_info():
    provider = CodexAppServerProvider(poll_interval_seconds=10)
    provider.process = object()
    provider.thread_id = "thread-error"
    messages = iter(
        [
            {"method": "turn/started", "params": {"turn": {"id": "turn-error", "status": "inProgress"}}},
            {
                "method": "turn/completed",
                "params": {
                    "turn": {
                        "id": "turn-error",
                        "status": "failed",
                        "error": {"message": "quota", "codexErrorInfo": "usageLimitExceeded"},
                    }
                },
            },
        ]
    )

    async def fake_send(_message):
        return None

    async def fake_read_message():
        return next(messages)

    provider._send = fake_send
    provider._read_message = fake_read_message

    events = [event async for event in provider.send_turn("error")]

    error = next(event for event in events if event.kind == "agent_error")
    assert error.payload["source"] == "turn/completed"
    assert error.payload["message"] == "quota"
    assert error.payload["codex_error_info"] == "usageLimitExceeded"


@pytest.mark.asyncio
async def test_codex_provider_reports_system_error_notification():
    provider = CodexAppServerProvider(poll_interval_seconds=10)
    provider.process = object()
    provider.thread_id = "thread-system-error"
    messages = iter(
        [
            {"method": "turn/started", "params": {"turn": {"id": "turn-system-error", "status": "inProgress"}}},
            {"method": "thread/status/changed", "params": {"threadId": "thread-system-error", "status": {"type": "systemError"}}},
        ]
    )

    async def fake_send(_message):
        return None

    async def fake_read_message():
        return next(messages)

    provider._send = fake_send
    provider._read_message = fake_read_message

    events = [event async for event in provider.send_turn("system error")]

    error = next(event for event in events if event.kind == "agent_error")
    assert error.payload["code"] == "thread_system_error"
    assert error.payload["source"] == "thread/status/changed"


@pytest.mark.asyncio
async def test_codex_provider_reports_blocked_goal_from_active_poll():
    provider = CodexAppServerProvider(poll_interval_seconds=0.001, protocol_failure_seconds=10)
    provider.process = object()
    provider.thread_id = "thread-blocked"
    queue: asyncio.Queue[dict] = asyncio.Queue()

    async def fake_send(message):
        method = message.get("method")
        if method == "turn/start":
            await queue.put({"method": "turn/started", "params": {"turn": {"id": "turn-blocked", "status": "inProgress"}}})
        elif method == "thread/read":
            await queue.put(
                {
                    "id": message["id"],
                    "result": {
                        "thread": {
                            "id": "thread-blocked",
                            "status": {"type": "active", "activeFlags": []},
                            "updatedAt": 1,
                            "turns": [{"id": "turn-blocked", "status": "inProgress", "items": []}],
                        }
                    },
                }
            )
        elif method == "thread/goal/get":
            await queue.put({"id": message["id"], "result": {"goal": {"status": "blocked"}}})

    async def fake_read_message():
        return await queue.get()

    provider._send = fake_send
    provider._read_message = fake_read_message

    events = [event async for event in provider.send_turn("blocked")]

    error = next(event for event in events if event.kind == "agent_error")
    assert error.payload["code"] == "coding_agent_blocked"
    assert error.payload["goal_status"] == "blocked"


@pytest.mark.asyncio
async def test_codex_provider_consumes_goal_status_notification():
    provider = CodexAppServerProvider(poll_interval_seconds=10)
    provider.process = object()
    provider.thread_id = "thread-goal-notification"
    messages = iter(
        [
            {"method": "turn/started", "params": {"turn": {"id": "turn-goal-notification", "status": "inProgress"}}},
            {
                "method": "thread/goal/updated",
                "params": {"goal": {"status": "usageLimited", "objective": "refine", "threadId": "thread-goal-notification"}},
            },
        ]
    )

    async def fake_send(_message):
        return None

    async def fake_read_message():
        return next(messages)

    provider._send = fake_send
    provider._read_message = fake_read_message

    events = [event async for event in provider.send_turn("goal")]

    error = next(event for event in events if event.kind == "agent_error")
    assert error.payload["code"] == "coding_agent_usage_limited"
    assert error.payload["source"] == "thread/goal/updated"


@pytest.mark.asyncio
async def test_codex_provider_keeps_retryable_error_as_warning():
    provider = CodexAppServerProvider(poll_interval_seconds=10)
    provider.process = object()
    provider.thread_id = "thread-retry"
    messages = iter(
        [
            {"method": "turn/started", "params": {"turn": {"id": "turn-retry", "status": "inProgress"}}},
            {
                "method": "error",
                "params": {
                    "threadId": "thread-retry",
                    "turnId": "turn-retry",
                    "willRetry": True,
                    "error": {"message": "temporary", "codexErrorInfo": "serverOverloaded"},
                },
            },
            {"method": "turn/completed", "params": {"turn": {"id": "turn-retry", "status": "completed"}}},
        ]
    )

    async def fake_send(_message):
        return None

    async def fake_read_message():
        return next(messages)

    provider._send = fake_send
    provider._read_message = fake_read_message

    events = [event async for event in provider.send_turn("retry")]

    assert not any(event.kind == "agent_error" for event in events)
    warning = next(event for event in events if event.kind == "agent_warning")
    assert warning.payload["will_retry"] is True


@pytest.mark.asyncio
async def test_codex_provider_reports_failed_turn_from_thread_read():
    provider = CodexAppServerProvider(poll_interval_seconds=0.001, protocol_failure_seconds=10)
    provider.process = object()
    provider.thread_id = "thread-read-failed"
    queue: asyncio.Queue[dict] = asyncio.Queue()

    async def fake_send(message):
        method = message.get("method")
        if method == "turn/start":
            await queue.put({"method": "turn/started", "params": {"turn": {"id": "turn-read-failed", "status": "inProgress"}}})
        elif method == "thread/read":
            await queue.put(
                {
                    "id": message["id"],
                    "result": {
                        "thread": {
                            "id": "thread-read-failed",
                            "status": {"type": "active", "activeFlags": []},
                            "updatedAt": 1,
                            "turns": [{"id": "turn-read-failed", "status": "failed", "items": [], "error": None}],
                        }
                    },
                }
            )
        elif method == "thread/goal/get":
            await queue.put({"id": message["id"], "result": {"goal": {"status": "active"}}})

    async def fake_read_message():
        return await queue.get()

    provider._send = fake_send
    provider._read_message = fake_read_message

    events = [event async for event in provider.send_turn("failed")]

    error = next(event for event in events if event.kind == "agent_error")
    assert error.payload["code"] == "coding_agent_turn_failed"
    assert error.payload["source"] == "thread/read"


async def _tool_result(name: str, args: dict) -> dict:
    return {"tool": name, **args}
