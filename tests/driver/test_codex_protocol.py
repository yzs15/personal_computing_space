import pytest

from loom_v2.coding_agents.codex import CodexAppServerProvider


def test_codex_provider_defaults_to_requested_model():
    assert CodexAppServerProvider().model == "deepseek-v4-flash"


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


async def _tool_result(name: str, args: dict) -> dict:
    return {"tool": name, **args}
