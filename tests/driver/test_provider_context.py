import asyncio

import pytest

from loom_v2.coding_agents.codex import CodexAppServerProvider
from loom_v2.coding_agents.turn import TurnContext


@pytest.mark.asyncio
async def test_codex_dynamic_tool_handler_is_context_scoped():
    provider = CodexAppServerProvider()
    provider.process = type("Process", (), {"stdin": object()})()
    calls = []

    async def send(message):
        calls.append(message)

    provider._send = send
    context_a = TurnContext("w", "a", "r-a", "c-a", 1, "owner-a", process=provider.process, mcp_handler=lambda *_: asyncio.sleep(0, result={"owner": "a"}))
    context_b = TurnContext("w", "b", "r-b", "c-b", 1, "owner-b", process=provider.process, mcp_handler=lambda *_: asyncio.sleep(0, result={"owner": "b"}))
    provider._active_context = context_a
    await provider._handle_dynamic_tool_call({"id": 1, "params": {"tool": "tool-a", "arguments": {}}})
    provider._active_context = context_b
    await provider._handle_dynamic_tool_call({"id": 2, "params": {"tool": "tool-b", "arguments": {}}})
    assert '"owner": "a"' in calls[0]["result"]["contentItems"][0]["text"]
    assert '"owner": "b"' in calls[1]["result"]["contentItems"][0]["text"]


@pytest.mark.asyncio
async def test_stale_codex_context_cannot_end_newer_turn():
    provider = CodexAppServerProvider()
    await provider._turn_lock.acquire()
    newer = TurnContext("w", "new", "new", "", 2, "new-owner", process=None)
    older = TurnContext("w", "old", "old", "", 1, "old-owner", process=None)
    provider._active_context = newer
    await provider.end_turn(older)
    assert provider._active_context is newer
    provider._turn_lock.release()
