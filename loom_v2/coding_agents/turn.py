from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable


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

