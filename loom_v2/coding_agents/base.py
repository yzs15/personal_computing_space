from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Protocol


@dataclass
class AgentEvent:
    kind: str
    payload: dict[str, Any] = field(default_factory=dict)


class CodingAgentProvider(Protocol):
    async def start(self, conversation_ref: str, workspace_root: str) -> str: ...

    def send_turn(self, user_message: str) -> AsyncIterator[AgentEvent]: ...

    async def interrupt(self, turn_ref: str) -> None: ...

    async def close(self) -> None: ...
