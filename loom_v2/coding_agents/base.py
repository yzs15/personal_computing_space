from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Protocol


@dataclass
class AgentEvent:
    kind: str
    payload: dict[str, Any] = field(default_factory=dict)


class CodingAgentError(RuntimeError):
    """Structured failure reported by a coding-agent app-server."""

    def __init__(self, reason: dict[str, Any]) -> None:
        self.reason = dict(reason)
        message = str(self.reason.get("code") or self.reason.get("message") or "coding_agent_error")
        super().__init__(message)


class CodingAgentProvider(Protocol):
    async def start(self, conversation_ref: str, workspace_root: str) -> str: ...

    def send_turn(self, user_message: str) -> AsyncIterator[AgentEvent]: ...

    async def interrupt(self, turn_ref: str | None = None) -> None: ...

    async def close(self) -> None: ...
