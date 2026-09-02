from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Awaitable, Callable, Protocol

from .turn import TurnContext


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
    async def begin_turn(
        self,
        conversation_ref: str,
        request_id: str,
        workspace_root: str,
        existing_thread_id: str | None,
        tools: list[dict[str, Any]],
        handler: Callable[[str, dict[str, Any]], Awaitable[dict[str, Any]]] | None,
    ) -> TurnContext: ...

    def send_turn(self, context: TurnContext, user_message: str) -> AsyncIterator[AgentEvent]: ...

    async def interrupt(self, context: TurnContext) -> None: ...

    async def end_turn(self, context: TurnContext) -> None: ...

    async def force_shutdown(self) -> None: ...
