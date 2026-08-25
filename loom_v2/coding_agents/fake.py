from __future__ import annotations

from collections.abc import AsyncIterator

from .base import AgentEvent


class FakeCodingAgentProvider:
    def __init__(self) -> None:
        self.session_ref: str | None = None

    async def start(self, conversation_ref: str, workspace_root: str) -> str:
        self.session_ref = f"fake-session:{conversation_ref}"
        return self.session_ref

    async def send_turn(self, user_message: str) -> AsyncIterator[AgentEvent]:
        yield AgentEvent("assistant_text", {"text": "I will refine the closure in multiple patches."})
        yield AgentEvent("apply_plan_patch", {"ops": [{"kind": "set_program_ref", "value": "loom://echo"}]})
        yield AgentEvent("apply_plan_patch", {"ops": [{"kind": "set_compute_spec", "value": {"operation_ref": "loom://echo"}}]})
        yield AgentEvent("apply_plan_patch", {"ops": [{"kind": "add_typed_hole", "value": {"hole_id": "h_compute"}}]})
        yield AgentEvent("inspect_plan_readiness", {})
        yield AgentEvent("commit_plan", {})

    async def interrupt(self, turn_ref: str | None = None) -> None:
        return None

    async def close(self) -> None:
        self.session_ref = None
