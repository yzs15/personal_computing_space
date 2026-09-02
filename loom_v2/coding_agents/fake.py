from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any, Awaitable, Callable
from uuid import uuid4

from .base import AgentEvent
from .turn import TurnContext


class FakeCodingAgentProvider:
    def __init__(self) -> None:
        self.session_ref: str | None = None
        self._active_context: TurnContext | None = None
        self.contexts: list[TurnContext] = []

    async def begin_turn(
        self,
        conversation_ref: str,
        request_id: str,
        workspace_root: str,
        existing_thread_id: str | None = None,
        tools: list[dict[str, Any]] | None = None,
        handler: Callable[[str, dict[str, Any]], Awaitable[dict[str, Any]]] | None = None,
    ) -> TurnContext:
        context = TurnContext(
            workspace_id="workspace-default",
            conversation_ref=conversation_ref,
            request_id=request_id,
            claim_token="",
            driver_epoch=0,
            owner_generation=uuid4().hex,
            thread_id=existing_thread_id or f"fake-session:{conversation_ref}",
            dynamic_tools=list(tools or []),
            mcp_handler=handler,
        )
        self._active_context = context
        self.contexts.append(context)
        self.session_ref = context.thread_id
        return context

    async def start(self, conversation_ref: str, workspace_root: str, existing_thread_id: str | None = None) -> str:
        context = await self.begin_turn(conversation_ref, f"legacy-{conversation_ref}", workspace_root, existing_thread_id=existing_thread_id)
        return str(context.thread_id)

    async def send_turn(self, context_or_message: TurnContext | str, user_message: str | None = None) -> AsyncIterator[AgentEvent]:
        context = context_or_message if isinstance(context_or_message, TurnContext) else self._active_context
        if isinstance(context_or_message, TurnContext):
            if self._active_context is not context:
                raise RuntimeError("stale_turn_context")
            self._active_context = context
            self.session_ref = context.thread_id
            user_message = user_message or ""
        else:
            user_message = context_or_message
        yield AgentEvent(
            "open_run",
            {
                "closure_contract": {
                    "closure_id": f"closure-{self.session_ref or 'fake'}",
                    "goal": user_message,
                    "required_success_criteria": [],
                    "allowed_effects": ["read_workspace"],
                    "resource_budget": {"max_node_concurrency": 1, "max_attempts": 1},
                    "recovery_policy": {"allow_reassignment": False},
                    "result_expectations": [{"kind": "content", "identity_criterion": "content_digest"}],
                    "declared_constraints": [],
                    "body": {"closure_id": f"closure-{self.session_ref or 'fake'}", "metadata": {"goal": user_message}},
                }
            },
        )
        yield AgentEvent("assistant_text", {"text": "I will refine the closure in multiple patches."})
        yield AgentEvent("apply_plan_patch", {"ops": [{"kind": "set_program_ref", "value": "loom://echo"}]})
        yield AgentEvent("apply_plan_patch", {"ops": [{"kind": "set_compute_spec", "value": {"operation_ref": "loom://echo"}}]})
        yield AgentEvent("apply_plan_patch", {"ops": [{"kind": "add_typed_hole", "value": {"hole_id": "h_compute"}}]})
        yield AgentEvent(
            "apply_plan_patch",
            {
                "ops": [
                    {
                        "kind": "bind_compute_hole",
                        "value": {
                            "binding_id": "binding-h_compute",
                            "hole_id": "h_compute",
                            "capability_descriptor_ref": {"resource_id": "capability://slave-a/echo"},
                            "target_resource_ref": {"resource_id": "slave-a"},
                            "realization_digest": "fake-realization-echo",
                            "bound_by": "fake-driver",
                        },
                    }
                ]
            },
        )
        yield AgentEvent("inspect_plan_readiness", {})
        yield AgentEvent("commit_plan", {})
        yield AgentEvent("start_run", {})

    async def interrupt(self, context_or_turn: TurnContext | str | None = None) -> None:
        if isinstance(context_or_turn, TurnContext) and self._active_context is not context_or_turn:
            raise RuntimeError("stale_turn_context")
        return None

    async def end_turn(self, context: TurnContext) -> None:
        if self._active_context is context:
            self._active_context = None
            self.session_ref = None

    async def force_shutdown(self) -> None:
        self._active_context = None
        self.session_ref = None

    async def close(self) -> None:
        if self._active_context is not None:
            await self.end_turn(self._active_context)
        else:
            self.session_ref = None
