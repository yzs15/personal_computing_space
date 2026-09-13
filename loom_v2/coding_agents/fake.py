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
        self._tool_handler: Callable[[str, dict[str, Any]], Awaitable[dict[str, Any]]] | None = None
        self.contexts: list[TurnContext] = []

    def set_tool_handler(
        self,
        tools: list[dict[str, Any]],
        handler: Callable[[str, dict[str, Any]], Awaitable[dict[str, Any]]],
    ) -> None:
        # The local legacy ``start`` API does not pass a handler into
        # ``begin_turn``. Keep the same explicit tool seam as the remote
        # context API so the deterministic provider exercises real MCP/package
        # mutations instead of a hidden builtin executor.
        self._tool_handler = handler

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
            mcp_handler=handler if handler is not None else self._tool_handler,
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
        handler = context.mcp_handler if context is not None else self._tool_handler
        if handler is None:
            raise RuntimeError("fake_mcp_handler_required")

        program = await handler(
            "loom_put_content",
            {
                "media_type": "text/x-python",
                "content": (
                    "import json, sys\n"
                    "payload = json.load(sys.stdin)\n"
                    "value = payload.get('value', 0)\n"
                    "print(json.dumps({'value': value * 2}))\n"
                ),
            },
        )
        yield AgentEvent("tool_call", {"tool": "loom_put_content"})
        io_contract = await handler(
            "loom_put_content",
            {
                "media_type": "application/vnd.loom.io-contract+json",
                "content": {
                    "schema_version": "io.v1",
                    "input_schema_ref": None,
                    "output_schema_ref": None,
                    "success_semantics": None,
                    "success_validator_ref": None,
                },
            },
        )
        yield AgentEvent("tool_call", {"tool": "loom_put_content"})
        input_content = await handler(
            "loom_put_content",
            {"media_type": "application/json", "content": {"value": 3}},
        )
        yield AgentEvent("tool_call", {"tool": "loom_put_content"})
        program_ref = program["resource_ref"]
        io_contract_ref = io_contract["resource_ref"]
        input_ref = input_content["resource_ref"]
        operation_ref = "loom://test_double"
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

        async def apply_patch(operation_id: str, ops: list[dict[str, Any]]) -> AgentEvent:
            await handler(
                "loom_apply_plan_patch",
                {"operation_id": operation_id, "ops": ops},
            )
            return AgentEvent("tool_call", {"tool": "loom_apply_plan_patch"})

        yield await apply_patch(
            "fake-run-code-program",
            [
                {"kind": "set_program_ref", "value": operation_ref},
                {"kind": "set_io_contract_ref", "value": io_contract_ref},
                {"kind": "set_compute_spec", "value": {"operation_ref": operation_ref}},
                {"kind": "add_typed_hole", "value": {"hole_id": "h_compute"}},
            ],
        )
        yield await apply_patch(
            "fake-run-code-package",
            [
                {
                    "kind": "materialize_capability_package_candidate",
                    "value": {
                                 "package_id": "fake-run-code",
                                 "package_version": "v1",
                                 "package_type": "function",
                                 "execution": {"kind": "process:json_stdio", "version": "1"},
                                 "body": {
                                     "operation_descriptor_ref": operation_ref,
                                     "program_content_ref": program_ref,
                                     "io_contract_ref": io_contract_ref,
                                 },
                             },
                }
            ],
        )
        yield await apply_patch(
            "fake-run-code-binding",
            [
                {
                    "kind": "bind_compute_hole",
                    "value": {
                        "binding_id": "binding-h_compute",
                        "hole_id": "h_compute",
                        "capability_descriptor_ref": {"resource_id": "executor://process:json_stdio/1"},
                        "capability_package_ref": {"resource_id": "capability-package://fake-run-code/v1"},
                        "target_resource_ref": {"resource_id": "slave-a"},
                        "bound_by": "fake-driver",
                    },
                },
                {"kind": "set_execution_payload", "value": {"node_id": operation_ref, "input_ref": input_ref}},
            ],
        )
        await handler("loom_inspect_plan_readiness", {})
        yield AgentEvent("tool_call", {"tool": "loom_inspect_plan_readiness"})
        await handler("loom_commit_plan", {})
        yield AgentEvent("tool_call", {"tool": "loom_commit_plan"})
        await handler("loom_start_run", {})
        yield AgentEvent("tool_call", {"tool": "loom_start_run"})

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
