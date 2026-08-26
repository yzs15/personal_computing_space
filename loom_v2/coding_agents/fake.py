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

    async def interrupt(self, turn_ref: str | None = None) -> None:
        return None

    async def close(self) -> None:
        self.session_ref = None
