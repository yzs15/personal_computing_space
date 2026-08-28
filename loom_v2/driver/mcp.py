from __future__ import annotations

from typing import Any
from uuid import uuid4

from loom_v2.contracts.types import ClosureContract, TaskClosure
from loom_v2.observer.repository import ObserverRepository

from .tools import DriverTools


class DriverMCP:
    """Scoped Driver tool surface exposed to exactly one coding-agent turn.

    The coding agent decides when to call ``open_run``.  This facade binds the
    conversation, user and workspace at construction time, so those identity
    fields are never supplied by model-generated tool arguments.
    """

    def __init__(
        self,
        repository: ObserverRepository,
        conversation_ref: str,
        *,
        user_id: str = "user-default",
        workspace_id: str = "workspace-default",
    ) -> None:
        self.repository = repository
        self.conversation_ref = conversation_ref
        self.user_id = user_id
        self.workspace_id = workspace_id
        self.active_run_id: str | None = None
        self.tools = DriverTools(repository)

    @property
    def run_id(self) -> str | None:
        return self.active_run_id

    async def call(self, tool_name: str, arguments: dict[str, Any] | None = None) -> dict[str, Any]:
        arguments = arguments or {}
        if tool_name == "loom_open_run":
            return await self.open_run(arguments)
        if tool_name == "loom_query_capabilities":
            return self._query_capabilities()
        if tool_name == "loom_list_run_capability_packages":
            if self.active_run_id is None:
                return {"packages": []}
            packages = await self.repository.list_capability_packages(run_id=self.active_run_id, include_abandoned=True)
            return {"packages": [package.model_dump(mode="json") for package in packages]}
        if self.active_run_id is None:
            raise ValueError("run_not_open")
        if tool_name == "loom_apply_plan_patch":
            operation_id = str(arguments.get("operation_id") or f"mcp-patch-{uuid4().hex[:12]}")
            return await self.tools.apply_plan_patch(self.active_run_id, operation_id, arguments.get("ops", []))
        if tool_name == "loom_inspect_plan_readiness":
            return await self.tools.inspect_plan_readiness(self.active_run_id)
        if tool_name == "loom_commit_plan":
            return await self.tools.commit_plan(self.active_run_id)
        if tool_name == "loom_start_run":
            run = await self.repository.get_run(self.active_run_id)
            version_id = arguments.get("closure_version") or (run.committed.version_id if run.committed else None)
            if not version_id:
                raise ValueError("closure_not_committed")
            return await self.tools.start_run(self.active_run_id, version_id)
        if tool_name == "loom_get_run_status":
            return self.run_view(await self.repository.get_run(self.active_run_id))
        if tool_name == "loom_close_run":
            return self.run_view(await self.repository.close_run(self.active_run_id))
        raise ValueError(f"unknown_driver_tool:{tool_name}")

    def _query_capabilities(self) -> dict[str, Any]:
        return {
            "workspace_id": self.workspace_id,
            "capabilities": [
                {
                    "resource_id": resource_id,
                    "available": self.repository.slave_availability.get(resource_id, False),
                    "operations": sorted(details.get("operations", set())),
                }
                for resource_id, details in sorted(self.repository.slave_capabilities.items())
            ],
            "executor_descriptors": [
                {"kind": "builtin_v1", "version": "1", "operations": ["echo", "hash", "sort", "run_code"]},
                {"kind": "subprocess_json_v1", "version": "1", "operations": ["run_code"]},
            ],
        }

    async def open_run(self, arguments: dict[str, Any]) -> dict[str, Any]:
        contract_payload = arguments.get("closure_contract")
        if contract_payload is None:
            contract_payload = arguments
        contract_payload = dict(contract_payload)
        closure_id = str(contract_payload.get("closure_id") or f"closure-{self.conversation_ref}")
        body = contract_payload.get("body")
        if isinstance(body, str):
            contract_payload["body"] = TaskClosure.minimal(
                closure_id=closure_id,
                metadata={"description": body},
            ).model_dump(mode="json")
        elif isinstance(body, dict) and "description" in body and not any(
            key in body for key in {"data", "program", "compute", "constraints", "metadata"}
        ):
            contract_payload["body"] = TaskClosure.minimal(
                closure_id=str(body.get("closure_id") or closure_id),
                metadata={"description": body["description"]},
            ).model_dump(mode="json")
        if "body" not in contract_payload:
            contract_payload["body"] = TaskClosure.minimal(
                closure_id=closure_id,
                metadata={"goal": contract_payload.get("goal", "")},
            ).model_dump(mode="json")
        contract = ClosureContract.model_validate(contract_payload)
        if not contract.goal:
            raise ValueError("goal_required")
        if self.active_run_id is not None:
            existing = await self.repository.get_run(self.active_run_id)
            if existing.closure_contract and existing.closure_contract.model_dump(mode="json") == contract.model_dump(mode="json"):
                return {"run_id": existing.run_id, "state": existing.state, "idempotent": True, "closure_contract": contract.model_dump(mode="json")}
            raise ValueError("conversation_run_already_open")
        allow_reassignment = bool(contract.recovery_policy.get("allow_reassignment", False))
        record = await self.repository.open_run(
            arguments.get("run_id"),
            self.conversation_ref,
            contract.goal,
            allow_reassignment,
            contract,
            user_id=self.user_id,
            workspace_id=self.workspace_id,
        )
        self.active_run_id = record.run_id
        return {
            "run_id": record.run_id,
            "state": record.state,
            "draft_version": record.draft.version_id,
            "draft_digest": record.draft.snapshot_digest,
            "closure_contract": record.closure_contract.model_dump(mode="json") if record.closure_contract else None,
        }

    @staticmethod
    def run_view(record: Any) -> dict[str, Any]:
        return {
            "run_id": record.run_id,
            "state": record.state,
            "status": ObserverStatus.status(record.state),
            "draft_version": record.draft.version_id,
            "committed_version": record.committed.version_id if record.committed else None,
            "execution_id": record.execution_id,
            "outcome": record.outcome,
        }

    @staticmethod
    def tool_specs() -> list[dict[str, Any]]:
        contract_schema = ClosureContract.model_json_schema()
        patch_op_schema = {
            "type": "object",
            "description": "One deterministic closure refinement operation. Use canonical field `kind`; `op` is accepted as an alias.",
            "properties": {
                "kind": {
                    "type": "string",
                    "enum": [
                        "set_program_ref",
                        "set_compute_spec",
                        "add_typed_hole",
                        "bind_compute_hole",
                        "set_execution_payload",
                        "set_result_expectation",
                        "add_constraint",
                        "tighten_constraint",
                        "materialize_capability_package_candidate",
                    ],
                },
                "op": {"type": "string"},
                "operation": {"type": "string"},
                "value": {},
            },
            "required": ["kind", "value"],
        }
        return [
            {
                "type": "function",
                "name": "loom_open_run",
                "description": "Open a Loom Run after the user's goal, success criteria, effects, budget, recovery policy, and result expectations are understood. The closure_contract is the high-level immutable contract; call this before planning patches.",
                "inputSchema": {
                    "type": "object",
                    "properties": {"closure_contract": contract_schema},
                    "required": ["closure_contract"],
                },
            },
            {
                "type": "function",
                "name": "loom_query_capabilities",
                "description": "Query the current capabilities visible in the bound Workspace.",
                "inputSchema": {"type": "object", "properties": {}},
            },
            {
                "type": "function",
                "name": "loom_apply_plan_patch",
                "description": (
                    "Apply deterministic closure refinement operations after open_run. "
                    "Batch independent operations in one ordered `ops` array whenever possible: "
                    "the complete array is applied atomically and creates one draft version, "
                    "receipt, and draft_patched event. Split into multiple calls only when an "
                    "intermediate readiness result or a prior operation's result is required."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "operation_id": {"type": "string"},
                        "ops": {"type": "array", "items": patch_op_schema},
                    },
                    "required": ["ops"],
                },
            },
            {
                "type": "function",
                "name": "loom_inspect_plan_readiness",
                "description": "Return structured blockers for the current draft closure.",
                "inputSchema": {"type": "object", "properties": {}},
            },
            {
                "type": "function",
                "name": "loom_list_run_capability_packages",
                "description": "List candidate capability packages materialized by this Run; publication remains a user decision.",
                "inputSchema": {"type": "object", "properties": {}},
            },
            {
                "type": "function",
                "name": "loom_commit_plan",
                "description": "Commit the current draft only when readiness is complete.",
                "inputSchema": {"type": "object", "properties": {}},
            },
            {
                "type": "function",
                "name": "loom_start_run",
                "description": "Start execution of the committed closure version.",
                "inputSchema": {"type": "object", "properties": {"closure_version": {"type": "string"}}},
            },
            {
                "type": "function",
                "name": "loom_get_run_status",
                "description": "Read the current Run state and execution outcome.",
                "inputSchema": {"type": "object", "properties": {}},
            },
            {
                "type": "function",
                "name": "loom_close_run",
                "description": "Close a terminal Run after its outcome has been reconciled.",
                "inputSchema": {"type": "object", "properties": {}},
            },
        ]


class ObserverStatus:
    @staticmethod
    def status(state: str) -> str:
        return {
            "opened": "idle",
            "thinking": "thinking",
            "committed": "executing",
            "running": "executing",
            "completed": "completed",
            "cancelled": "interrupted",
            "failed": "failed",
            "closed": "idle",
        }.get(state, "idle")
