from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import uuid4

from loom_v2.contracts.types import ClosureContract, TaskClosure
from loom_v2.observer.repository import ObserverRepository
from loom_v2.content_store import ContentStore

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
        content_store: ContentStore | None = None,
        request_id: str | None = None,
        prompt: str | None = None,
        run_executor: Callable[[str, str], Awaitable[Any]] | None = None,
    ) -> None:
        self.repository = repository
        self.control = repository if hasattr(repository, "command") and not hasattr(repository, "get_run") else None
        self.conversation_ref = conversation_ref
        self.user_id = user_id
        self.workspace_id = workspace_id
        self.content_store = content_store
        self.request_id = request_id
        self.prompt = prompt or ""
        self.run_executor = run_executor
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
            return await self._query_capabilities()
        if tool_name == "loom_put_content":
            if "content" not in arguments:
                raise ValueError("content_required")
            if self.control is not None:
                if self.content_store is None:
                    raise RuntimeError("content_store_unavailable")
                media_type = str(arguments.get("media_type") or "application/json")
                raw = arguments["content"]
                body = raw if isinstance(raw, (bytes, bytearray)) else raw.encode("utf-8") if isinstance(raw, str) else json.dumps(raw, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
                ref = await self.content_store.put(body, media_type=media_type)
                return {"resource_ref": ref.model_dump(mode="json")}
            ref = await self.repository.put_content(
                arguments["content"],
                media_type=str(arguments.get("media_type") or ""),
            )
            return {"resource_ref": ref.model_dump(mode="json")}
        if tool_name == "loom_list_run_capability_packages":
            if self.active_run_id is None:
                return {"packages": []}
            if self.control is not None:
                packages = await self.control.list_capability_packages(run_id=self.active_run_id, include_abandoned=True)
            else:
                packages = await self.repository.list_capability_packages(run_id=self.active_run_id, include_abandoned=True)
            return {"packages": [package.model_dump(mode="json") for package in packages]}
        if self.active_run_id is None:
            raise ValueError("run_not_open")
        if tool_name == "loom_apply_plan_patch":
            operation_id = str(arguments.get("operation_id") or f"mcp-patch-{uuid4().hex[:12]}")
            if self.control is not None:
                return await self.control.command(
                    "run.patch",
                    {"run_id": self.active_run_id, "operation_id": operation_id, "ops": arguments.get("ops", [])},
                    request_id=operation_id,
                )
            return await self.tools.apply_plan_patch(self.active_run_id, operation_id, arguments.get("ops", []))
        if tool_name == "loom_inspect_plan_readiness":
            if self.control is not None:
                return await self.control.command("run.readiness", {"run_id": self.active_run_id})
            return await self.tools.inspect_plan_readiness(self.active_run_id)
        if tool_name == "loom_commit_plan":
            if self.control is not None:
                run = await self.control.command("run.get", {"run_id": self.active_run_id})
                return await self.control.command(
                    "run.commit",
                    {"run_id": self.active_run_id, "version_id": run.get("draft_version"), "digest": run.get("draft_digest")},
                    request_id=f"{self.request_id}:commit" if self.request_id else None,
                )
            return await self.tools.commit_plan(self.active_run_id)
        if tool_name == "loom_start_run":
            if self.control is not None:
                run = await self.control.command("run.get", {"run_id": self.active_run_id})
                version_id = arguments.get("closure_version") or run.get("committed_version")
                if not version_id:
                    raise ValueError("closure_not_committed")
                await self.control.command(
                    "run.start",
                    {"run_id": self.active_run_id, "version_id": version_id},
                    request_id=f"{self.request_id}:start" if self.request_id else None,
                )
                if self.run_executor is not None:
                    await self.run_executor(self.active_run_id, self.prompt)
                result = self._decorate_run_payload(await self.control.command("run.get", {"run_id": self.active_run_id}))
                result["success"] = True
                return result
            run = await self.repository.get_run(self.active_run_id)
            version_id = arguments.get("closure_version") or (run.committed.version_id if run.committed else None)
            if not version_id:
                raise ValueError("closure_not_committed")
            await self.tools.start_run(self.active_run_id, version_id)
            if self.run_executor is not None:
                await self.run_executor(self.active_run_id, self.prompt)
            result = self.run_view(await self.repository.get_run(self.active_run_id))
            result["success"] = True
            return result
        if tool_name == "loom_get_run_status":
            if self.control is not None:
                return self._decorate_run_payload(await self.control.command("run.get", {"run_id": self.active_run_id}))
            return self.run_view(await self.repository.get_run(self.active_run_id))
        if tool_name == "loom_close_run":
            if self.control is not None:
                return await self.control.command("run.close", {"run_id": self.active_run_id})
            return self.run_view(await self.repository.close_run(self.active_run_id))
        if tool_name == "loom_resolve_run":
            decision = str(arguments.get("decision") or "")
            if decision not in {"accept", "abandon"}:
                raise ValueError("invalid_decision")
            if self.control is not None:
                return self._decorate_run_payload(await self.control.command(
                    "run.resolve",
                    {"run_id": self.active_run_id, "decision": decision},
                    request_id=f"{self.request_id}:resolve:{decision}" if self.request_id else None,
                ))
            return self.run_view(await self.repository.resolve_run(self.active_run_id, decision))
        raise ValueError(f"unknown_driver_tool:{tool_name}")

    async def _query_capabilities(self) -> dict[str, Any]:
        if self.control is not None:
            result = await self.control.command("capability.list", {})
            return {"workspace_id": self.workspace_id, "capabilities": result.get("capabilities", []), "packages": result.get("packages", [])}
        await self.repository.refresh_slaves(self.workspace_id)
        return {
            "workspace_id": self.workspace_id,
            "capabilities": [
                {
                    "resource_id": resource_id,
                    "available": self.repository._slave_is_active(resource_id),
                    "operations": sorted(details.get("operations", set())),
                }
                for resource_id, details in sorted(self.repository.slave_capabilities.items())
            ],
            "executor_descriptors": [
                {"kind": "builtin_v1", "version": "1", "operations": ["echo", "hash", "sort", "run_code"]},
                {"kind": "subprocess_json_v1", "version": "1", "operations": ["run_code"]},
                {"kind": "orchestrator_python_v1", "version": "1", "operations": ["orchestrate"]},
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
        if not self.prompt:
            self.prompt = contract.goal
        if self.control is not None:
            result = await self.control.command(
                "run.open",
                {
                    "run_id": arguments.get("run_id"),
                    "task_ref": self.conversation_ref,
                    "goal": contract.goal,
                    "allow_reassignment": bool(contract.recovery_policy.get("allow_reassignment", False)),
                    "closure_contract": contract.model_dump(mode="json"),
                    "user_id": self.user_id,
                },
                request_id=self.request_id,
            )
            self.active_run_id = result.get("run_id")
            return result
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
        outcome = record.outcome or {}
        state = record.state
        decision = outcome.get("decision") if isinstance(outcome, dict) else None
        hint = {
            "repair": "repair_plan_and_retry",
            "attestation": "resolve_run_accept_or_abandon",
        }.get(decision, "none")
        return {
            "run_id": record.run_id,
            "state": record.state,
            "status": ObserverStatus.status(record.state),
            "draft_version": record.draft.version_id,
            "committed_version": record.committed.version_id if record.committed else None,
            "execution_id": record.execution_id,
            "outcome": record.outcome,
            "disposition": outcome.get("disposition") if isinstance(outcome, dict) else None,
            "decision": decision,
            "resource_ref": outcome.get("resource_ref") if isinstance(outcome, dict) else None,
            "value": outcome.get("value") if isinstance(outcome, dict) else None,
            "terminal_error": outcome.get("terminal_error") if isinstance(outcome, dict) else None,
            "decision_hint": hint,
            "dynamic_nodes": [node.model_dump(mode="json") for node in record.dynamic_nodes],
        }

    @staticmethod
    def _decorate_run_payload(payload: dict[str, Any]) -> dict[str, Any]:
        outcome = payload.get("outcome") or {}
        decision = outcome.get("decision") if isinstance(outcome, dict) else None
        return {
            **payload,
            "status": payload.get("status") or ObserverStatus.status(str(payload.get("state") or "")),
            "disposition": outcome.get("disposition") if isinstance(outcome, dict) else None,
            "decision": decision,
            "resource_ref": outcome.get("resource_ref") if isinstance(outcome, dict) else None,
            "value": outcome.get("value") if isinstance(outcome, dict) else None,
            "terminal_error": outcome.get("terminal_error") if isinstance(outcome, dict) else None,
            "decision_hint": {
                "repair": "repair_plan_and_retry",
                "attestation": "resolve_run_accept_or_abandon",
            }.get(decision, "none"),
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
                        "set_io_contract_ref",
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
                "name": "loom_put_content",
                "description": "Upload one immutable content object. JSON Schema and IoContract bodies are canonicalized and validated; use media_type to distinguish schemas, contracts, programs, and artifacts.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "media_type": {"type": "string"},
                        "content": {},
                    },
                    "required": ["media_type", "content"],
                },
            },
            {
                "type": "function",
                "name": "loom_apply_plan_patch",
                "description": (
                    "Apply deterministic closure refinement operations after open_run. "
                    "Batch independent operations in one ordered `ops` array whenever possible: "
                    "the complete array is applied atomically and creates one draft version, "
                    "receipt, and draft_patched event. Split into multiple calls only when an "
                    "intermediate readiness result or a prior operation's result is required. "
                    "For `set_execution_payload`, provide `value.input_ref` (a ResourceRef "
                    "returned by `loom_put_content`); raw payload values are rejected. "
                    "Capability package materialization likewise requires pre-uploaded "
                    "`program_content_ref` and `io_contract_ref`."
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
                "description": "Return structured blockers for the current draft closure. Orchestration programs are checked with Pyright; diagnostics include file, line, column, end position, severity, rule, and message.",
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
                "description": "Commit the current draft only when readiness is complete. A blocked orchestration program returns readiness_blocked with the complete Pyright diagnostics for repair.",
                "inputSchema": {"type": "object", "properties": {}},
            },
            {
                "type": "function",
                "name": "loom_start_run",
                "description": "Start execution of the committed closure version and wait for its complete outcome. Readiness failures preserve readiness_blocked and all diagnostics; no source is rewritten automatically.",
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
            {
                "type": "function",
                "name": "loom_resolve_run",
                "description": "Resolve an awaiting-decision Run: accept attestation or abandon the result.",
                "inputSchema": {
                    "type": "object",
                    "properties": {"decision": {"type": "string", "enum": ["accept", "abandon"]}},
                    "required": ["decision"],
                },
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
            "awaiting_decision": "awaiting_decision",
            "cancelled": "interrupted",
            "failed": "failed",
            "closed": "idle",
        }.get(state, "idle")
