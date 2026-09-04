from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from loom_v2.content_store import ContentStore, canonical_json_bytes
from loom_v2.settings import Settings
from loom_v2.contracts.types import (
    CapabilityHealthReport,
    CapabilityPackageActivation,
    CapabilityPackageVersion,
    ClosureContract,
    ClosureVersion,
    ComputeBinding,
    DynamicNode,
    NodeIntent,
    ResourceRef,
    TaskClosure,
)

from .control_client import ObserverControlClient


def _select_slave_agents(agents: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    selected: dict[str, dict[str, Any]] = {}
    for agent in agents:
        slave_id = str(agent.get("agent_id") or "")
        if not slave_id:
            continue
        current = selected.get(slave_id)
        if current is None or (
            agent.get("lease_state") == "active"
            and current.get("lease_state") != "active"
        ):
            selected[slave_id] = dict(agent)
    return selected


@dataclass
class RemoteRunRecord:
    """The small read model the Driver needs from Observer.

    It intentionally contains no mutation methods.  All writes go through
    ``ObserverControlClient.command`` so the Observer remains the authority.
    """

    run_id: str
    task_ref: str
    goal: str
    draft: ClosureVersion
    closure_contract: ClosureContract | None = None
    allow_reassignment: bool = False
    committed: ClosureVersion | None = None
    execution_id: str | None = None
    execution_epoch: int = 1
    state: str = "opened"
    outcome: dict[str, Any] | None = None
    events: list[dict[str, Any]] | None = None
    attempts: list[dict[str, Any]] | None = None
    capability_packages: list[CapabilityPackageVersion] | None = None
    capability_activations: list[CapabilityPackageActivation] | None = None
    dynamic_nodes: list[DynamicNode] | None = None

    def __post_init__(self) -> None:
        self.events = self.events or []
        self.attempts = self.attempts or []
        self.capability_packages = self.capability_packages or []
        self.capability_activations = self.capability_activations or []
        self.dynamic_nodes = self.dynamic_nodes or []


def _decode_run(payload: dict[str, Any]) -> RemoteRunRecord:
    draft = ClosureVersion.model_validate(payload["draft"])
    committed_payload = payload.get("committed")
    contract_payload = payload.get("closure_contract")
    return RemoteRunRecord(
        run_id=str(payload["run_id"]),
        task_ref=str(payload.get("task_ref") or ""),
        goal=str(payload.get("goal") or ""),
        draft=draft,
        closure_contract=ClosureContract.model_validate(contract_payload) if contract_payload else None,
        allow_reassignment=bool(payload.get("allow_reassignment", False)),
        committed=ClosureVersion.model_validate(committed_payload) if committed_payload else None,
        execution_id=payload.get("execution_id"),
        execution_epoch=int(payload.get("execution_epoch", 1)),
        state=str(payload.get("state") or "opened"),
        outcome=payload.get("outcome"),
        events=list(payload.get("events") or []),
        attempts=list(payload.get("attempts") or []),
        capability_packages=[CapabilityPackageVersion.model_validate(item) for item in payload.get("capability_packages", [])],
        capability_activations=[CapabilityPackageActivation.model_validate(item) for item in payload.get("capability_activations", [])],
        dynamic_nodes=[DynamicNode.model_validate(item) for item in payload.get("dynamic_nodes", [])],
    )


class RemoteObserverRepository:
    """Repository-shaped Driver adapter backed solely by Observer RPC."""

    def __init__(self, control: ObserverControlClient, content_store: ContentStore | None = None) -> None:
        self.control = control
        self.content_store = content_store or ContentStore.from_settings(Settings())
        self.workspace_id = control.workspace_id
        self.slave_capabilities: dict[str, dict[str, Any]] = {}
        self.slave_agents: dict[str, dict[str, Any]] = {}
        self.slave_instances: dict[tuple[str, str], dict[str, Any]] = {}

    async def get_run(self, run_id: str) -> RemoteRunRecord:
        return _decode_run(await self.control.command("run.get", {"run_id": run_id}))

    async def begin_refinement(self, run_id: str) -> RemoteRunRecord:
        payload = await self.control.command("run.begin", {"run_id": run_id}, request_id=f"run-begin:{run_id}")
        return _decode_run(payload)

    async def append_message(self, run_id: str, role: str, content: str, *, request_id: str | None = None) -> dict[str, Any]:
        return await self.control.command(
            "message.append",
            {"run_id": run_id, "role": role, "content": content, "request_id": request_id},
            request_id=f"{request_id}:message" if request_id else None,
        )

    async def claim_message(self, request_id: str, payload_digest: str, *, conversation_ref: str | None = None, claim_token: str) -> dict[str, Any]:
        return await self.control.claim_message(request_id, payload_digest, conversation_ref=conversation_ref, claim_token=claim_token)

    async def update_message(self, request_id: str, *, claim_token: str, state: str | None = None, assistant_text: str | None = None, run_id: str | None = None, outcome: dict[str, Any] | None = None) -> dict[str, Any]:
        return await self.control.update_message(request_id, claim_token=claim_token, state=state, assistant_text=assistant_text, run_id=run_id, outcome=outcome)

    async def release_message(self, request_id: str, *, claim_token: str) -> dict[str, Any]:
        return await self.control.release_message(request_id, claim_token=claim_token)

    async def list_capability_packages(self, *, run_id: str | None = None, include_abandoned: bool = False) -> list[CapabilityPackageVersion]:
        payload = await self.control.command(
            "capability.list",
            {"run_id": run_id, "include_abandoned": include_abandoned},
        )
        return [CapabilityPackageVersion.model_validate(item) for item in payload.get("packages", [])]

    async def get_capability_package(self, package_ref: str | ResourceRef, *, run_id: str | None = None) -> CapabilityPackageVersion:
        ref = package_ref.model_dump(mode="json") if isinstance(package_ref, ResourceRef) else package_ref
        payload = await self.control.command("capability.get", {"package_ref": ref, "run_id": run_id})
        return CapabilityPackageVersion.model_validate(payload)

    async def refresh_slaves(self, workspace_id: str | None = None) -> list[dict[str, Any]]:
        agents = await self.control.list_slaves()
        self.slave_instances = {
            (str(agent.get("agent_id") or ""), str(agent.get("instance_id") or "")): dict(agent)
            for agent in agents
            if agent.get("agent_id")
        }
        self.slave_agents = _select_slave_agents(agents)
        self.slave_capabilities = {}
        for slave_id, agent in self.slave_agents.items():
            if agent.get("lease_state") != "active":
                continue
            capabilities = dict(agent.get("capabilities") or {})
            operations = capabilities.get("operations", [])
            self.slave_capabilities[slave_id] = {
                **capabilities,
                "operations": set(operations),
            }
        return agents

    def _slave_supports_package(self, slave_id: str, package: CapabilityPackageVersion) -> bool:
        agent = self.slave_agents.get(slave_id)
        if agent is None or agent.get("lease_state") != "active":
            return False
        operation = package.executor_operation or "run_code"
        details = self.slave_capabilities.get(slave_id, {})
        if operation not in details.get("operations", set()):
            return False
        declared_executors = (
            details.get("executor_kinds")
            or details.get("executors")
            or details.get("executor_descriptors")
        )
        if declared_executors:
            raw_executors = [declared_executors] if isinstance(declared_executors, str) else declared_executors
            executor_kinds = {
                str(item.get("kind")) if isinstance(item, dict) else str(item)
                for item in raw_executors
            }
            return package.executor_kind in executor_kinds
        return True

    async def accept_node_intent(self, run_id: str, intent: NodeIntent, *, selected_target: str) -> DynamicNode:
        payload = await self.control.command(
            "node.accept",
            {"run_id": run_id, "intent": intent.model_dump(mode="json"), "selected_target": selected_target},
        )
        return DynamicNode.model_validate(payload)

    async def dispatch_dynamic_node(self, run_id: str, node_id: str, *, target: str) -> dict[str, Any]:
        payload = await self.control.command("node.dispatch", {"run_id": run_id, "node_id": node_id, "target": target})
        return dict(payload["attempt"])

    async def reassign_dynamic_node(self, run_id: str, node_id: str, **arguments: Any) -> dict[str, Any]:
        payload = await self.control.command(
            "node.reassign",
            {"run_id": run_id, "node_id": node_id, **arguments},
            request_id=f"node-reassign:{arguments['lost_attempt_id']}:{arguments['target']}",
        )
        return dict(payload["attempt"])

    async def record_dynamic_node_result(self, run_id: str, node_id: str, result: dict[str, Any]) -> DynamicNode:
        payload = await self.control.command("node.result", {"run_id": run_id, "node_id": node_id, "result": result})
        return DynamicNode.model_validate(payload)

    async def fail_dynamic_node(self, run_id: str, node_id: str, *, attempt_id: str | None = None, error: dict[str, Any] | None = None) -> DynamicNode:
        payload = await self.control.command(
            "node.fail",
            {"run_id": run_id, "node_id": node_id, "attempt_id": attempt_id, "reason": error or {"code": "dynamic_node_failed"}},
        )
        return DynamicNode.model_validate(payload)

    async def record_capability_health(self, report: CapabilityHealthReport, *, run_id: str | None = None) -> CapabilityPackageActivation:
        payload = await self.control.command(
            "capability.health",
            {"run_id": run_id, "report": report.model_dump(mode="json")},
        )
        return CapabilityPackageActivation.model_validate(payload)

    async def record_result(self, run_id: str, result: dict[str, Any]) -> RemoteRunRecord:
        payload = await self.control.command("run.result", {"run_id": run_id, "result": result})
        return _decode_run(payload)

    async def fail_run(self, run_id: str, reason: str | dict[str, Any]) -> RemoteRunRecord:
        """Use the fixed cancellation command for a failed remote turn.

        Driver failures use their own fenced lifecycle transition so an
        execution error is not confused with an explicit user cancellation.
        """
        payload = await self.control.command("run.fail", {"run_id": run_id, "reason": reason})
        return _decode_run(payload)

    async def resolve_run(self, run_id: str, decision: str) -> RemoteRunRecord:
        payload = await self.control.command(
            "run.resolve",
            {"run_id": run_id, "decision": decision},
            request_id=f"run-resolve:{run_id}:{decision}",
        )
        return _decode_run(payload)

    async def complete_orchestration(self, run_id: str, final_ref: ResourceRef) -> RemoteRunRecord:
        payload = await self.control.command(
            "run.result",
            {"run_id": run_id, "result": {"orchestration_final_ref": final_ref.model_dump(mode="json")}},
            request_id=f"orchestration:{run_id}:{final_ref.version_or_digest or final_ref.resource_id}",
        )
        return _decode_run(payload)

    async def put_content(self, content: Any, *, media_type: str) -> ResourceRef:
        if isinstance(content, bytes):
            body = content
        elif isinstance(content, str):
            body = content.encode("utf-8")
        else:
            body = canonical_json_bytes(content)
        return await self.content_store.put(body, media_type=media_type)

    async def _load_json_content(self, ref: ResourceRef) -> Any:
        raw = await self.content_store.get(ref)
        import json

        return json.loads(raw)

    @staticmethod
    def _input_binding_for(snapshot: TaskClosure, operation_ref: str):
        candidates = {operation_ref, operation_ref.rstrip("/").rsplit("/", 1)[-1].rsplit(":", 1)[-1], "default"}
        return next((binding for binding in snapshot.node_input_bindings if binding.node_id in candidates), None)

    @staticmethod
    def _operation_name(operation_ref: str) -> str:
        return str(operation_ref or "").rstrip("/").rsplit("/", 1)[-1].rsplit(":", 1)[-1]
