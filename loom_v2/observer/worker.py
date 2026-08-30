from __future__ import annotations

import asyncio
import os
from typing import Any

import httpx

from loom_v2.contracts.types import (
    CapabilityHealthReport,
    CapabilityPackageVersion,
    CapabilityProvisionCommand,
    ComputeBinding,
    ResourceRef,
    TaskClosure,
)
from loom_v2.slave.executor import ExecutionResult


class WorkerSession:
    """Authenticated-session-shaped HTTP client for one bound Slave.

    The current single-user deployment uses loopback HTTP; the envelope keeps
    the stable attempt/execution epoch fields needed by the full WorkerSession
    protocol and is ready for TLS/auth transport configuration.
    """

    def __init__(
        self,
        slave_id: str,
        base_url: str,
        *,
        timeout: float | None = None,
        operation_timeout: float | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.slave_id = slave_id
        self.base_url = base_url.rstrip("/")
        configured_timeout = os.getenv("LOOM_WORKER_OPERATION_TIMEOUT_SECONDS", "90")
        self.operation_timeout = operation_timeout if operation_timeout is not None else timeout if timeout is not None else float(configured_timeout)
        # ``timeout`` remains as a compatibility alias for existing callers.
        self.timeout = self.operation_timeout
        self.transport = transport

    async def dispatch(
        self,
        *,
        attempt_id: str,
        execution_id: str,
        execution_epoch: int,
        workspace_id: str,
        operation: str,
        payload: dict[str, Any],
        closure: TaskClosure,
        binding: ComputeBinding | None,
    ) -> ExecutionResult:
        request = {
            "attempt_id": attempt_id,
            "execution_id": execution_id,
            "execution_epoch": execution_epoch,
            "workspace_id": workspace_id,
            "operation": operation,
            "closure": closure.model_dump(mode="json"),
            "binding": binding.model_dump(mode="json") if binding is not None else None,
        }
        # A bound closure carries an immutable content reference.  Do not
        # transmit a mutable duplicate payload across the Worker boundary;
        # the Slave rereads and validates the binding itself.  Legacy closures
        # without an input binding still use the small inline envelope for
        # built-in operations such as echo/sort.
        if not closure.node_input_bindings:
            request["payload"] = payload
        response = await self._post("/worker/v1/dispatch", request)
        if response.status_code >= 400:
            detail = response.json().get("detail", "worker_dispatch_failed")
            raise RuntimeError(str(detail))
        envelope = response.json()
        report = envelope.get("terminal_report") or {}
        terminal_state = str(report.get("state") or "")
        if envelope.get("accepted") is not True or terminal_state not in {"completed", "failed", "decision_required"}:
            raise RuntimeError("worker_dispatch_not_completed")
        if envelope.get("attempt_id") != attempt_id or report.get("attempt_id") != attempt_id:
            raise RuntimeError("stale_attempt")
        if envelope.get("execution_id") != execution_id or report.get("execution_id") != execution_id:
            raise RuntimeError("stale_execution_id")
        if report.get("execution_epoch") != execution_epoch or envelope.get("execution_epoch") != execution_epoch:
            raise RuntimeError("stale_execution_epoch")
        result = report.get("result") or {}
        return ExecutionResult(
            resource_ref=ResourceRef.model_validate(result["resource_ref"]),
            value=result["value"],
            replay_safety=result.get("replay_safety", "Idempotent"),
            digest=result["digest"],
            terminal_state=terminal_state,
            terminal_error=report.get("error") or report.get("terminal_error"),
            validation_evidence=report.get("validation_evidence") or [],
        )

    async def provision(
        self,
        *,
        command: CapabilityProvisionCommand,
        package: CapabilityPackageVersion,
    ) -> CapabilityHealthReport:
        """Install a package on this WorkerSession's exact target Slave."""
        request = {
            "command": command.model_dump(mode="json"),
            "package": package.model_dump(mode="json"),
        }
        response = await self._post("/worker/v1/provision", request)
        if response.status_code >= 400:
            detail = response.json().get("detail", "worker_provision_failed")
            raise RuntimeError(str(detail))
        return CapabilityHealthReport.model_validate(response.json().get("health_report") or response.json())

    async def _post(self, path: str, payload: dict[str, Any]) -> httpx.Response:
        """Perform one bounded Worker operation.

        The explicit asyncio deadline complements httpx's transport timeout;
        in-process ASGI transports do not consistently enforce read timeouts,
        while a real Slave call must never inherit the Conversation deadline.
        """
        try:
            async with asyncio.timeout(self.operation_timeout):
                async with httpx.AsyncClient(timeout=self.operation_timeout, transport=self.transport) as client:
                    return await client.post(f"{self.base_url}{path}", json=payload)
        except (TimeoutError, httpx.TimeoutException) as exc:
            raise RuntimeError("worker_operation_timeout") from exc
