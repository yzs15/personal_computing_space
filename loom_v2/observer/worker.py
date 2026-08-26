from __future__ import annotations

from typing import Any

import httpx

from loom_v2.contracts.types import ComputeBinding, ResourceRef, TaskClosure
from loom_v2.slave.executor import ExecutionResult


class WorkerSession:
    """Authenticated-session-shaped HTTP client for one bound Slave.

    The current single-user deployment uses loopback HTTP; the envelope keeps
    the stable attempt/execution epoch fields needed by the full WorkerSession
    protocol and is ready for TLS/auth transport configuration.
    """

    def __init__(self, slave_id: str, base_url: str, *, timeout: float = 90.0, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self.slave_id = slave_id
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
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
            "payload": payload,
            "closure": closure.model_dump(mode="json"),
            "binding": binding.model_dump(mode="json") if binding is not None else None,
        }
        async with httpx.AsyncClient(timeout=self.timeout, transport=self.transport) as client:
            response = await client.post(f"{self.base_url}/worker/v1/dispatch", json=request)
        if response.status_code >= 400:
            detail = response.json().get("detail", "worker_dispatch_failed")
            raise RuntimeError(str(detail))
        envelope = response.json()
        report = envelope.get("terminal_report") or {}
        if not envelope.get("accepted") or report.get("state") != "completed":
            raise RuntimeError("worker_dispatch_not_completed")
        result = report.get("result") or {}
        return ExecutionResult(
            resource_ref=ResourceRef.model_validate(result["resource_ref"]),
            value=result["value"],
            replay_safety=result.get("replay_safety", "Idempotent"),
            digest=result["digest"],
        )
