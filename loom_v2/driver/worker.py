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


class WorkerUnavailableError(RuntimeError):
    pass


class WorkerSession:
    """Authenticated HTTP client for one directly-connected Slave."""

    def __init__(
        self,
        slave_id: str,
        base_url: str,
        *,
        timeout: float | None = None,
        operation_timeout: float | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        internal_api_secret: str | None = None,
    ) -> None:
        self.slave_id = slave_id
        self.base_url = base_url.rstrip("/")
        configured_timeout = os.getenv("LOOM_WORKER_OPERATION_TIMEOUT_SECONDS", "90")
        self.operation_timeout = operation_timeout if operation_timeout is not None else timeout if timeout is not None else float(configured_timeout)
        self.timeout = self.operation_timeout
        self.transport = transport
        self.internal_api_secret = internal_api_secret or os.getenv("LOOM_INTERNAL_API_SECRET", "")

    def _headers(self) -> dict[str, str]:
        if not self.internal_api_secret:
            return {}
        return {"X-Loom-Internal-Token": self.internal_api_secret, "Authorization": f"Bearer {self.internal_api_secret}"}

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
        driver_id: str | None = None,
        driver_epoch: int | None = None,
    ) -> ExecutionResult:
        request: dict[str, Any] = {
            "attempt_id": attempt_id,
            "execution_id": execution_id,
            "execution_epoch": execution_epoch,
            "workspace_id": workspace_id,
            "operation": operation,
            "closure": closure.model_dump(mode="json"),
            "binding": binding.model_dump(mode="json") if binding is not None else None,
        }
        if driver_id is not None:
            request["driver_id"] = driver_id
        if driver_epoch is not None:
            request["driver_epoch"] = driver_epoch
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
        driver_id: str | None = None,
        driver_epoch: int | None = None,
    ) -> CapabilityHealthReport:
        payload: dict[str, Any] = {"command": command.model_dump(mode="json"), "package": package.model_dump(mode="json")}
        if driver_id is not None:
            payload["driver_id"] = driver_id
        if driver_epoch is not None:
            payload["driver_epoch"] = driver_epoch
        response = await self._post("/worker/v1/provision", payload)
        if response.status_code >= 400:
            detail = response.json().get("detail", "worker_provision_failed")
            raise RuntimeError(str(detail))
        return CapabilityHealthReport.model_validate(response.json().get("health_report") or response.json())

    async def _post(self, path: str, payload: dict[str, Any]) -> httpx.Response:
        try:
            async with asyncio.timeout(self.operation_timeout):
                async with httpx.AsyncClient(timeout=self.operation_timeout, transport=self.transport) as client:
                    return await client.post(f"{self.base_url}{path}", json=payload, headers=self._headers())
        except (TimeoutError, httpx.TimeoutException) as exc:
            raise WorkerUnavailableError("worker_operation_timeout") from exc
        except httpx.TransportError as exc:
            raise WorkerUnavailableError("worker_unavailable") from exc
