from __future__ import annotations

import asyncio
import os
from typing import Any
from uuid import uuid4

import httpx

from loom_v2.contracts.agents import AgentLease, AgentRegistration, DriverCommand
from loom_v2.contracts.errors import DomainError, DomainErrorEnvelope
from loom_v2.contracts.types import CapabilityPackageVersion, ResourceRef


class ObserverControlClient:
    """Small authenticated client for Observer's fixed Driver RPC surface."""

    ALLOWED_COMMANDS = frozenset(
        {
            "run.open", "run.begin", "run.get", "run.patch", "run.commit", "run.start", "run.close", "run.cancel", "run.fail", "run.resolve",
            "run.readiness", "run.recovery.list", "run.recovery.mark", "message.append", "message.claim", "message.update", "message.release", "agent_signal.record",
            "run.result",
            "thread.bind", "thread.get", "turn.state", "capability.list", "capability.get", "capability.health",
            "node.accept", "node.dispatch", "node.result", "node.fail",
        }
    )

    def __init__(
        self,
        observer_url: str,
        *,
        driver_id: str,
        instance_id: str,
        lease_id: str | None = None,
        driver_epoch: int | None = None,
        workspace_id: str = "workspace-default",
        internal_api_secret: str | None = None,
        timeout: float = 10.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.observer_url = observer_url.rstrip("/")
        self.driver_id = driver_id
        self.instance_id = instance_id
        self.lease_id = lease_id
        self.driver_epoch = driver_epoch
        self.workspace_id = workspace_id
        self.internal_api_secret = internal_api_secret if internal_api_secret is not None else os.getenv("LOOM_INTERNAL_API_SECRET", "")
        self.timeout = timeout
        self.transport = transport

    def _headers(self) -> dict[str, str]:
        if not self.internal_api_secret:
            return {}
        return {"X-Loom-Internal-Token": self.internal_api_secret, "Authorization": f"Bearer {self.internal_api_secret}"}

    async def _request(self, method: str, path: str, payload: dict[str, Any] | None = None, *, timeout: float | None = None) -> dict[str, Any]:
        request_timeout = timeout if timeout is not None else self.timeout
        try:
            async with asyncio.timeout(request_timeout):
                async with httpx.AsyncClient(timeout=request_timeout, transport=self.transport) as client:
                    response = await client.request(method, f"{self.observer_url}{path}", json=payload, headers=self._headers())
        except (TimeoutError, httpx.TimeoutException, httpx.TransportError) as exc:
            raise RuntimeError("observer_unavailable") from exc
        if response.status_code >= 400:
            try:
                detail = response.json().get("detail", "observer_request_failed")
            except Exception:
                detail = "observer_request_failed"
            if isinstance(detail, dict) and detail.get("code"):
                try:
                    raise DomainError(DomainErrorEnvelope.model_validate(detail))
                except DomainError:
                    raise
                except Exception:
                    pass
            raise RuntimeError(str(detail))
        if not response.content:
            return {}
        value = response.json()
        return value if isinstance(value, dict) else {"result": value}

    async def register(self, registration: AgentRegistration | None = None) -> AgentLease:
        registration = registration or AgentRegistration(
            role="driver",
            agent_id=self.driver_id,
            instance_id=self.instance_id,
            workspace_id=self.workspace_id,
            endpoint_url=os.getenv("LOOM_DRIVER_URL", "http://driver:8090"),
            protocol_version="loom.v1",
        )
        payload = await self._request("POST", "/internal/v1/agents/register", registration.model_dump(mode="json"))
        lease = AgentLease.model_validate(payload)
        self.lease_id = lease.lease_id
        self.driver_epoch = lease.epoch
        self.workspace_id = lease.workspace_id
        return lease

    async def heartbeat(self) -> AgentLease:
        self._require_lease()
        payload = await self._request(
            "POST",
            f"/internal/v1/agents/{self.driver_id}/heartbeat",
            {"instance_id": self.instance_id, "workspace_id": self.workspace_id, "lease_id": self.lease_id, "epoch": self.driver_epoch, "driver_epoch": self.driver_epoch, "role": "driver"},
        )
        lease = AgentLease.model_validate(payload)
        self.lease_id = lease.lease_id
        self.driver_epoch = lease.epoch
        return lease

    async def release(self) -> None:
        if self.lease_id is None or self.driver_epoch is None:
            return
        await self._request(
            "POST",
            f"/internal/v1/agents/{self.driver_id}/release",
            {"instance_id": self.instance_id, "workspace_id": self.workspace_id, "lease_id": self.lease_id, "epoch": self.driver_epoch, "driver_epoch": self.driver_epoch, "role": "driver"},
        )

    def _require_lease(self) -> None:
        if not self.lease_id or self.driver_epoch is None:
            raise RuntimeError("driver_not_registered")

    async def command(self, command: str, arguments: dict[str, Any] | None = None, *, request_id: str | None = None) -> dict[str, Any]:
        if command not in self.ALLOWED_COMMANDS:
            raise ValueError("driver_command_not_allowed")
        self._require_lease()
        envelope = DriverCommand(
            request_id=request_id or f"driver-request-{os.urandom(8).hex()}",
            driver_id=self.driver_id,
            instance_id=self.instance_id,
            lease_id=str(self.lease_id),
            driver_epoch=int(self.driver_epoch),
            command=command,
            arguments={**(arguments or {}), "workspace_id": self.workspace_id},
        )
        return await self._request("POST", "/internal/v1/driver/commands", envelope.model_dump(mode="json"))

    async def thread_get(self, conversation_ref: str) -> dict[str, Any] | None:
        result = await self.command("thread.get", {"conversation_ref": conversation_ref})
        return result or None

    async def thread_binding(self, conversation_ref: str) -> dict[str, Any] | None:
        return await self.thread_get(conversation_ref)

    async def thread_bind(self, binding: dict[str, Any]) -> dict[str, Any]:
        return await self.command("thread.bind", binding)

    async def turn_state(self, conversation_ref: str, turn_state: str, *, request_id: str | None = None, turn_id: str | None = None) -> dict[str, Any]:
        command_request_id = f"turn-state:{conversation_ref}:{turn_state}:{request_id or uuid4().hex}"
        return await self.command("turn.state", {"conversation_ref": conversation_ref, "turn_state": turn_state, "request_id": request_id, "turn_id": turn_id}, request_id=command_request_id)

    async def claim_message(self, request_id: str, payload_digest: str, *, conversation_ref: str | None = None, claim_token: str) -> dict[str, Any]:
        return await self.command(
            "message.claim",
            {"request_id": request_id, "conversation_ref": conversation_ref, "payload_digest": payload_digest, "claim_token": claim_token},
            request_id=f"message-claim:{request_id}:{claim_token}",
        )

    async def update_message(self, request_id: str, *, claim_token: str, state: str | None = None, assistant_text: str | None = None, run_id: str | None = None, outcome: dict[str, Any] | None = None) -> dict[str, Any]:
        return await self.command(
            "message.update",
            {"request_id": request_id, "claim_token": claim_token, "state": state, "assistant_text": assistant_text, "run_id": run_id, "outcome": outcome},
            request_id=f"message-update:{request_id}:{state or 'progress'}:{uuid4().hex}",
        )

    async def release_message(self, request_id: str, *, claim_token: str) -> dict[str, Any]:
        return await self.command(
            "message.release",
            {"request_id": request_id, "claim_token": claim_token},
            request_id=f"message-release:{request_id}:{uuid4().hex}",
        )

    async def list_slaves(self) -> list[dict[str, Any]]:
        payload = await self._request("GET", f"/internal/v1/agents/slaves?workspace_id={self.workspace_id}")
        return payload if isinstance(payload, list) else list(payload.get("agents", payload.get("result", [])))

    async def list_capability_packages(self, *, run_id: str | None = None, include_abandoned: bool = False) -> list[CapabilityPackageVersion]:
        payload = await self.command("capability.list", {"run_id": run_id, "include_abandoned": include_abandoned})
        return [CapabilityPackageVersion.model_validate(item) for item in payload.get("packages", [])]

    async def get_capability_package(self, package_ref: str | ResourceRef, *, run_id: str | None = None) -> CapabilityPackageVersion:
        ref = package_ref.model_dump(mode="json") if isinstance(package_ref, ResourceRef) else package_ref
        payload = await self.command("capability.get", {"package_ref": ref, "run_id": run_id})
        return CapabilityPackageVersion.model_validate(payload)
