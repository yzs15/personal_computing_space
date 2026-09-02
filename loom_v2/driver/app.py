from __future__ import annotations

import asyncio
import os
import re
from contextlib import suppress
from uuid import uuid4
from typing import Any

from fastapi import FastAPI, HTTPException, Request

from loom_v2.coding_agents.codex import CodexAppServerProvider
from loom_v2.coding_agents.fake import FakeCodingAgentProvider
from loom_v2.contracts.agents import AgentLease, AgentRegistration
from loom_v2.contracts.types import ComputeBinding, ResourceRef
from loom_v2.settings import Settings
from loom_v2.content_store import ContentStore

from .control_client import ObserverControlClient
from .service import DriverService
from .worker import WorkerSession
from .mcp_server import DriverMCPServer


def create_app(
    *,
    settings: Settings | None = None,
    observer_transport: Any | None = None,
    control_client: ObserverControlClient | None = None,
    provider: Any | None = None,
) -> FastAPI:
    settings = settings or Settings()
    app = FastAPI(title="Loom v2 Driver")
    instance_id = settings.agent_instance_id or f"driver-{uuid4().hex[:12]}"
    control = control_client or ObserverControlClient(
        settings.observer_url,
        driver_id=settings.agent_id,
        instance_id=instance_id,
        workspace_id=settings.workspace_id,
        internal_api_secret=settings.internal_api_secret,
        timeout=settings.worker_operation_timeout_seconds,
        transport=observer_transport,
    )
    if provider is None:
        provider = FakeCodingAgentProvider() if settings.coding_agent_backend.lower() == "fake" else CodexAppServerProvider(model=settings.codex_model, poll_interval_seconds=settings.coding_agent_poll_interval_seconds, protocol_failure_seconds=settings.coding_agent_protocol_failure_seconds)
    workers: dict[str, WorkerSession] = {}
    if settings.slave_a_url:
        workers["slave-a"] = WorkerSession("slave-a", settings.slave_a_url, operation_timeout=settings.worker_operation_timeout_seconds, internal_api_secret=settings.internal_api_secret)
    if settings.slave_b_url:
        workers["slave-b"] = WorkerSession("slave-b", settings.slave_b_url, operation_timeout=settings.worker_operation_timeout_seconds, internal_api_secret=settings.internal_api_secret)
    app.state.control = control
    app.state.provider = provider
    content_store = ContentStore.from_settings(settings)
    app.state.service = DriverService(control, provider, workers=workers, deadline_seconds=settings.coding_agent_deadline_seconds, content_store=content_store)
    app.state.mcp_server = DriverMCPServer(
        control,
        content_store=content_store,
        run_executor=app.state.service._execute_and_wait_remote,
    )
    app.state.lease: AgentLease | None = None
    app.state.heartbeat_task: asyncio.Task[None] | None = None

    def require_internal(request: Request) -> None:
        configured = settings.internal_api_secret.strip()
        if not configured:
            return
        provided = request.headers.get("x-loom-internal-token", "")
        if not provided:
            authorization = request.headers.get("authorization", "")
            if authorization.lower().startswith("bearer "):
                provided = authorization[7:]
        if provided != configured:
            raise HTTPException(status_code=401, detail="invalid_internal_token")

    async def registration_loop() -> None:
        while True:
            try:
                registration = AgentRegistration(
                    role="driver",
                    agent_id=settings.agent_id,
                    instance_id=instance_id,
                    workspace_id=settings.workspace_id,
                    endpoint_url=settings.driver_url or os.getenv("LOOM_DRIVER_URL", "http://driver:8090"),
                    protocol_version=settings.agent_protocol_version,
                    capabilities={"backend": settings.coding_agent_backend, "model": settings.codex_model, "label": "Codex app-server"},
                )
                app.state.lease = await control.register(registration)
                app.state.service.coordinator.driver_epoch = app.state.lease.epoch
                with suppress(Exception):
                    await control.command("run.recovery.list", {"workspace_id": settings.workspace_id})
                break
            except Exception:
                await asyncio.sleep(max(0.1, settings.agent_registration_retry_seconds))
        while True:
            await asyncio.sleep(max(0.1, app.state.lease.heartbeat_interval_seconds if app.state.lease else settings.agent_heartbeat_interval_seconds))
            try:
                await control.heartbeat()
            except Exception:
                # A later registration obtains a new epoch and fences this
                # instance if another Driver has taken ownership.
                try:
                    app.state.lease = await control.register()
                    app.state.service.coordinator.driver_epoch = app.state.lease.epoch
                except Exception:
                    continue

    @app.on_event("startup")
    async def startup() -> None:
        app.state.heartbeat_task = asyncio.create_task(registration_loop())
        # Give the first registration a scheduling opportunity so TestClient
        # and health probes observe a lease immediately when Observer is up.
        await asyncio.sleep(0)

    @app.on_event("shutdown")
    async def shutdown() -> None:
        task = app.state.heartbeat_task
        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        if app.state.lease is not None:
            with suppress(Exception):
                await control.release()
        with suppress(Exception):
            await app.state.service.coordinator.shutdown()
        with suppress(Exception):
            await provider.force_shutdown()

    @app.get("/healthz")
    async def health() -> dict[str, Any]:
        lease = app.state.lease
        return {"ok": lease is not None, "service": "driver", "agent_id": settings.agent_id, "epoch": lease.epoch if lease else None}

    @app.post("/driver/v1/messages")
    async def message(payload: dict[str, Any], request: Request) -> dict[str, Any]:
        require_internal(request)
        if not isinstance(payload.get("text"), str) or not payload["text"].strip():
            raise HTTPException(status_code=422, detail="text_required")
        if not str(payload.get("request_id") or "").strip():
            raise HTTPException(status_code=422, detail="request_id_required")
        if not str(payload.get("conversation_ref") or "").strip():
            raise HTTPException(status_code=422, detail="conversation_ref_required")
        if not re.fullmatch(r"[0-9a-f]{64}", str(payload.get("payload_digest") or "")):
            raise HTTPException(status_code=422, detail="payload_digest_required")
        workspace_id = str(payload.get("workspace_id") or settings.workspace_id)
        if workspace_id != settings.workspace_id:
            raise HTTPException(status_code=403, detail="workspace_binding_mismatch")
        try:
            return await app.state.service.run_prompt(str(payload["conversation_ref"]), str(payload.get("text", "")), request_id=str(payload["request_id"]), payload_digest=str(payload["payload_digest"]))
        except HTTPException:
            raise
        except Exception as exc:
            reason = getattr(exc, "reason", None)
            detail = reason if isinstance(reason, dict) else str(exc)
            raise HTTPException(status_code=503, detail=detail) from exc

    @app.post("/driver/v1/conversations/{conversation_ref}/interrupt")
    async def interrupt(conversation_ref: str, request: Request, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        require_internal(request)
        payload = payload or {}
        if str(payload.get("workspace_id") or settings.workspace_id) != settings.workspace_id:
            raise HTTPException(status_code=403, detail="workspace_binding_mismatch")
        try:
            return await app.state.service.interrupt(conversation_ref)
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.post("/driver/v1/capability/provision")
    async def provision_capability(payload: dict[str, Any], request: Request) -> dict[str, Any]:
        require_internal(request)
        target_slaves = payload.get("target_slaves") or ([payload.get("target_slave")] if payload.get("target_slave") else [])
        if not target_slaves:
            raise HTTPException(status_code=400, detail="target_slave_required")
        package_ref = payload.get("package_ref") or payload.get("package_version_ref")
        if not package_ref:
            raise HTTPException(status_code=400, detail="package_ref_required")
        try:
            package_ref_value = ResourceRef.model_validate(package_ref) if isinstance(package_ref, dict) else str(package_ref)
            binding_payload = payload.get("compute_binding")
            binding = ComputeBinding.model_validate(binding_payload) if binding_payload else None
            reports = [
                await app.state.service.provision_capability(
                    package_ref_value,
                    str(target),
                    compute_binding=binding,
                    idempotency_key=payload.get("idempotency_key"),
                )
                for target in target_slaves
            ]
        except (ValueError, RuntimeError, KeyError) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"health_reports": reports}

    @app.post("/driver/v1/mcp")
    async def mcp(payload: dict[str, Any], request: Request) -> dict[str, Any]:
        require_internal(request)
        conversation_ref = request.headers.get("x-loom-conversation-ref", "")
        if not conversation_ref:
            raise HTTPException(status_code=400, detail="conversation_ref_required")
        return await app.state.mcp_server.handle(payload, conversation_ref)

    return app


app = create_app()
