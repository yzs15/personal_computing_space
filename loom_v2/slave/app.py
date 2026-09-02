import asyncio
import os
from contextlib import suppress
from uuid import uuid4
from typing import Any

import httpx

from fastapi import FastAPI, HTTPException, Request

from loom_v2.db.session import make_engine
from loom_v2.settings import Settings
from loom_v2.content_store import ContentStore

from .service import SlaveService
from loom_v2.contracts.types import CapabilityPackageVersion, CapabilityProvisionCommand, ComputeBinding, TaskClosure
from loom_v2.contracts.agents import AgentLease, AgentRegistration


def create_app(slave_id: str | None = None, *, observer_transport: httpx.AsyncBaseTransport | None = None) -> FastAPI:
    slave_id = slave_id or os.getenv("LOOM_SERVICE_NAME", "slave-a")
    app = FastAPI(title=f"Loom v2 {slave_id}")
    settings = Settings()
    app.state.engine = None if settings.database_url.startswith("sqlite+aiosqlite:///:memory:") else make_engine(settings.database_url)
    app.state.service = SlaveService(
        slave_id=slave_id,
        engine=app.state.engine,
        content_store=ContentStore.from_settings(settings),
        capability_operation_timeout_seconds=settings.capability_operation_timeout_seconds,
    )
    app.state.lease: AgentLease | None = None
    app.state.registration_task: asyncio.Task[None] | None = None
    app.state.driver_epochs: dict[str, int] = {}

    def require_internal(request: Request) -> None:
        configured = settings.internal_api_secret.strip()
        if not configured:
            return
        provided = request.headers.get("x-loom-internal-token", "")
        if not provided:
            auth = request.headers.get("authorization", "")
            if auth.lower().startswith("bearer "):
                provided = auth[7:]
        if provided != configured:
            raise HTTPException(status_code=401, detail="invalid_internal_token")

    async def register_loop() -> None:
        instance_id = f"{slave_id}-{uuid4().hex[:12]}"
        endpoint_url = os.getenv("LOOM_SLAVE_ENDPOINT_URL", f"http://{slave_id}:8081" if slave_id == "slave-a" else f"http://{slave_id}:8082")
        registration = AgentRegistration(role="slave", agent_id=slave_id, instance_id=instance_id, workspace_id=app.state.service.workspace_id, endpoint_url=endpoint_url, protocol_version=settings.agent_protocol_version, capabilities={"operations": sorted(app.state.service.supported_operations), "executor_descriptors": [descriptor.kind for descriptor in app.state.service.executor_registry.descriptors()], "term_support": [item.model_dump(mode="json") for item in app.state.service.term_support()]})
        while True:
            try:
                headers = {"X-Loom-Internal-Token": settings.internal_api_secret} if settings.internal_api_secret else {}
                async with httpx.AsyncClient(timeout=5, transport=observer_transport) as client:
                    response = await client.post(f"{settings.observer_url.rstrip('/')}/internal/v1/agents/register", json=registration.model_dump(mode="json"), headers=headers)
                if response.status_code < 400:
                    app.state.lease = AgentLease.model_validate(response.json())
                else:
                    raise RuntimeError("slave_registration_failed")
            except Exception:
                await asyncio.sleep(max(0.1, settings.agent_registration_retry_seconds))
                continue
            while app.state.lease is not None:
                await asyncio.sleep(max(0.1, app.state.lease.heartbeat_interval_seconds))
                try:
                    headers = {"X-Loom-Internal-Token": settings.internal_api_secret} if settings.internal_api_secret else {}
                    payload = {"instance_id": instance_id, "workspace_id": app.state.service.workspace_id, "lease_id": app.state.lease.lease_id, "epoch": app.state.lease.epoch, "driver_epoch": app.state.lease.epoch, "role": "slave"}
                    async with httpx.AsyncClient(timeout=5, transport=observer_transport) as client:
                        response = await client.post(f"{settings.observer_url.rstrip('/')}/internal/v1/agents/{slave_id}/heartbeat", json=payload, headers=headers)
                    if response.status_code >= 400:
                        app.state.lease = None
                        break
                except Exception:
                    continue

    @app.on_event("startup")
    async def initialize_database() -> None:
        await app.state.service.init_db()
        app.state.registration_task = asyncio.create_task(register_loop())

    @app.on_event("shutdown")
    async def close_database() -> None:
        task = app.state.registration_task
        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        lease = app.state.lease
        if lease is not None:
            try:
                headers = {"X-Loom-Internal-Token": settings.internal_api_secret} if settings.internal_api_secret else {}
                payload = {"instance_id": lease.instance_id, "workspace_id": lease.workspace_id, "lease_id": lease.lease_id, "epoch": lease.epoch, "driver_epoch": lease.epoch, "role": "slave"}
                async with httpx.AsyncClient(timeout=5, transport=observer_transport) as client:
                    await client.post(f"{settings.observer_url.rstrip('/')}/internal/v1/agents/{slave_id}/release", json=payload, headers=headers)
            except Exception:
                pass
        if app.state.engine is not None:
            await app.state.engine.dispose()

    @app.get("/healthz")
    async def health() -> dict[str, object]:
        service: SlaveService = app.state.service
        return {"ok": service.available, "service": service.slave_id, "replica": service.replica.state}

    @app.post("/worker/v1/dispatch")
    async def dispatch(payload: dict[str, object], request: Request) -> dict[str, object]:
        require_internal(request)
        service: SlaveService = app.state.service
        try:
            configured_secret = settings.internal_api_secret.strip()
            if configured_secret and (not str(payload.get("driver_id") or "").strip() or payload.get("driver_epoch") is None):
                raise ValueError("driver_identity_required")
            driver_id = str(payload.get("driver_id") or "")
            if configured_secret:
                driver_epoch = int(payload["driver_epoch"])
                previous_epoch = app.state.driver_epochs.get(driver_id)
                if previous_epoch is not None and driver_epoch < previous_epoch:
                    raise ValueError("stale_driver_epoch")
                app.state.driver_epochs[driver_id] = max(driver_epoch, previous_epoch or driver_epoch)
            attempt_id = str(payload["attempt_id"])
            execution_id = str(payload["execution_id"])
            operation = str(payload["operation"])
            body = payload.get("payload", {})
            if not isinstance(body, dict):
                raise ValueError("invalid_dispatch_payload")
            closure_payload = payload.get("closure")
            closure = TaskClosure.model_validate(closure_payload) if closure_payload is not None else None
            binding_payload = payload.get("binding")
            binding = ComputeBinding.model_validate(binding_payload) if binding_payload is not None else None
            workspace_id = str(payload.get("workspace_id", service.workspace_id))
            if workspace_id != service.workspace_id:
                raise ValueError("workspace_binding_mismatch")
            if binding is not None:
                target_ref = binding.target_resource_ref.resource_id.rstrip("/")
                if target_ref != service.slave_id and not target_ref.endswith(f"/{service.slave_id}") and not target_ref.endswith(f":{service.slave_id}"):
                    raise ValueError("target_slave_mismatch")
            execution_epoch = int(payload.get("execution_epoch", 1))
            result = await service.run(
                attempt_id,
                operation,
                body,
                closure=closure,
                binding=binding,
                execution_epoch=execution_epoch,
            )
        except KeyError as exc:
            raise HTTPException(status_code=400, detail=f"missing_field:{exc.args[0]}") from exc
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {
            "type": "dispatch_ack",
            "accepted": True,
            "attempt_id": attempt_id,
            "execution_id": execution_id,
            "execution_epoch": execution_epoch,
            "terminal_report": {
                "type": "terminal_report",
                "attempt_id": attempt_id,
                "execution_id": execution_id,
                "execution_epoch": execution_epoch,
                "state": result.terminal_state,
                "error": result.terminal_error,
                "validation_evidence": result.validation_evidence,
                "result": {
                    "resource_ref": result.resource_ref.model_dump(mode="json"),
                    "value": result.value,
                    "replay_safety": result.replay_safety,
                    "digest": result.digest,
                },
            },
        }

    @app.post("/worker/v1/provision")
    async def provision(payload: dict[str, object], request: Request) -> dict[str, object]:
        require_internal(request)
        service: SlaveService = app.state.service
        try:
            configured_secret = settings.internal_api_secret.strip()
            if configured_secret and (not str(payload.get("driver_id") or "").strip() or payload.get("driver_epoch") is None):
                raise ValueError("driver_identity_required")
            if configured_secret:
                driver_id = str(payload["driver_id"])
                driver_epoch = int(payload["driver_epoch"])
                previous_epoch = app.state.driver_epochs.get(driver_id)
                if previous_epoch is not None and driver_epoch < previous_epoch:
                    raise ValueError("stale_driver_epoch")
                app.state.driver_epochs[driver_id] = max(driver_epoch, previous_epoch or driver_epoch)
            command = CapabilityProvisionCommand.model_validate(payload.get("command") or {})
            package = CapabilityPackageVersion.model_validate(payload.get("package") or {})
            if command.workspace_id != service.workspace_id:
                raise ValueError("workspace_binding_mismatch")
            if command.target_slave != service.slave_id:
                raise ValueError("target_slave_mismatch")
            report = await service.provision(command, package)
        except (ValueError, RuntimeError) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"accepted": True, "health_report": report.model_dump(mode="json")}

    @app.get("/worker/v1/capabilities")
    async def capabilities(request: Request) -> dict[str, object]:
        require_internal(request)
        service: SlaveService = app.state.service
        return {
            "slave_id": service.slave_id,
            "workspace_id": service.workspace_id,
            "available": service.available,
            "replica": service.replica.state,
            "operations": sorted(service.supported_operations),
            "executor_descriptors": [descriptor.__dict__ | {"operations": sorted(descriptor.operations), "digest": descriptor.digest, "descriptor_ref": descriptor.descriptor_ref} for descriptor in service.executor_registry.descriptors()],
            "activations": [activation.model_dump(mode="json") for activation in service.activations.values()],
            "resource_events": [event.model_dump(mode="json") for event in service.resource_events],
            "term_support": [support.model_dump(mode="json") for support in service.term_support()],
        }

    return app
