import asyncio
from contextlib import asynccontextmanager, suppress
from uuid import uuid4
from typing import Any

import httpx

from fastapi import Depends, FastAPI, HTTPException

from loom_v2.db.session import make_engine
from loom_v2.settings import Settings
from loom_v2.content_store import ContentStore
from loom_v2.contracts.api import SlaveDeprovisionRequest, SlaveDispatchRequest, SlaveProvisionRequest

from .service import SlaveService
from loom_v2.contracts.agents import AgentLease, AgentRegistration
from loom_v2.auth import InternalAuth
from loom_v2.internal_http import InternalHttpClient
from loom_v2.contracts.package_contracts import load_operator_package_contracts


def create_app(
    slave_id: str | None = None,
    *,
    settings: Settings | None = None,
    observer_transport: httpx.AsyncBaseTransport | None = None,
) -> FastAPI:
    settings = settings or Settings()
    if slave_id is None:
        slave_id = settings.service_name if settings.service_name.startswith("slave") else "slave-a"

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        await app.state.service.init_db()
        app.state.registration_task = asyncio.create_task(register_loop())
        app.state.capability_health_task = asyncio.create_task(capability_health_loop())
        try:
            yield
        finally:
            for task in (app.state.capability_health_task, app.state.registration_task):
                if task is not None:
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
            lease = app.state.lease
            if lease is not None:
                try:
                    payload = {"instance_id": lease.instance_id, "workspace_id": lease.workspace_id, "lease_id": lease.lease_id, "epoch": lease.epoch, "driver_epoch": lease.epoch, "role": "slave"}
                    await observer_request(f"/internal/v1/agents/{slave_id}/release", payload)
                except Exception:
                    pass
            if app.state.engine is not None:
                await app.state.engine.dispose()
            await app.state.service.close()
            await app.state.observer_http.close()

    app = FastAPI(title=f"Loom v2 {slave_id}", lifespan=lifespan)
    load_operator_package_contracts(settings.package_contract_dir)
    app.state.engine = None if settings.database_url.startswith("sqlite+aiosqlite:///:memory:") else make_engine(settings.database_url)
    app.state.service = SlaveService(
        slave_id=slave_id,
        engine=app.state.engine,
        content_store=ContentStore.from_settings(settings),
        capability_operation_timeout_seconds=settings.capability_operation_timeout_seconds,
        settings=settings,
    )
    app.state.lease = None
    app.state.registration_task = None
    app.state.capability_health_task = None
    app.state.driver_epochs = {}
    app.state.observer_http = InternalHttpClient(timeout=5, transport=observer_transport)

    require_internal = InternalAuth(settings.internal_api_secret)

    def observer_headers() -> dict[str, str]:
        if not settings.internal_api_secret:
            return {}
        return {"X-Loom-Internal-Token": settings.internal_api_secret}

    async def observer_request(path: str, payload: dict[str, Any]) -> httpx.Response:
        app.state.observer_http.transport = observer_transport
        return await app.state.observer_http.request(
            "POST",
            f"{settings.observer_url.rstrip('/')}{path}",
            json=payload,
            headers=observer_headers(),
        )

    def record_driver_epoch(driver_id: str | None, driver_epoch: int | None) -> None:
        if not settings.internal_api_secret.strip():
            return
        if driver_id is None or not driver_id.strip() or driver_epoch is None:
            raise ValueError("driver_identity_required")
        previous_epoch = app.state.driver_epochs.get(driver_id)
        if previous_epoch is not None and driver_epoch < previous_epoch:
            raise ValueError("stale_driver_epoch")
        app.state.driver_epochs[driver_id] = max(
            driver_epoch,
            previous_epoch or driver_epoch,
        )

    async def register_loop() -> None:
        instance_id = f"{slave_id}-{uuid4().hex[:12]}"
        endpoint_url = settings.slave_endpoint_url or (f"http://{slave_id}:8081" if slave_id == "slave-a" else f"http://{slave_id}:8082")
        runtime_descriptors = [descriptor.__dict__ | {"supports": [support.to_mapping() for support in descriptor.supports]} for descriptor in app.state.service.runtime_plugin_host.descriptors()] if app.state.service.runtime_plugin_host is not None else []
        executor_descriptors = [
            {
                "package_type": descriptor.package_type,
                "kind": descriptor.kind,
                "version": descriptor.version,
                "operations": sorted(descriptor.operations),
                "descriptor_ref": descriptor.descriptor_ref,
                "digest": descriptor.digest,
            }
            for descriptor in app.state.service.executor_registry.descriptors()
        ]
        while True:
            try:
                registration = AgentRegistration(role="slave", agent_id=slave_id, instance_id=instance_id, workspace_id=app.state.service.workspace_id, endpoint_url=endpoint_url, protocol_version=settings.agent_protocol_version, capabilities={"operations": sorted(app.state.service.capability_snapshot()), "base_operations": sorted(app.state.service.supported_operations), "executor_descriptors": executor_descriptors, "runtime_plugin_descriptors": runtime_descriptors, "runtime_plugins": runtime_descriptors, "term_support": [item.model_dump(mode="json") for item in app.state.service.term_support()]})
                response = await observer_request(
                    "/internal/v1/agents/register",
                    registration.model_dump(mode="json"),
                )
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
                    payload = {"instance_id": instance_id, "workspace_id": app.state.service.workspace_id, "lease_id": app.state.lease.lease_id, "epoch": app.state.lease.epoch, "driver_epoch": app.state.lease.epoch, "role": "slave"}
                    response = await observer_request(
                        f"/internal/v1/agents/{slave_id}/heartbeat",
                        payload,
                    )
                    if response.status_code >= 400:
                        app.state.lease = None
                        break
                except Exception:
                    continue

    async def capability_health_loop() -> None:
        interval = max(0.1, settings.capability_health_interval_seconds)
        while True:
            lease = app.state.lease
            if lease is None:
                await asyncio.sleep(interval)
                continue
            try:
                reports = await app.state.service.inspect_activation_health()
                for report in reports:
                    current_lease = app.state.lease
                    if current_lease is None:
                        break
                    payload = {
                        "agent_id": slave_id,
                        "instance_id": current_lease.instance_id,
                        "lease_id": current_lease.lease_id,
                        "epoch": current_lease.epoch,
                        "workspace_id": current_lease.workspace_id,
                        "report": report.model_dump(mode="json"),
                    }
                    response = await observer_request(
                        "/internal/v1/capability-health",
                        payload,
                    )
                    if response.status_code < 400:
                        app.state.service.acknowledge_health_report(report.report_id)
            except Exception:
                pass
            await asyncio.sleep(interval)

    @app.get("/healthz")
    async def health() -> dict[str, object]:
        service: SlaveService = app.state.service
        return {"ok": service.available, "service": service.slave_id, "replica": service.replica.state}

    @app.post("/worker/v1/dispatch", dependencies=[Depends(require_internal)])
    async def dispatch(payload: SlaveDispatchRequest) -> dict[str, object]:
        service: SlaveService = app.state.service
        try:
            record_driver_epoch(payload.driver_id, payload.driver_epoch)
            attempt_id = payload.attempt_id
            execution_id = payload.execution_id
            operation = payload.operation
            body = payload.payload
            closure = payload.closure
            binding = payload.binding
            workspace_id = payload.workspace_id or service.workspace_id
            if workspace_id != service.workspace_id:
                raise ValueError("workspace_binding_mismatch")
            if binding is not None:
                target_ref = binding.target_resource_ref.resource_id.rstrip("/")
                if target_ref != service.slave_id and not target_ref.endswith(f"/{service.slave_id}") and not target_ref.endswith(f":{service.slave_id}"):
                    raise ValueError("target_slave_mismatch")
            execution_epoch = payload.execution_epoch
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
                "provenance": result.provenance,
                "result": {
                    "resource_ref": result.resource_ref.model_dump(mode="json"),
                    "value": result.value,
                    "replay_safety": result.replay_safety,
                    "provenance": result.provenance,
                },
            },
        }

    @app.post("/worker/v1/provision", dependencies=[Depends(require_internal)])
    async def provision(payload: SlaveProvisionRequest) -> dict[str, object]:
        service: SlaveService = app.state.service
        try:
            record_driver_epoch(payload.driver_id, payload.driver_epoch)
            command = payload.command
            package = payload.package
            if command.workspace_id != service.workspace_id:
                raise ValueError("workspace_binding_mismatch")
            if command.target_slave != service.slave_id:
                raise ValueError("target_slave_mismatch")
            report = await service.provision(command, package)
        except (ValueError, RuntimeError) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"accepted": True, "health_report": report.model_dump(mode="json")}

    @app.post("/worker/v1/deprovision", dependencies=[Depends(require_internal)])
    async def deprovision(payload: SlaveDeprovisionRequest) -> dict[str, object]:
        service: SlaveService = app.state.service
        try:
            record_driver_epoch(payload.driver_id, payload.driver_epoch)
            command = payload.command
            package = payload.package
            if command.workspace_id != service.workspace_id:
                raise ValueError("workspace_binding_mismatch")
            if command.target_slave != service.slave_id:
                raise ValueError("target_slave_mismatch")
            report = await service.deprovision(command, package)
        except (ValueError, RuntimeError) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"accepted": True, "health_report": report.model_dump(mode="json")}

    @app.get("/worker/v1/capabilities", dependencies=[Depends(require_internal)])
    async def capabilities() -> dict[str, object]:
        service: SlaveService = app.state.service
        return {
            "slave_id": service.slave_id,
            "workspace_id": service.workspace_id,
            "available": service.available,
            "replica": service.replica.state,
            "operations": sorted(service.capability_snapshot()),
            "base_operations": sorted(service.supported_operations),
            "executor_descriptors": [descriptor.__dict__ | {"operations": sorted(descriptor.operations), "digest": descriptor.digest, "descriptor_ref": descriptor.descriptor_ref} for descriptor in service.executor_registry.descriptors()],
            "runtime_plugin_descriptors": [descriptor.__dict__ | {"supports": [support.to_mapping() for support in descriptor.supports]} for descriptor in service.runtime_plugin_host.descriptors()] if service.runtime_plugin_host is not None else [],
            "runtime_plugins": [descriptor.__dict__ | {"supports": [support.to_mapping() for support in descriptor.supports]} for descriptor in service.runtime_plugin_host.descriptors()] if service.runtime_plugin_host is not None else [],
            "activations": [activation.model_dump(mode="json") for activation in service.activations.values()],
            "resource_events": [event.model_dump(mode="json") for event in service.resource_events],
            "term_support": [support.model_dump(mode="json") for support in service.term_support()],
        }

    return app
