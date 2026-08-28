import os

from fastapi import FastAPI, HTTPException

from loom_v2.db.session import make_engine
from loom_v2.settings import Settings
from loom_v2.content_store import ContentStore

from .service import SlaveService
from loom_v2.contracts.types import CapabilityPackageVersion, CapabilityProvisionCommand, ComputeBinding, TaskClosure


def create_app(slave_id: str | None = None) -> FastAPI:
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

    @app.on_event("startup")
    async def initialize_database() -> None:
        await app.state.service.init_db()

    @app.on_event("shutdown")
    async def close_database() -> None:
        if app.state.engine is not None:
            await app.state.engine.dispose()

    @app.get("/healthz")
    async def health() -> dict[str, object]:
        service: SlaveService = app.state.service
        return {"ok": service.available, "service": service.slave_id, "replica": service.replica.state}

    @app.post("/worker/v1/dispatch")
    async def dispatch(payload: dict[str, object]) -> dict[str, object]:
        service: SlaveService = app.state.service
        try:
            attempt_id = str(payload["attempt_id"])
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
            execution_epoch = int(payload.get("execution_epoch", 1))
            result = await service.run(attempt_id, operation, body, closure=closure, binding=binding)
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
            "execution_epoch": execution_epoch,
            "terminal_report": {
                "type": "terminal_report",
                "attempt_id": attempt_id,
                "execution_epoch": execution_epoch,
                "state": "completed",
                "result": {
                    "resource_ref": result.resource_ref.model_dump(mode="json"),
                    "value": result.value,
                    "replay_safety": result.replay_safety,
                    "digest": result.digest,
                },
            },
        }

    @app.post("/worker/v1/provision")
    async def provision(payload: dict[str, object]) -> dict[str, object]:
        service: SlaveService = app.state.service
        try:
            command = CapabilityProvisionCommand.model_validate(payload.get("command") or {})
            package = CapabilityPackageVersion.model_validate(payload.get("package") or {})
            report = await service.provision(command, package)
        except (ValueError, RuntimeError) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"accepted": True, "health_report": report.model_dump(mode="json")}

    @app.get("/worker/v1/capabilities")
    async def capabilities() -> dict[str, object]:
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
