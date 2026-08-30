from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles

from loom_v2.db.session import make_engine
from loom_v2.settings import Settings
from loom_v2.coding_agents.codex import CodexAppServerProvider
from loom_v2.coding_agents.fake import FakeCodingAgentProvider
from loom_v2.coding_agents.base import CodingAgentError
from loom_v2.driver.service import DriverService
from loom_v2.slave.service import SlaveService
from loom_v2.contracts.types import ClosureContract
from loom_v2.observer.worker import WorkerSession
from loom_v2.driver.mcp_server import DriverMCPServer
from loom_v2.content_store import ContentStore

from .repository import ObserverRepository


def _run_view(record: Any) -> dict[str, Any]:
    return {
        "run_id": record.run_id,
        "task_ref": record.task_ref,
        "goal": record.goal,
        "closure_contract": record.closure_contract.model_dump(mode="json") if record.closure_contract else None,
        "allow_reassignment": record.allow_reassignment,
        "state": record.state,
        "status": ObserverRepository._conversation_status_for_state(record.state),
        "draft_version": record.draft.version_id,
        "draft_digest": record.draft.snapshot_digest,
        "committed_version": record.committed.version_id if record.committed else None,
        "execution_id": record.execution_id,
        "execution_epoch": record.execution_epoch,
        "outcome": record.outcome,
        "attempts": record.attempts,
        "events": record.events,
        "capability_packages": [package.model_dump(mode="json") for package in record.capability_packages],
        "capability_activations": [activation.model_dump(mode="json") for activation in record.capability_activations],
    }


def create_app(repository: ObserverRepository | None = None) -> FastAPI:
    app = FastAPI(title="Loom v2 Observer")
    settings = Settings()
    app.state.engine = None
    if repository is None and not settings.database_url.startswith("sqlite+aiosqlite:///:memory:"):
        app.state.engine = make_engine(settings.database_url)
    content_store = repository.content_store if repository is not None else ContentStore.from_settings(settings)
    app.state.repo = repository or ObserverRepository(app.state.engine, content_store=content_store)
    provider = (
        FakeCodingAgentProvider()
        if settings.coding_agent_backend == "fake"
        else CodexAppServerProvider(
            model=settings.codex_model,
            poll_interval_seconds=settings.coding_agent_poll_interval_seconds,
            protocol_failure_seconds=settings.coding_agent_protocol_failure_seconds,
        )
    )
    app.state.slaves = {
        "slave-a": SlaveService("slave-a", content_store=ContentStore.from_settings(settings), capability_operation_timeout_seconds=settings.capability_operation_timeout_seconds),
        "slave-b": SlaveService("slave-b", content_store=ContentStore.from_settings(settings), capability_operation_timeout_seconds=settings.capability_operation_timeout_seconds),
    }
    app.state.workers = {}
    if settings.slave_a_url:
        app.state.workers["slave-a"] = WorkerSession("slave-a", settings.slave_a_url, operation_timeout=settings.worker_operation_timeout_seconds)
    if settings.slave_b_url:
        app.state.workers["slave-b"] = WorkerSession("slave-b", settings.slave_b_url, operation_timeout=settings.worker_operation_timeout_seconds)
    app.state.driver = DriverService(
        app.state.repo,
        provider,
        slaves=app.state.slaves,
        workers=app.state.workers,
        deadline_seconds=settings.coding_agent_deadline_seconds,
    )
    app.state.mcp_server = DriverMCPServer(app.state.repo)
    static_dir = Path(__file__).resolve().parents[1] / "web" / "static"
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

    @app.on_event("startup")
    async def initialize_database() -> None:
        await app.state.repo.init_db()
        # Driver ownership is in-memory.  Any active-looking Run persisted by
        # a previous Observer process has lost its owner and must be fenced so
        # it cannot remain visible as a live conversation forever.
        await app.state.repo.recover_stale_runs()

    @app.on_event("shutdown")
    async def close_database() -> None:
        if app.state.engine is not None:
            await app.state.engine.dispose()

    @app.get("/healthz")
    async def health() -> dict[str, object]:
        return {"ok": True, "service": "observer"}

    @app.get("/api/v1/runtime")
    async def runtime() -> dict[str, str | None]:
        backend = settings.coding_agent_backend.strip().lower()
        if backend == "codex":
            label = "Codex app-server"
            model = settings.codex_model
        elif backend == "fake":
            label = "Fake coding agent"
            model = None
        else:
            label = backend or "Unknown coding agent"
            model = settings.codex_model or None
        return {
            "coding_agent_backend": backend,
            "coding_agent_label": label,
            "model": model,
        }

    @app.get("/api/v1/content/{digest}")
    async def content(digest: str) -> Response:
        try:
            body = await app.state.repo.content_store.get(digest)
        except (FileNotFoundError, ValueError) as exc:
            raise HTTPException(status_code=404, detail="content_not_found") from exc
        return Response(content=body, media_type="application/octet-stream", headers={"X-Content-Digest": digest})

    @app.post("/api/v1/content")
    async def put_content(payload: dict[str, Any]) -> dict[str, Any]:
        if "content" not in payload:
            raise HTTPException(status_code=400, detail="content_required")
        try:
            ref = await app.state.repo.put_content(payload["content"], media_type=str(payload.get("media_type") or ""))
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return ref.model_dump(mode="json")

    @app.post("/mcp")
    async def mcp(payload: dict[str, Any], request: Request) -> dict[str, Any]:
        conversation_ref = request.headers.get("x-loom-conversation-ref", "")
        return await app.state.mcp_server.handle(payload, conversation_ref)

    @app.post("/api/v1/runs")
    async def open_run(payload: dict[str, Any]) -> dict[str, Any]:
        contract_payload = payload.get("closure_contract")
        contract = ClosureContract.model_validate(contract_payload) if contract_payload is not None else None
        goal = contract.goal if contract is not None else payload.get("goal", "")
        record = await app.state.repo.open_run(
            payload.get("run_id"),
            payload["task_ref"],
            goal,
            payload.get("allow_reassignment", False),
            contract,
            user_id=payload.get("user_id", "user-default"),
            workspace_id=payload.get("workspace_id", settings.workspace_id),
        )
        return _run_view(record)

    @app.post("/api/v1/messages")
    async def message(payload: dict[str, Any]) -> dict[str, Any]:
        try:
            return await app.state.driver.run_prompt(payload.get("conversation_ref", "conversation-default"), payload.get("text", ""))
        except (RuntimeError, FileNotFoundError) as exc:
            if isinstance(exc, CodingAgentError):
                reason = dict(exc.reason)
                code = str(reason.get("code") or "coding_agent_error")
                retryable = code not in {"coding_agent_blocked", "coding_agent_usage_limited", "coding_agent_budget_limited"}
                return JSONResponse(status_code=503, content={"code": code, "retryable": retryable, "details": reason})
            return JSONResponse(status_code=503, content={"code": str(exc), "retryable": True})

    @app.get("/api/v1/conversations")
    async def conversations() -> list[dict[str, Any]]:
        return await app.state.repo.list_conversations()

    @app.get("/api/v1/conversations/{conversation_ref}")
    async def conversation(conversation_ref: str) -> dict[str, Any]:
        try:
            return await app.state.repo.get_conversation(conversation_ref)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="conversation_not_found") from exc

    @app.post("/api/v1/conversations/{conversation_ref}/interrupt")
    async def interrupt_conversation(conversation_ref: str) -> dict[str, Any]:
        try:
            return await app.state.driver.interrupt(conversation_ref)
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.get("/api/v1/conversations/{conversation_ref}/stream")
    async def stream(conversation_ref: str) -> StreamingResponse:
        async def events() -> Any:
            try:
                conversation_view = await app.state.repo.get_conversation(conversation_ref)
            except KeyError:
                return
            for event in conversation_view["events"]:
                yield f"data: {json.dumps(event, sort_keys=True, ensure_ascii=False)}\n\n"

        return StreamingResponse(events(), media_type="text/event-stream")

    @app.post("/api/v1/runs/{run_id}/patches")
    async def apply_patch(run_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            receipt = await app.state.repo.apply_patch(
                run_id,
                payload["base_draft_version"],
                payload["base_snapshot_digest"],
                payload["operation_id"],
                payload.get("ops", []),
            )
        except KeyError as exc:
            raise HTTPException(status_code=400, detail=f"missing_field:{exc.args[0]}") from exc
        except ValueError as exc:
            code = str(exc)
            raise HTTPException(status_code=409 if code == "version_conflict" else 422, detail=code) from exc
        return {
            "receipt": receipt.receipt,
            "run_id": receipt.run_id,
            "kind": receipt.kind,
            "draft_version": receipt.draft_version,
            "draft_digest": receipt.draft_digest,
            "patch_cursor": receipt.patch_cursor,
            "readiness": receipt.readiness,
        }

    @app.get("/api/v1/runs/{run_id}/readiness")
    async def readiness(run_id: str) -> dict[str, Any]:
        try:
            return await app.state.repo.inspect_readiness(run_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="run_not_found") from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.post("/api/v1/runs/{run_id}/commit")
    async def commit(run_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            version = await app.state.repo.commit(run_id, payload["draft_version"], payload["draft_digest"])
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"closure_version": version.version_id, "snapshot_digest": version.snapshot_digest, "state": "committed"}

    @app.post("/api/v1/runs/{run_id}/start")
    async def start(run_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            return await app.state.repo.start(run_id, payload["closure_version"])
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.post("/api/v1/runs/{run_id}/close")
    async def close_run(run_id: str) -> dict[str, Any]:
        try:
            return _run_view(await app.state.repo.close_run(run_id))
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.get("/api/v1/runs/{run_id}")
    async def get_run(run_id: str) -> dict[str, Any]:
        try:
            return _run_view(await app.state.repo.get_run(run_id))
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="run_not_found") from exc

    @app.get("/api/v1/capabilities")
    async def capabilities() -> list[dict[str, Any]]:
        records = await app.state.repo._all_records()
        return [
            {
                "slave_id": slave_id,
                "available": slave.available,
                "replica": slave.replica.state,
                "operations": sorted(set(slave.supported_operations) | set(app.state.repo.slave_capabilities.get(slave_id, {}).get("operations", set()))),
                "term_support": [support.model_dump(mode="json") for support in slave.term_support()],
                "activations": [activation.model_dump(mode="json") for activation in slave.activations.values()] + [activation.model_dump(mode="json") for record in records for activation in record.capability_activations if activation.target_slave == slave_id and activation.activation_state == "ready"],
                "executor_descriptors": [{"kind": descriptor.kind, "version": descriptor.version, "operations": sorted(descriptor.operations), "descriptor_ref": descriptor.descriptor_ref, "digest": descriptor.digest} for descriptor in slave.executor_registry.descriptors()],
            }
            for slave_id, slave in app.state.slaves.items()
        ]

    @app.get("/api/v1/capability-packages")
    async def capability_packages(run_id: str | None = None, include_abandoned: bool = False) -> list[dict[str, Any]]:
        packages = await app.state.repo.list_capability_packages(run_id=run_id, include_abandoned=include_abandoned)
        result = []
        records = await app.state.repo._all_records()
        for package in packages:
            item = package.model_dump(mode="json")
            refs = {f"{package.package_id}:{package.package_version}", package.version_ref}
            item["activations"] = [activation.model_dump(mode="json") for record in records for activation in record.capability_activations if activation.package_version_ref in refs]
            result.append(item)
        return result

    @app.get("/api/v1/runs/{run_id}/capability-packages")
    async def run_capability_packages(run_id: str) -> list[dict[str, Any]]:
        try:
            packages = await app.state.repo.list_capability_packages(run_id=run_id, include_abandoned=True)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="run_not_found") from exc
        record = await app.state.repo.get_run(run_id)
        return [{**package.model_dump(mode="json"), "activations": [activation.model_dump(mode="json") for activation in record.capability_activations if activation.package_version_ref in {f"{package.package_id}:{package.package_version}", package.version_ref}]} for package in packages]

    @app.post("/api/v1/capability-packages/{package_ref:path}/promote")
    async def promote_capability_package(package_ref: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = payload or {}
        try:
            package = await app.state.repo.promote_capability_package(package_ref, idempotency_key=payload.get("idempotency_key"), approved_digest=payload.get("approved_digest"))
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="capability_package_not_found") from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        targets = payload.get("target_slaves") or []
        reports = []
        for target in targets:
            worker = app.state.workers.get(target)
            slave = app.state.slaves.get(target)
            command = {
                "command_id": f"provision-{package.package_id}-{target}",
                "package_version_ref": f"{package.package_id}:{package.package_version}",
                "package_digest": package.package_digest,
                "target_slave": target,
                "workspace_id": settings.workspace_id,
                "activation_closure_version_ref": package.package_closure_version_ref,
                "program_content_ref": package.program_content_ref.model_dump(mode="json"),
                "compute_binding": payload.get("compute_binding"),
                "idempotency_key": payload.get("idempotency_key") or f"promote-{package.package_digest}-{target}",
            }
            from loom_v2.contracts.types import CapabilityProvisionCommand
            provision_command = CapabilityProvisionCommand.model_validate(command)
            try:
                if worker is not None:
                    report = await worker.provision(command=provision_command, package=package)
                elif slave is not None:
                    report = await slave.provision(provision_command, package)
                else:
                    raise RuntimeError("slave_not_found")
                await app.state.repo.record_capability_health(report)
                reports.append(report.model_dump(mode="json"))
            except (RuntimeError, ValueError) as exc:
                reports.append({"target_slave": target, "activation_state": "failed", "error": str(exc)})
        return {"package": package.model_dump(mode="json"), "health_reports": reports}

    @app.post("/api/v1/capability-packages/{package_ref:path}/abandon")
    async def abandon_capability_package(package_ref: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        try:
            package = await app.state.repo.abandon_capability_package(package_ref, idempotency_key=(payload or {}).get("idempotency_key"))
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="capability_package_not_found") from exc
        return package.model_dump(mode="json")

    @app.post("/api/v1/slaves/{slave_id}/availability")
    async def set_slave_availability(slave_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        available = bool(payload.get("available", True))
        await app.state.repo.set_slave_availability(slave_id, available)
        if slave_id in app.state.slaves:
            app.state.slaves[slave_id].available = available
        return {"slave_id": slave_id, "available": available}

    @app.post("/api/v1/runs/{run_id}/reconcile")
    async def reconcile(run_id: str) -> dict[str, Any]:
        return _run_view(await app.state.repo.reconcile(run_id))

    @app.get("/")
    async def home() -> FileResponse:
        return FileResponse(static_dir / "index.html")

    return app


app = create_app()
