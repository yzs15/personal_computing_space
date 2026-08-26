from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from loom_v2.db.session import make_engine
from loom_v2.settings import Settings
from loom_v2.coding_agents.codex import CodexAppServerProvider
from loom_v2.coding_agents.fake import FakeCodingAgentProvider
from loom_v2.driver.service import DriverService
from loom_v2.slave.service import SlaveService

from .repository import ObserverRepository


def _run_view(record: Any) -> dict[str, Any]:
    return {
        "run_id": record.run_id,
        "task_ref": record.task_ref,
        "goal": record.goal,
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
    }


def create_app(repository: ObserverRepository | None = None) -> FastAPI:
    app = FastAPI(title="Loom v2 Observer")
    settings = Settings()
    app.state.engine = None
    if repository is None and not settings.database_url.startswith("sqlite+aiosqlite:///:memory:"):
        app.state.engine = make_engine(settings.database_url)
    app.state.repo = repository or ObserverRepository(app.state.engine)
    provider = FakeCodingAgentProvider() if settings.coding_agent_backend == "fake" else CodexAppServerProvider(model=settings.codex_model)
    app.state.slaves = {"slave-a": SlaveService("slave-a"), "slave-b": SlaveService("slave-b")}
    app.state.driver = DriverService(app.state.repo, provider, slaves=app.state.slaves)
    static_dir = Path(__file__).resolve().parents[1] / "web" / "static"
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

    @app.on_event("startup")
    async def initialize_database() -> None:
        await app.state.repo.init_db()

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

    @app.post("/api/v1/runs")
    async def open_run(payload: dict[str, Any]) -> dict[str, Any]:
        record = await app.state.repo.open_run(payload.get("run_id"), payload["task_ref"], payload.get("goal", ""), payload.get("allow_reassignment", False))
        return _run_view(record)

    @app.post("/api/v1/messages")
    async def message(payload: dict[str, Any]) -> dict[str, Any]:
        try:
            return await app.state.driver.run_prompt(payload.get("conversation_ref", "conversation-default"), payload.get("text", ""))
        except (RuntimeError, FileNotFoundError) as exc:
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
        return [
            {"slave_id": slave_id, "available": slave.available, "replica": slave.replica.state, "operations": sorted(slave.supported_operations), "term_support": [support.model_dump(mode="json") for support in slave.term_support()]}
            for slave_id, slave in app.state.slaves.items()
        ]

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

    @app.post("/worker/v1/terminal")
    async def terminal(payload: dict[str, Any]) -> dict[str, Any]:
        try:
            return await app.state.repo.terminal(payload)
        except ValueError as exc:
            return JSONResponse(status_code=409, content={"code": str(exc), "retryable": False})

    @app.get("/")
    async def home() -> FileResponse:
        return FileResponse(static_dir / "index.html")

    return app


app = create_app()
