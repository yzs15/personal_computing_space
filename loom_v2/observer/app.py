from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles

from loom_v2.db.session import make_engine
from loom_v2.settings import Settings
from loom_v2.contracts.types import ClosureContract
from loom_v2.content_store import ContentStore
from loom_v2.contracts.agents import AgentRegistration, DriverCommand
from loom_v2.contracts.errors import DomainError
from loom_v2.observer.gateway import ObserverDriverGateway
from loom_v2.observer.dispatcher import ObserverMessageDispatcher

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
        "dynamic_nodes": [node.model_dump(mode="json") for node in record.dynamic_nodes],
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
    app.state.gateway = ObserverDriverGateway(
        app.state.repo,
        workspace_id=settings.workspace_id,
        internal_api_secret=settings.internal_api_secret,
        timeout=settings.worker_operation_timeout_seconds,
    )
    app.state.dispatcher = ObserverMessageDispatcher(
        app.state.repo,
        app.state.gateway,
        workspace_id=settings.workspace_id,
        forward_timeout=settings.observer_forward_timeout_seconds,
    )
    # The production Compose profile injects the internal secret and runs a
    # standalone Driver. Keep the in-process objects only for the hermetic
    # in-memory test profile (or an explicitly injected repository); a
    # PostgreSQL Observer without its internal secret never starts a Driver.
    legacy_embedded = (
        not settings.internal_api_secret.strip()
        and (repository is not None or settings.database_url.startswith("sqlite+aiosqlite:///:memory:"))
    )
    if not legacy_embedded:
        # Observer never executes Docker orchestration itself; readiness and
        # sandbox checks belong to the Driver that owns the Docker socket.
        app.state.repo.orchestrator_runtime_available = False
    elif repository is None:
        app.state.repo.orchestrator_runtime_available = True
    if not legacy_embedded:
        app.state.repo.slave_capabilities.clear()
        app.state.repo.slave_agents.clear()
        app.state.repo.slave_instances.clear()
        app.state.repo.agents = {
            key: value
            for key, value in app.state.repo.agents.items()
            if not str(value.get("instance_id") or "").startswith("embedded-")
        }
    if legacy_embedded:
        from loom_v2.coding_agents.codex import CodexAppServerProvider
        from loom_v2.coding_agents.fake import FakeCodingAgentProvider
        from loom_v2.driver.mcp_server import DriverMCPServer
        from loom_v2.driver.service import DriverService
        from loom_v2.driver.worker import WorkerSession
        from loom_v2.slave.service import SlaveService

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
        app.state.forward_tasks: dict[str, asyncio.Task[Any]] = {}
    else:
        app.state.provider = None
        app.state.slaves = {}
        app.state.workers = {}
        app.state.driver = None
    app.state.mcp_server = (
        DriverMCPServer(app.state.repo, run_executor=app.state.driver._execute_and_wait_local)
        if legacy_embedded
        else None
    )
    static_dir = Path(__file__).resolve().parents[1] / "web" / "static"
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

    @app.on_event("startup")
    async def initialize_database() -> None:
        await app.state.repo.init_db()
        await app.state.dispatcher.start()
        # Run ownership belongs to the registered Driver lease.  Do not mark
        # persisted active Runs failed merely because Observer restarted; the
        # next Driver instance will inspect and recover them using thread ids.

    @app.on_event("shutdown")
    async def close_database() -> None:
        await app.state.dispatcher.stop()
        if app.state.engine is not None:
            await app.state.engine.dispose()

    @app.get("/healthz")
    async def health() -> dict[str, object]:
        return {"ok": True, "service": "observer"}

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

    @app.post("/internal/v1/agents/register")
    async def register_agent(payload: dict[str, Any], request: Request) -> dict[str, Any]:
        require_internal(request)
        try:
            lease = await app.state.repo.register_agent(AgentRegistration.model_validate(payload))
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return lease.model_dump(mode="json")

    @app.post("/internal/v1/agents/{agent_id}/heartbeat")
    async def heartbeat_agent(agent_id: str, payload: dict[str, Any], request: Request) -> dict[str, Any]:
        require_internal(request)
        try:
            epoch_value = payload.get("driver_epoch", payload.get("epoch"))
            if epoch_value is None:
                raise KeyError("epoch")
            if payload.get("driver_epoch") is not None and payload.get("epoch") is not None and int(payload["driver_epoch"]) != int(payload["epoch"]):
                raise ValueError("driver_epoch_mismatch")
            lease = await app.state.repo.heartbeat_agent(
                agent_id,
                str(payload["instance_id"]),
                str(payload["lease_id"]),
                int(epoch_value),
                workspace_id=str(payload.get("workspace_id") or settings.workspace_id),
                role=str(payload.get("role", "driver")),
            )
        except KeyError as exc:
            raise HTTPException(status_code=400, detail=f"missing_field:{exc.args[0]}") from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return lease.model_dump(mode="json")

    @app.post("/internal/v1/agents/{agent_id}/release")
    async def release_agent(agent_id: str, payload: dict[str, Any], request: Request) -> dict[str, Any]:
        require_internal(request)
        try:
            epoch_value = payload.get("driver_epoch", payload.get("epoch"))
            if epoch_value is None:
                raise KeyError("epoch")
            if payload.get("driver_epoch") is not None and payload.get("epoch") is not None and int(payload["driver_epoch"]) != int(payload["epoch"]):
                raise ValueError("driver_epoch_mismatch")
            await app.state.repo.release_agent(
                agent_id,
                str(payload["instance_id"]),
                str(payload["lease_id"]),
                int(epoch_value),
                workspace_id=str(payload.get("workspace_id") or settings.workspace_id),
                role=str(payload.get("role", "driver")),
            )
        except KeyError as exc:
            raise HTTPException(status_code=400, detail=f"missing_field:{exc.args[0]}") from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"released": True}

    @app.get("/internal/v1/agents/slaves")
    async def list_slaves(request: Request, workspace_id: str | None = None) -> list[dict[str, Any]]:
        require_internal(request)
        return await app.state.repo.list_agents(workspace_id or settings.workspace_id, role="slave")

    @app.post("/internal/v1/driver/commands")
    async def driver_command(payload: dict[str, Any], request: Request) -> dict[str, Any]:
        require_internal(request)
        try:
            result = await app.state.repo.execute_driver_command(DriverCommand.model_validate(payload))
        except DomainError as exc:
            envelope = exc.envelope.model_dump(mode="json")
            raise HTTPException(status_code=409, detail=envelope) from exc
        except ValueError as exc:
            code = str(exc)
            status = 400 if code == "driver_command_not_allowed" else 409
            raise HTTPException(status_code=status, detail=code) from exc
        except KeyError as exc:
            raise HTTPException(status_code=400, detail=f"missing_field:{exc.args[0]}") from exc
        return result

    @app.post("/driver-gateway/v1/registration-check")
    async def registration_check(request: Request) -> dict[str, Any]:
        require_internal(request)
        driver = await app.state.gateway.active_driver()
        return {"registered": driver is not None, "driver": driver}

    @app.get("/api/v1/runtime")
    async def runtime() -> dict[str, str | None]:
        active_driver = await app.state.gateway.active_driver()
        if active_driver is not None:
            capabilities = active_driver.get("capabilities") or {}
            return {
                "coding_agent_backend": str(capabilities.get("backend") or "codex"),
                "coding_agent_label": str(capabilities.get("label") or "Codex app-server"),
                "model": capabilities.get("model") or settings.codex_model,
                "driver_id": active_driver.get("agent_id"),
                "driver_epoch": str(active_driver.get("epoch")),
            }
        if not legacy_embedded:
            return JSONResponse(status_code=503, content={"code": "driver_unavailable", "retryable": True})
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
        if not legacy_embedded:
            try:
                return await app.state.gateway.forward(
                    "/driver/v1/mcp",
                    payload,
                    extra_headers={"X-Loom-Conversation-Ref": conversation_ref},
                )
            except RuntimeError as exc:
                raise HTTPException(status_code=503, detail=str(exc)) from exc
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
        if not isinstance(payload.get("text"), str) or not payload["text"].strip():
            raise HTTPException(status_code=422, detail="text_required")
        if not legacy_embedded and not str(payload.get("request_id") or "").strip():
            raise HTTPException(status_code=422, detail="request_id_required")
        if not legacy_embedded and not str(payload.get("conversation_ref") or "").strip():
            raise HTTPException(status_code=422, detail="conversation_ref_required")
        if not legacy_embedded and str(payload.get("workspace_id") or settings.workspace_id) != settings.workspace_id:
            raise HTTPException(status_code=403, detail="workspace_binding_mismatch")
        conversation_ref = str(payload.get("conversation_ref", "conversation-default"))
        supplied_request_id = bool(payload.get("request_id"))
        request_id = str(payload.get("request_id") or f"request-{uuid4().hex}")
        if not legacy_embedded:
            try:
                existing = await app.state.repo.get_message_receipt(settings.workspace_id, request_id)
                receipt = await app.state.repo.create_or_get_message_receipt(
                    settings.workspace_id,
                    request_id,
                    conversation_ref,
                    payload["text"],
                )
            except ValueError as exc:
                if str(exc) == "request_id_reused":
                    raise HTTPException(status_code=409, detail="request_id_reused") from exc
                raise HTTPException(status_code=422, detail=str(exc)) from exc
            if existing is not None and receipt.state == "accepted":
                receipt = await app.state.repo.queue_message_receipt(settings.workspace_id, request_id)
            app.state.dispatcher.wake()
            body = {"accepted": True, "conversation_ref": conversation_ref, "request_id": request_id, "status": receipt.state}
            return JSONResponse(status_code=202, content=body)
        active_driver = await app.state.gateway.active_driver()
        if active_driver is not None:
            pending = app.state.forward_tasks.get(request_id)
            if pending is not None and not pending.done():
                return JSONResponse(status_code=202, content={"accepted": True, "conversation_ref": conversation_ref, "request_id": request_id, "status": "in_flight"})

            async def _forward_legacy() -> dict[str, Any] | None:
                try:
                    return await app.state.gateway.forward(
                        "/driver/v1/messages",
                        {**payload, "conversation_ref": conversation_ref, "request_id": request_id, "workspace_id": settings.workspace_id},
                        timeout=settings.observer_forward_timeout_seconds,
                    )
                except Exception:
                    return None

            task = asyncio.create_task(_forward_legacy())
            app.state.forward_tasks[request_id] = task
            task.add_done_callback(lambda done, rid=request_id: app.state.forward_tasks.pop(rid, None) if app.state.forward_tasks.get(rid) is done else None)
            return JSONResponse(status_code=202, content={"accepted": True, "conversation_ref": conversation_ref, "request_id": request_id, "status": "accepted"})
        if supplied_request_id:
            return JSONResponse(status_code=503, content={"code": "driver_unavailable", "retryable": True})
        if app.state.driver is None:
            return JSONResponse(status_code=503, content={"code": "driver_unavailable", "retryable": True})
        try:
            return await app.state.driver.run_prompt(conversation_ref, payload.get("text", ""))
        except (RuntimeError, FileNotFoundError) as exc:
            reason = getattr(exc, "reason", None)
            if isinstance(reason, dict):
                reason = dict(reason)
                code = str(reason.get("code") or "coding_agent_error")
                retryable = code not in {"coding_agent_blocked", "coding_agent_usage_limited", "coding_agent_budget_limited"}
                return JSONResponse(status_code=503, content={"code": code, "retryable": retryable, "details": reason})
            return JSONResponse(status_code=503, content={"code": str(exc), "retryable": True})

    @app.get("/api/v1/conversations")
    async def conversations() -> list[dict[str, Any]]:
        return await app.state.repo.list_conversations(settings.workspace_id)

    @app.get("/api/v1/conversations/{conversation_ref}")
    async def conversation(conversation_ref: str) -> dict[str, Any]:
        try:
            return await app.state.repo.get_conversation(conversation_ref, settings.workspace_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="conversation_not_found") from exc

    @app.post("/api/v1/conversations/{conversation_ref}/interrupt")
    async def interrupt_conversation(conversation_ref: str) -> dict[str, Any]:
        if not legacy_embedded:
            receipts = await app.state.repo.list_message_receipts(settings.workspace_id, conversation_ref)
            pending = next((item for item in reversed(receipts) if item.state in {"accepted", "queued", "retryable", "in_flight"}), None)
            if pending is None:
                raise HTTPException(status_code=409, detail="conversation_not_active")
            if pending.state != "in_flight":
                interrupted = await app.state.repo.interrupt_message_receipt(settings.workspace_id, pending.request_id)
                return {"conversation_ref": conversation_ref, "request_id": interrupted.request_id, "status": "interrupted"}
            try:
                return await app.state.gateway.forward(
                    f"/driver/v1/conversations/{conversation_ref}/interrupt",
                    {"conversation_ref": conversation_ref, "workspace_id": settings.workspace_id, "request_id": pending.request_id},
                )
            except RuntimeError as exc:
                code = str(exc)
                raise HTTPException(status_code=409 if code == "stale_driver_epoch" else 503, detail=code) from exc
        active_driver = await app.state.gateway.active_driver()
        if active_driver is not None:
            try:
                return await app.state.gateway.forward(f"/driver/v1/conversations/{conversation_ref}/interrupt", {"conversation_ref": conversation_ref, "workspace_id": settings.workspace_id})
            except RuntimeError as exc:
                raise HTTPException(status_code=503, detail=str(exc)) from exc
        if app.state.driver is None:
            raise HTTPException(status_code=503, detail="driver_unavailable")
        try:
            return await app.state.driver.interrupt(conversation_ref)
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.get("/api/v1/conversations/{conversation_ref}/stream")
    async def stream(conversation_ref: str) -> StreamingResponse:
        async def events() -> Any:
            try:
                conversation_view = await app.state.repo.get_conversation(conversation_ref, settings.workspace_id)
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
        except DomainError as exc:
            raise HTTPException(status_code=409, detail=exc.envelope.model_dump(mode="json")) from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"closure_version": version.version_id, "snapshot_digest": version.snapshot_digest, "state": "committed"}

    @app.post("/api/v1/runs/{run_id}/start")
    async def start(run_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            return await app.state.repo.start(run_id, payload["closure_version"])
        except DomainError as exc:
            raise HTTPException(status_code=409, detail=exc.envelope.model_dump(mode="json")) from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.post("/api/v1/runs/{run_id}/close")
    async def close_run(run_id: str) -> dict[str, Any]:
        try:
            return _run_view(await app.state.repo.close_run(run_id))
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.post("/api/v1/runs/{run_id}/resolve")
    async def resolve_run(run_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        decision = str(payload.get("decision") or "")
        if decision not in {"accept", "abandon"}:
            raise HTTPException(status_code=422, detail="invalid_decision")
        try:
            return _run_view(await app.state.repo.resolve_run(run_id, decision))
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="run_not_found") from exc
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
        await app.state.repo.refresh_slaves(settings.workspace_id)
        registered = list(app.state.repo.slave_agents.values())
        if registered:
            records = await app.state.repo._all_records()
            result: list[dict[str, Any]] = []
            for agent in registered:
                caps = agent.get("capabilities") or {}
                result.append(
                    {
                        "slave_id": agent["agent_id"],
                        "available": agent.get("lease_state") == "active",
                        "replica": "ready" if agent.get("lease_state") == "active" else "unavailable",
                        "operations": sorted(set(caps.get("operations", []))),
                        "term_support": caps.get("term_support", []),
                        "activations": [activation.model_dump(mode="json") for record in records for activation in record.capability_activations if activation.target_slave == agent["agent_id"] and activation.activation_state == "ready"],
                        "executor_descriptors": caps.get("executor_descriptors", []),
                    }
                )
            return result
        if not legacy_embedded:
            return []
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
        if targets and not legacy_embedded:
            try:
                return {
                    "package": package.model_dump(mode="json"),
                    **await app.state.gateway.forward(
                        "/driver/v1/capability/provision",
                        {
                            "package_ref": package.version_ref,
                            "target_slaves": targets,
                            "compute_binding": payload.get("compute_binding"),
                            "idempotency_key": payload.get("idempotency_key"),
                        },
                    ),
                }
            except RuntimeError as exc:
                return {
                    "package": package.model_dump(mode="json"),
                    "health_reports": [
                        {"target_slave": target, "activation_state": "failed", "error": str(exc)}
                        for target in targets
                    ],
                }
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

    @app.get("/")
    async def home() -> FileResponse:
        return FileResponse(static_dir / "index.html")

    return app


app = create_app()
