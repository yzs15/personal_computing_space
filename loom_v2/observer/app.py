from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
import json
from pathlib import Path
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles

from loom_v2.db.session import make_engine
from loom_v2.settings import Settings
from loom_v2.contracts.types import CapabilityHealthReport
from loom_v2.contracts.api import (
    AgentLeaseRequest,
    CapabilityActivationRequest,
    CapabilityHealthRequest,
    CommitRequest,
    ContentPutRequest,
    IdempotencyRequest,
    OpenRunRequest,
    PatchRequest,
    PublicMessageRequest,
    PublicSlave,
    PublicSlaveList,
    ResolveRequest,
    StartRequest,
)
from loom_v2.content_store import ContentStore
from loom_v2.contracts.agents import AgentRegistration, DriverCommand
from loom_v2.contracts.refs import select_slave_agents
from loom_v2.contracts.errors import DomainError
from loom_v2.observer.gateway import ObserverDriverGateway
from loom_v2.observer.dispatcher import ObserverMessageDispatcher
from loom_v2.observer.activation import execute_activation_targets
from loom_v2.auth import InternalAuth
from loom_v2.contracts.package_contracts import load_operator_package_contracts

from .repository import ObserverRepository, run_record_payload


def _run_view(record: Any) -> dict[str, Any]:
    return run_record_payload(record, include_snapshots=False)


def create_app(
    repository: ObserverRepository | None = None,
    *,
    settings: Settings | None = None,
) -> FastAPI:
    settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        await app.state.repo.init_db()
        await app.state.dispatcher.start()
        try:
            yield
        finally:
            await app.state.dispatcher.stop()
            await app.state.gateway.close()
            for worker in app.state.workers.values():
                with suppress(Exception):
                    await worker.close()
            for slave in app.state.slaves.values():
                with suppress(Exception):
                    await slave.close()
            if app.state.engine is not None:
                await app.state.engine.dispose()

    app = FastAPI(title="Loom v2 Observer", lifespan=lifespan)
    load_operator_package_contracts(settings.package_contract_dir)
    app.state.engine = None
    if repository is None and not settings.database_url.startswith("sqlite+aiosqlite:///:memory:"):
        app.state.engine = make_engine(settings.database_url)
    content_store = repository.content_store if repository is not None else ContentStore.from_settings(settings)
    app.state.repo = repository or ObserverRepository(app.state.engine, content_store=content_store, settings=settings)
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
    # Observer production composition only owns the repository, gateway and
    # dispatcher.  Embedded Driver/Slave/MCP wiring lives in
    # ``loom_v2.testing.observer`` and is never imported by this module.
    app.state.embedded = False
    app.state.repo.orchestrator_runtime_available = False
    app.state.provider = None
    app.state.slaves = {}
    app.state.workers = {}
    app.state.driver = None
    app.state.forward_tasks = {}
    app.state.mcp_server = None
    static_dir = Path(__file__).resolve().parents[1] / "web" / "static"
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

    @app.get("/healthz")
    async def health() -> dict[str, object]:
        return {"ok": True, "service": "observer"}

    require_internal = InternalAuth(settings.internal_api_secret)
    internal_router = APIRouter(dependencies=[Depends(require_internal)])

    @internal_router.post("/internal/v1/agents/register")
    async def register_agent(payload: AgentRegistration) -> dict[str, Any]:
        try:
            lease = await app.state.repo.register_agent(payload)
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return lease.model_dump(mode="json")

    @internal_router.post("/internal/v1/agents/{agent_id}/heartbeat")
    async def heartbeat_agent(agent_id: str, payload: AgentLeaseRequest) -> dict[str, Any]:
        try:
            epoch_value = payload.driver_epoch if payload.driver_epoch is not None else payload.epoch
            if epoch_value is None:
                raise KeyError("epoch")
            if payload.driver_epoch is not None and payload.epoch is not None and payload.driver_epoch != payload.epoch:
                raise ValueError("driver_epoch_mismatch")
            lease = await app.state.repo.heartbeat_agent(
                agent_id,
                payload.instance_id,
                payload.lease_id,
                int(epoch_value),
                workspace_id=payload.workspace_id or settings.workspace_id,
                role=payload.role,
            )
        except KeyError as exc:
            raise HTTPException(status_code=400, detail=f"missing_field:{exc.args[0]}") from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return lease.model_dump(mode="json")

    @internal_router.post("/internal/v1/agents/{agent_id}/release")
    async def release_agent(agent_id: str, payload: AgentLeaseRequest) -> dict[str, Any]:
        try:
            epoch_value = payload.driver_epoch if payload.driver_epoch is not None else payload.epoch
            if epoch_value is None:
                raise KeyError("epoch")
            if payload.driver_epoch is not None and payload.epoch is not None and payload.driver_epoch != payload.epoch:
                raise ValueError("driver_epoch_mismatch")
            await app.state.repo.release_agent(
                agent_id,
                payload.instance_id,
                payload.lease_id,
                int(epoch_value),
                workspace_id=payload.workspace_id or settings.workspace_id,
                role=payload.role,
            )
        except KeyError as exc:
            raise HTTPException(status_code=400, detail=f"missing_field:{exc.args[0]}") from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"released": True}

    @internal_router.get("/internal/v1/agents/slaves")
    async def list_slaves(workspace_id: str | None = None) -> list[dict[str, Any]]:
        agents = await app.state.repo.refresh_slaves(workspace_id or settings.workspace_id)
        for agent in agents:
            projected = app.state.repo.slave_capabilities.get(str(agent.get("agent_id") or ""))
            if projected is None:
                continue
            capabilities = dict(agent.get("capabilities") or {})
            capabilities["operations"] = sorted(projected.get("operations", set()))
            agent["capabilities"] = capabilities
        return agents

    @internal_router.post("/internal/v1/capability-health")
    async def capability_health(payload: CapabilityHealthRequest) -> dict[str, Any]:
        report: CapabilityHealthReport | None = None
        try:
            report = payload.report
            agent_id = payload.agent_id
            workspace_id = payload.workspace_id
            if report.target_slave != agent_id:
                raise ValueError("target_slave_mismatch")
            await app.state.repo.heartbeat_agent(
                agent_id,
                payload.instance_id,
                payload.lease_id,
                payload.epoch,
                workspace_id=workspace_id,
                role="slave",
            )
            # Health reports are scoped by the lease workspace as well as by
            # package identity.  Do this check before mutating the activation
            # projection so a valid Slave lease cannot report on another
            # workspace's package.
            try:
                await app.state.repo._assert_package_workspace(
                    report.package_version_ref,
                    workspace_id,
                )
            except KeyError as exc:
                raise HTTPException(
                    status_code=404,
                    detail="capability_package_not_found",
                ) from exc
            activation = await app.state.repo.record_capability_health(report)
        except KeyError as exc:
            raise HTTPException(status_code=400, detail=f"missing_field:{exc.args[0]}") from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"accepted": True, "activation": activation.model_dump(mode="json")}

    @internal_router.post("/internal/v1/driver/commands")
    async def driver_command(payload: DriverCommand) -> dict[str, Any]:
        try:
            result = await app.state.repo.execute_driver_command(payload)
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

    @internal_router.post("/driver-gateway/v1/registration-check")
    async def registration_check() -> dict[str, Any]:
        driver = await app.state.gateway.active_driver()
        return {"registered": driver is not None, "driver": driver}

    app.include_router(internal_router)

    @app.get("/api/v1/runtime", response_model=dict[str, str | None])
    async def runtime() -> dict[str, str | None] | JSONResponse:
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
        if not app.state.embedded:
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
    async def put_content(payload: ContentPutRequest) -> dict[str, Any]:
        if "content" not in payload.model_fields_set:
            raise HTTPException(status_code=400, detail="content_required")
        try:
            ref = await app.state.repo.put_content(payload.content, media_type=str(payload.media_type or ""))
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return ref.model_dump(mode="json")

    @app.post("/mcp")
    async def mcp(payload: dict[str, Any], request: Request) -> dict[str, Any]:
        conversation_ref = request.headers.get("x-loom-conversation-ref", "")
        if not app.state.embedded:
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
    async def open_run(payload: OpenRunRequest) -> dict[str, Any]:
        if not payload.task_ref:
            raise HTTPException(status_code=400, detail="task_ref_required")
        contract = payload.closure_contract
        goal = contract.goal if contract is not None else payload.goal or ""
        record = await app.state.repo.open_run(
            payload.run_id,
            payload.task_ref,
            goal,
            payload.allow_reassignment,
            contract,
            user_id=payload.user_id or "user-default",
            workspace_id=payload.workspace_id or settings.workspace_id,
        )
        return _run_view(record)

    @app.post("/api/v1/messages", response_model=dict[str, Any])
    async def message(payload: PublicMessageRequest) -> dict[str, Any] | JSONResponse:
        if not isinstance(payload.text, str) or not payload.text.strip():
            raise HTTPException(status_code=422, detail="text_required")
        if not app.state.embedded and not str(payload.request_id or "").strip():
            raise HTTPException(status_code=422, detail="request_id_required")
        if not app.state.embedded and not str(payload.conversation_ref or "").strip():
            raise HTTPException(status_code=422, detail="conversation_ref_required")
        if not app.state.embedded and (payload.workspace_id or settings.workspace_id) != settings.workspace_id:
            raise HTTPException(status_code=403, detail="workspace_binding_mismatch")
        conversation_ref = payload.conversation_ref or "conversation-default"
        supplied_request_id = bool(payload.request_id)
        request_id = payload.request_id or f"request-{uuid4().hex}"
        if not app.state.embedded:
            # A standalone Observer without an internal control-plane secret
            # has no safe way to deliver work when no Driver is registered.
            # Keep the synchronous failure contract for local deployments;
            # split deployments with a secret may durably queue the receipt
            # for a Driver that will register later.
            if not settings.internal_api_secret.strip() and await app.state.gateway.active_driver() is None:
                return JSONResponse(status_code=503, content={"code": "driver_unavailable", "retryable": True})
            try:
                existing = await app.state.repo.get_message_receipt(settings.workspace_id, request_id)
                receipt = await app.state.repo.create_or_get_message_receipt(
                    settings.workspace_id,
                    request_id,
                    conversation_ref,
                    payload.text,
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
                        {
                            **payload.model_dump(exclude_none=True),
                            "conversation_ref": conversation_ref,
                            "request_id": request_id,
                            "workspace_id": settings.workspace_id,
                        },
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
            return await app.state.driver.run_prompt(conversation_ref, payload.text)
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
        if not app.state.embedded:
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
        async def events() -> AsyncIterator[str]:
            try:
                conversation_view = await app.state.repo.get_conversation(conversation_ref, settings.workspace_id)
            except KeyError:
                return
            for event in conversation_view["events"]:
                yield f"data: {json.dumps(event, sort_keys=True, ensure_ascii=False)}\n\n"

        return StreamingResponse(events(), media_type="text/event-stream")

    @app.post("/api/v1/runs/{run_id}/patches")
    async def apply_patch(run_id: str, payload: PatchRequest) -> dict[str, Any]:
        for field_name in ("base_draft_version", "base_snapshot_digest", "operation_id"):
            if getattr(payload, field_name) is None:
                raise HTTPException(status_code=400, detail=f"missing_field:{field_name}")
        try:
            receipt = await app.state.repo.apply_patch(
                run_id,
                payload.base_draft_version,
                payload.base_snapshot_digest,
                payload.operation_id,
                payload.ops,
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
    async def commit(run_id: str, payload: CommitRequest) -> dict[str, Any]:
        for field_name in ("draft_version", "draft_digest"):
            if getattr(payload, field_name) is None:
                raise HTTPException(status_code=400, detail=f"missing_field:{field_name}")
        try:
            version = await app.state.repo.commit(run_id, payload.draft_version, payload.draft_digest)
        except DomainError as exc:
            raise HTTPException(status_code=409, detail=exc.envelope.model_dump(mode="json")) from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"closure_version": version.version_id, "snapshot_digest": version.snapshot_digest, "state": "committed"}

    @app.post("/api/v1/runs/{run_id}/start")
    async def start(run_id: str, payload: StartRequest) -> dict[str, Any]:
        if payload.closure_version is None:
            raise HTTPException(status_code=400, detail="missing_field:closure_version")
        try:
            return await app.state.repo.start(run_id, payload.closure_version)
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
    async def resolve_run(run_id: str, payload: ResolveRequest) -> dict[str, Any]:
        decision = str(payload.decision or "")
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

    @app.get("/api/v1/slaves", response_model=PublicSlaveList)
    async def public_slaves(
        available_only: bool = False, workspace_id: str | None = None,
    ) -> PublicSlaveList:
        # Public access is bound to this Observer's workspace, not an arbitrary
        # caller-supplied registry scope. Do not mutate the dispatcher's cache.
        if workspace_id is not None and workspace_id != settings.workspace_id:
            raise HTTPException(status_code=403, detail="workspace_binding_mismatch")
        agents = await app.state.repo.list_agents(settings.workspace_id, role="slave")
        # Prefer active instances; when all are offline show the latest report.
        agents.sort(key=lambda agent: str(agent.get("last_seen_at") or ""), reverse=True)
        selected = select_slave_agents(agents)
        slaves = []
        for slave_id, agent in sorted(selected.items()):
            available = agent.get("lease_state") == "active"
            if available_only and not available:
                continue
            caps = agent.get("capabilities") or {}
            slaves.append(PublicSlave(
                slave_id=slave_id,
                available=available,
                lease_state=agent["lease_state"],
                last_seen_at=agent.get("last_seen_at"),
                base_operations=sorted(caps.get("base_operations", caps.get("operations", []))),
                executor_descriptors=caps.get("executor_descriptors", []),
                runtime_plugin_descriptors=caps.get("runtime_plugin_descriptors", caps.get("runtime_plugins", [])),
                term_support=caps.get("term_support", []),
            ))
        return PublicSlaveList(workspace_id=settings.workspace_id, slaves=slaves)

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
                        "operations": sorted(app.state.repo.slave_capabilities.get(str(agent["agent_id"]), {}).get("operations", set())),
                        "term_support": caps.get("term_support", []),
                        "activations": [activation.model_dump(mode="json") for record in records for activation in record.capability_activations if activation.target_slave == agent["agent_id"] and activation.activation_state == "ready"],
                        "executor_descriptors": caps.get("executor_descriptors", []),
                        "runtime_plugin_descriptors": caps.get("runtime_plugin_descriptors", caps.get("runtime_plugins", [])),
                    }
                )
            return result
        if not app.state.embedded:
            return []
        records = await app.state.repo._all_records()
        result = []
        for slave_id, slave in app.state.slaves.items():
            activation_map = {
                (activation.package_version_ref, activation.target_slave): activation
                for activation in slave.activations.values()
            }
            for record in records:
                for activation in record.capability_activations:
                    if activation.target_slave == slave_id and activation.activation_state == "ready":
                        activation_map.setdefault(
                            (activation.package_version_ref, activation.target_slave),
                            activation,
                        )
            result.append(
                {
                    "slave_id": slave_id,
                    "available": slave.available,
                    "replica": slave.replica.state,
                    "operations": sorted(slave.capability_snapshot()),
                    "term_support": [support.model_dump(mode="json") for support in slave.term_support()],
                    "activations": [activation.model_dump(mode="json") for activation in activation_map.values()],
                    "executor_descriptors": [{"package_type": descriptor.package_type, "kind": descriptor.kind, "version": descriptor.version, "operations": sorted(descriptor.operations), "descriptor_ref": descriptor.descriptor_ref, "digest": descriptor.digest} for descriptor in slave.executor_registry.descriptors()],
                    "runtime_plugin_descriptors": [descriptor.__dict__ | {"supports": [support.to_mapping() for support in descriptor.supports]} for descriptor in slave.runtime_plugin_host.descriptors()] if slave.runtime_plugin_host is not None else [],
                }
            )
        return result

    @app.get("/api/v1/capability-packages")
    async def capability_packages(run_id: str | None = None, include_abandoned: bool = False) -> list[dict[str, Any]]:
        packages = await app.state.repo.list_capability_packages(run_id=run_id, include_abandoned=include_abandoned)
        result = []
        records = await app.state.repo._all_records()
        for package in packages:
            item = package.model_dump(mode="json")
            item["activations"] = [activation.model_dump(mode="json") for record in records for activation in record.capability_activations if activation.package_version_ref == package.version_ref]
            result.append(item)
        return result

    @app.get("/api/v1/runs/{run_id}/capability-packages")
    async def run_capability_packages(run_id: str) -> list[dict[str, Any]]:
        try:
            packages = await app.state.repo.list_capability_packages(run_id=run_id, include_abandoned=True)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="run_not_found") from exc
        record = await app.state.repo.get_run(run_id)
        return [{**package.model_dump(mode="json"), "activations": [activation.model_dump(mode="json") for activation in record.capability_activations if activation.package_version_ref == package.version_ref]} for package in packages]

    @app.post("/api/v1/capability-packages/{package_ref:path}/promote")
    async def promote_capability_package(package_ref: str, payload: CapabilityActivationRequest | None = None) -> dict[str, Any]:
        payload = payload or CapabilityActivationRequest()
        try:
            package = await app.state.repo.promote_capability_package(package_ref, idempotency_key=payload.idempotency_key, approved_digest=payload.approved_digest)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="capability_package_not_found") from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        targets = payload.target_slaves
        if targets and not app.state.embedded:
            try:
                return {
                    "package": package.model_dump(mode="json"),
                    **await app.state.gateway.forward(
                        "/driver/v1/capability/provision",
                        {
                            "package_ref": package.version_ref,
                            "target_slaves": targets,
                            "compute_binding": payload.compute_binding.model_dump(mode="json") if payload.compute_binding else None,
                            "idempotency_key": payload.idempotency_key,
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
        reports = await execute_activation_targets(
            app.state.repo,
            package,
            targets,
            desired_state="running",
            workspace_id=settings.workspace_id,
            workers=app.state.workers,
            slaves=app.state.slaves,
            compute_binding=payload.compute_binding.model_dump(mode="json") if payload.compute_binding else None,
            idempotency_key=payload.idempotency_key,
        )
        return {"package": package.model_dump(mode="json"), "health_reports": reports}

    @app.post("/api/v1/capability-packages/{package_ref:path}/deactivate")
    async def deactivate_capability_package(package_ref: str, payload: CapabilityActivationRequest | None = None) -> dict[str, Any]:
        payload = payload or CapabilityActivationRequest()
        try:
            package = await app.state.repo.get_capability_package(package_ref)
            approved_digest = payload.approved_digest
            if not approved_digest:
                raise ValueError("approved_digest_required")
            if str(approved_digest).lower() != package.package_digest.lower():
                raise ValueError("capability_package_digest_mismatch")
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="capability_package_not_found") from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        targets = payload.target_slaves
        if not targets:
            raise HTTPException(status_code=400, detail="target_slave_required")
        if not app.state.embedded:
            try:
                return {
                    "package": package.model_dump(mode="json"),
                    **await app.state.gateway.forward(
                        "/driver/v1/capability/deprovision",
                        {
                            "package_ref": package.version_ref,
                            "target_slaves": targets,
                            "approved_digest": package.package_digest,
                            "idempotency_key": payload.idempotency_key,
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
        reports = await execute_activation_targets(
            app.state.repo,
            package,
            targets,
            desired_state="stopped",
            workspace_id=settings.workspace_id,
            workers=app.state.workers,
            slaves=app.state.slaves,
            idempotency_key=payload.idempotency_key,
        )
        return {"package": package.model_dump(mode="json"), "health_reports": reports}

    @app.post("/api/v1/capability-packages/{package_ref:path}/abandon")
    async def abandon_capability_package(package_ref: str, payload: IdempotencyRequest | None = None) -> dict[str, Any]:
        try:
            package = await app.state.repo.abandon_capability_package(package_ref, idempotency_key=payload.idempotency_key if payload else None)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="capability_package_not_found") from exc
        return package.model_dump(mode="json")

    @app.get("/")
    async def home() -> FileResponse:
        return FileResponse(static_dir / "index.html")

    return app


app = create_app()
