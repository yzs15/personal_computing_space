"""Embedded Observer composition for hermetic tests.

Production Observer does not construct Driver, Slave, MCP, or Worker objects.
Tests that need the old in-process topology opt into this factory explicitly.
"""

from __future__ import annotations

from datetime import datetime, timezone

from loom_v2.coding_agents.codex import CodexAppServerProvider
from loom_v2.coding_agents.fake import FakeCodingAgentProvider
from loom_v2.driver.mcp_server import DriverMCPServer
from loom_v2.driver.service import DriverService
from loom_v2.driver.worker import WorkerSession
from loom_v2.observer.app import create_app as create_observer_app
from loom_v2.observer.repository import ObserverRepository
from loom_v2.settings import Settings
from loom_v2.slave.service import SlaveService


def seed_embedded_slaves(
    repository: ObserverRepository,
    slaves: dict[str, SlaveService] | None = None,
    *,
    settings: Settings | None = None,
) -> dict[str, SlaveService]:
    """Seed the in-memory slave lease/capability projection for tests.

    Production repositories intentionally start without embedded agents.  A
    repository-only test can opt into the same deterministic topology without
    constructing a FastAPI application by calling this helper directly.
    """
    settings = settings or repository.settings
    if slaves is None:
        slaves = {
            "slave-a": SlaveService(
                "slave-a",
                content_store=repository.content_store,
                capability_operation_timeout_seconds=settings.capability_operation_timeout_seconds,
            ),
            "slave-b": SlaveService(
                "slave-b",
                content_store=repository.content_store,
                capability_operation_timeout_seconds=settings.capability_operation_timeout_seconds,
            ),
        }
    now = datetime.now(timezone.utc)
    for slave_id, slave in slaves.items():
        instance_id = f"embedded-{slave_id}"
        repository.agents[(repository.settings.workspace_id, "slave", slave_id, instance_id)] = {
            "workspace_id": repository.settings.workspace_id,
            "role": "slave",
            "agent_id": slave_id,
            "instance_id": instance_id,
            "endpoint_url": f"http://{slave_id}",
            "protocol_version": "loom.v1",
            "capabilities": {
                "operations": sorted(slave.supported_operations),
                "base_operations": sorted(slave.supported_operations),
                "executor_descriptors": [
                    {
                        "package_type": descriptor.package_type,
                        "kind": descriptor.kind,
                        "version": descriptor.version,
                        "operations": sorted(descriptor.operations),
                        "descriptor_ref": descriptor.descriptor_ref,
                        "digest": descriptor.digest,
                    }
                    for descriptor in slave.executor_registry.descriptors()
                ],
                "runtime_plugin_descriptors": [
                    descriptor.__dict__
                    | {"supports": [support.to_mapping() for support in descriptor.supports]}
                    for descriptor in slave.runtime_plugin_host.descriptors()
                ] if slave.runtime_plugin_host is not None else [],
                "term_support": [item.model_dump(mode="json") for item in slave.term_support()],
            },
            "epoch": 1,
            "lease_id_hash": "",
            "lease_state": "active",
            "last_seen_at": now,
            "created_at": now,
            "updated_at": now,
        }
    repository._store_slave_snapshot(list(repository.agents.values()))
    return slaves


def create_embedded_app(
    repository: ObserverRepository | None = None,
    *,
    settings: Settings | None = None,
):
    """Build the in-process Driver/Slave topology used by hermetic tests."""
    settings = settings or Settings()
    app = create_observer_app(repository, settings=settings)
    repository = app.state.repo
    slaves = {
        "slave-a": SlaveService(
            "slave-a",
            content_store=repository.content_store,
            capability_operation_timeout_seconds=settings.capability_operation_timeout_seconds,
        ),
        "slave-b": SlaveService(
            "slave-b",
            content_store=repository.content_store,
            capability_operation_timeout_seconds=settings.capability_operation_timeout_seconds,
        ),
    }
    workers: dict[str, WorkerSession] = {}
    if settings.slave_a_url:
        workers["slave-a"] = WorkerSession("slave-a", settings.slave_a_url, operation_timeout=settings.worker_operation_timeout_seconds)
    if settings.slave_b_url:
        workers["slave-b"] = WorkerSession("slave-b", settings.slave_b_url, operation_timeout=settings.worker_operation_timeout_seconds)
    provider = (
        FakeCodingAgentProvider()
        if settings.coding_agent_backend == "fake"
        else CodexAppServerProvider(
            model=settings.codex_model,
            poll_interval_seconds=settings.coding_agent_poll_interval_seconds,
            protocol_failure_seconds=settings.coding_agent_protocol_failure_seconds,
            settings=settings,
        )
    )
    app.state.embedded = True
    app.state.repo.orchestrator_runtime_available = True
    app.state.provider = provider
    app.state.slaves = slaves
    app.state.workers = workers
    app.state.driver = DriverService(
        app.state.repo,
        provider,
        slaves=slaves,
        workers=workers,
        deadline_seconds=settings.coding_agent_deadline_seconds,
        settings=settings,
    )
    app.state.mcp_server = DriverMCPServer(
        app.state.repo,
        run_executor=app.state.driver._execute_and_wait_local,
    )
    # Preserve repositories supplied by a test (including intentionally
    # expired leases); only seed a fresh repository with the default topology.
    if not any(key[1] == "slave" for key in app.state.repo.agents):
        seed_embedded_slaves(app.state.repo, slaves, settings=settings)
    else:
        app.state.repo._store_slave_snapshot(list(app.state.repo.agents.values()))
    return app


__all__ = ["create_embedded_app", "seed_embedded_slaves"]
