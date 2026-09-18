"""Compare local lifecycle operations with the authenticated Observer HTTP API."""

from copy import deepcopy

import httpx
import pytest

from loom_v2.contracts.types import NodeIntent, ResourceRef
from loom_v2.driver.control_client import ObserverControlClient
from loom_v2.driver.remote_repository import RemoteObserverRepository
from loom_v2.observer.app import create_app
from loom_v2.observer.repository import ObserverRepository
from loom_v2.settings import Settings
from tests.api.test_dynamic_nodes import _dynamic_run


@pytest.fixture
async def repositories():
    authority = ObserverRepository()
    app = create_app(authority, settings=Settings(internal_api_secret="parity-secret"))
    control = ObserverControlClient(
        "http://observer", driver_id="parity-driver", instance_id="parity-instance",
        internal_api_secret="parity-secret", transport=httpx.ASGITransport(app=app),
    )
    try:
        await control.register()
        remote = RemoteObserverRepository(control, content_store=authority.content_store)
        local = ObserverRepository()
        yield local, authority, remote, control
    finally:
        await control.close()


def semantic_value(value):
    """Exclude only server timestamps and independently generated message IDs."""
    if isinstance(value, dict):
        return {key: semantic_value(item) for key, item in value.items() if key not in {"created_at", "message_id"}}
    if isinstance(value, list):
        return [semantic_value(item) for item in value]
    return value


def observe(record):
    return semantic_value({
        "state": record.state, "outcome": record.outcome, "events": record.events,
        "attempts": record.attempts, "execution_id": record.execution_id,
        "execution_epoch": record.execution_epoch,
    })


@pytest.mark.asyncio
async def test_local_and_http_run_lifecycle_preserve_messages_and_failure(repositories):
    local, authority, remote, _ = repositories
    await local.open_run("parity-run", "parity-conversation", "goal")
    authority.runs = deepcopy(local.runs)
    observations = []
    for adapter in (local, remote):
        assert (await adapter.get_run("parity-run")).state == "opened"
        assert (await adapter.begin_refinement("parity-run")).state == "thinking"
        await adapter.append_message("parity-run", "user", "hello", request_id="message-1")
        await adapter.append_message("parity-run", "user", "hello", request_id="message-1")
        failed = await adapter.fail_run("parity-run", {"code": "test_failure"})
        messages = [event for event in failed.events if event["phase"] == "message"]
        assert len(messages) == 1
        assert messages[0]["content"] == "hello"
        assert messages[0]["request_id"] == "message-1"
        observations.append(observe(failed))
    assert observations[0] == observations[1]


@pytest.mark.asyncio
@pytest.mark.parametrize("decision,expected", [("accept", "completed"), ("abandon", "failed")])
async def test_result_and_resolution_match_over_http(repositories, decision, expected):
    local, authority, remote, _ = repositories
    record = await local.open_run("parity-result", "conversation", "goal")
    await local.begin_refinement(record.run_id)
    version = await local.commit(record.run_id, record.draft_version, record.draft_digest)
    await local.start(record.run_id, version.version_id)
    authority.runs = deepcopy(local.runs)
    result = {
        "attempt_id": record.attempts[0]["attempt_id"], "execution_id": record.execution_id,
        "execution_epoch": record.execution_epoch, "value": {"text": "candidate"},
        "terminal_state": "decision_required",
    }
    observations = []
    for adapter in (local, remote):
        pending = await adapter.record_result(record.run_id, result)
        assert pending.state == "awaiting_decision"
        observations.append(observe(pending))
    assert observations[0] == observations[1]
    for adapter in (local, remote):
        with pytest.raises((ValueError, RuntimeError), match="^invalid_decision$"):
            await adapter.resolve_run(record.run_id, "invalid")
    resolved = []
    for adapter in (local, remote):
        final = await adapter.resolve_run(record.run_id, decision)
        assert final.state == expected
        resolved.append(observe(final))
        replay = await adapter.resolve_run(record.run_id, decision)
        assert observe(replay) == resolved[-1]
    assert resolved[0] == resolved[1]


@pytest.mark.asyncio
@pytest.mark.parametrize("valid_lineage", [True, False])
async def test_orchestration_completion_matches_over_http(repositories, valid_lineage):
    local, authority, remote, _ = repositories
    run, patched = await _dynamic_run(local)
    committed = await local.commit(run.run_id, patched.draft_version, patched.draft_digest)
    started = await local.start(run.run_id, committed.version_id)
    record = await local.get_run(run.run_id)
    package = next(item for item in record.capability_packages if item.package_id == "summarize")
    input_ref = next(item.input_ref for item in record.draft.snapshot.node_input_bindings if item.node_id == "loom://summarize")
    node = await local.accept_node_intent(run.run_id, NodeIntent(
        intent_id="parity-node", execution_id=started["execution_id"],
        package_ref=ResourceRef(resource_id=package.version_ref, version_or_digest=package.package_digest),
        capability_descriptor_ref=package.capability_exports[0].capability_descriptor_ref,
        input_refs=[input_ref],
    ), selected_target="slave-a")
    attempt = await local.dispatch_dynamic_node(run.run_id, node.node_id, target="slave-a")
    await local.record_dynamic_node_result(run.run_id, node.node_id, {
        "attempt_id": attempt["attempt_id"], "execution_id": started["execution_id"],
        "execution_epoch": 1, "value": {"count": 3, "sum": 6},
    })
    final_ref = ResourceRef.model_validate(record.events[-1]["result_ref"])
    if not valid_lineage:
        final_ref = await local.put_content({"unrelated": True}, media_type="application/json")
    authority.runs = deepcopy(local.runs)
    observations = []
    for adapter in (local, remote):
        completed = await adapter.complete_orchestration(run.run_id, final_ref)
        assert completed.state == ("completed" if valid_lineage else "awaiting_decision")
        if not valid_lineage:
            assert completed.outcome["terminal_error"]["code"] == "orchestration_lineage_mismatch"
        observations.append(observe(completed))
    assert observations[0] == observations[1]


@pytest.mark.asyncio
async def test_rpc_replay_is_fenced_before_cached_response(repositories):
    local, authority, remote, control = repositories
    await authority.open_run("replay-run", "conversation", "goal")
    await remote.begin_refinement("replay-run")
    await remote.fail_run("replay-run", "failed")
    # Same RPC id replays its original response; a new call to local begin
    # validates current state. This transport distinction must remain explicit.
    assert (await remote.begin_refinement("replay-run")).state == "thinking"
    assert (await remote.get_run("replay-run")).state == "failed"
    with pytest.raises(ValueError, match="^illegal_state_transition$"):
        await authority.begin_refinement("replay-run")
    control.driver_epoch += 1
    with pytest.raises(RuntimeError, match="^stale_driver_epoch$"):
        await remote.begin_refinement("replay-run")
