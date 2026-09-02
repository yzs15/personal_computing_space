import pytest

from loom_v2.observer.repository import ObserverRepository


@pytest.mark.asyncio
async def test_receipt_state_machine_is_fenced_and_idempotent():
    repo = ObserverRepository()
    receipt = await repo.create_or_get_message_receipt("workspace-default", "request-1", "conversation-1", "hello")
    assert receipt.state == "accepted"
    queued = await repo.queue_message_receipt(receipt.workspace_id, receipt.request_id)
    assert queued.state == "queued"
    claimed = await repo.claim_message_receipt(
        receipt.workspace_id,
        receipt.request_id,
        payload_digest=receipt.payload_digest,
        claim_token="claim-1",
    )
    assert claimed.state == "in_flight"
    duplicate = await repo.claim_message_receipt(
        receipt.workspace_id,
        receipt.request_id,
        payload_digest=receipt.payload_digest,
        claim_token="claim-2",
    )
    assert duplicate.claim_token == "claim-1"
    with pytest.raises(ValueError, match="stale_claim_token"):
        await repo.update_message_receipt("workspace-default", "request-1", claim_token="claim-2", state="completed")
    completed = await repo.update_message_receipt(
        "workspace-default", "request-1", claim_token="claim-1", state="completed", assistant_text="done"
    )
    assert completed.state == "completed"
    assert (await repo.claim_message_receipt(
        "workspace-default", "request-1", payload_digest=receipt.payload_digest, claim_token="claim-3"
    )).claim_token is None  # terminal receipts are immutable


@pytest.mark.asyncio
async def test_receipt_only_projection_can_be_loaded():
    repo = ObserverRepository()
    receipt = await repo.create_or_get_message_receipt("workspace-default", "request-2", "conversation-2", "hello")
    await repo.queue_message_receipt("workspace-default", "request-2")
    claimed = await repo.claim_message_receipt("workspace-default", "request-2", payload_digest=receipt.payload_digest, claim_token="claim")
    await repo.update_message_receipt("workspace-default", "request-2", claim_token=claimed.claim_token or "", state="completed", assistant_text="plain reply")
    view = await repo.get_conversation("conversation-2")
    assert view["status"] == "completed"
    assert [message["role"] for message in view["messages"]] == ["user", "assistant"]
    assert [event["state"] for event in view["events"] if event["phase"] == "message_receipt"] == ["accepted", "queued", "in_flight", "completed"]


@pytest.mark.asyncio
async def test_new_driver_registration_releases_old_message_claims():
    from loom_v2.contracts.agents import AgentRegistration

    repo = ObserverRepository()
    receipt = await repo.create_or_get_message_receipt("workspace-default", "request-3", "conversation-3", "hello")
    claimed = await repo.claim_message_receipt("workspace-default", receipt.request_id, payload_digest=receipt.payload_digest, claim_token="old")
    assert claimed.state == "in_flight"
    await repo.register_agent(AgentRegistration(role="driver", agent_id="driver", instance_id="new", workspace_id="workspace-default", endpoint_url="http://driver", protocol_version="loom.v1"))
    recovered = await repo.get_message_receipt("workspace-default", receipt.request_id)
    assert recovered is not None and recovered.state == "retryable" and recovered.claim_token is None
