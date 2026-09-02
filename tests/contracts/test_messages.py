import pytest
from pydantic import ValidationError

from loom_v2.contracts.messages import MessageReceipt


def test_message_receipt_requires_workspace_request_and_digest():
    receipt = MessageReceipt(
        workspace_id="workspace-default",
        request_id="request-1",
        conversation_ref="conversation-1",
        prompt="hello",
        payload_digest="a" * 64,
        state="accepted",
        attempt_count=0,
    )
    assert (receipt.workspace_id, receipt.request_id) == ("workspace-default", "request-1")


def test_message_receipt_rejects_unknown_state():
    with pytest.raises(ValidationError):
        MessageReceipt(
            workspace_id="workspace-default",
            request_id="request-1",
            conversation_ref="conversation-1",
            prompt="hello",
            payload_digest="a" * 64,
            state="unknown",
            attempt_count=0,
        )

