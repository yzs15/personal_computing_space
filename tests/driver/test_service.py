import pytest

from loom_v2.coding_agents.fake import FakeCodingAgentProvider
from loom_v2.driver.service import DriverService
from loom_v2.observer.repository import ObserverRepository


@pytest.mark.asyncio
async def test_driver_applies_fake_patches_continuously_and_starts_execution():
    repo = ObserverRepository()
    driver = DriverService(repo, FakeCodingAgentProvider())
    result = await driver.run_prompt("conversation-1", "echo hello")
    assert result["state"] == "completed"
    assert result["patches"] >= 3
    assert result["closure_version"].startswith("committed-")
    assert result["resource_ref"].startswith("result-")
    assert result["conversation_ref"] == "conversation-1"
    assert result["assistant_text"] == "I will refine the closure in multiple patches."
    conversation = await repo.get_conversation("conversation-1")
    assert [message["role"] for message in conversation["messages"]] == ["user", "assistant"]
