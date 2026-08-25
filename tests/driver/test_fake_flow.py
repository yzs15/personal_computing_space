import pytest

from loom_v2.coding_agents.fake import FakeCodingAgentProvider


@pytest.mark.asyncio
async def test_fake_provider_emits_multiple_patches_then_commit():
    provider = FakeCodingAgentProvider()
    events = [event async for event in provider.send_turn("echo")]
    assert [event.kind for event in events].count("apply_plan_patch") >= 2
    assert events[-1].kind == "commit_plan"
