import pytest

from loom_v2.coding_agents.fake import FakeCodingAgentProvider
from loom_v2.contracts.types import ResourceRef


async def fake_tool_handler(tool: str, arguments: dict[str, object]) -> dict[str, object]:
    if tool == "loom_put_content":
        digest = "a" * 64
        return {
            "resource_ref": ResourceRef(
                resource_id=f"content://sha256/{digest}",
                version_or_digest=digest,
                identity_criterion="content_digest",
            ).model_dump(mode="json")
        }
    return {}


@pytest.mark.asyncio
async def test_fake_provider_emits_multiple_patches_then_commit():
    provider = FakeCodingAgentProvider()
    context = await provider.begin_turn("conversation-fake", "request-fake", "/workspace", handler=fake_tool_handler)
    events = [event async for event in provider.send_turn(context, "run code")]
    assert [event.kind for event in events].count("tool_call") >= 4
    assert events[-1].kind == "tool_call"
