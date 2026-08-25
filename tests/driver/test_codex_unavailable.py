import pytest

from loom_v2.coding_agents.codex import CodexAppServerProvider


@pytest.mark.asyncio
async def test_missing_codex_binary_is_structured_unavailable():
    provider = CodexAppServerProvider(executable="loom-no-codex")
    with pytest.raises(RuntimeError, match="coding_agent_unavailable"):
        await provider.start("conversation", "/workspace")
