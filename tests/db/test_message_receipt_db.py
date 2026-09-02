import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from loom_v2.observer.repository import ObserverRepository


@pytest.mark.asyncio
async def test_message_receipt_persists_and_composite_key_is_idempotent():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    repo = ObserverRepository(engine)
    await repo.init_db()
    first = await repo.create_or_get_message_receipt("workspace-default", "request-1", "conversation-1", "hello")
    second = await repo.create_or_get_message_receipt("workspace-default", "request-1", "conversation-1", "hello")
    assert second.payload_digest == first.payload_digest
    with pytest.raises(ValueError, match="request_id_reused"):
        await repo.create_or_get_message_receipt("workspace-default", "request-1", "conversation-1", "different")
    loaded = await repo.get_message_receipt("workspace-default", "request-1")
    assert loaded is not None and loaded.state == "accepted"
    await engine.dispose()

