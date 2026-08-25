import asyncio

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import create_async_engine

from loom_v2.observer.app import create_app
from loom_v2.observer.repository import ObserverRepository


@pytest.mark.asyncio
async def test_message_events_survive_repository_reload():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    repo = ObserverRepository(engine)
    await repo.init_db()
    run = await repo.open_run("run-message", "conversation-1", "能力问题")
    await repo.append_message(run.run_id, "user", "能力问题")
    await repo.append_message(run.run_id, "assistant", "两者能力相同")

    restored = ObserverRepository(engine)
    await restored.init_db()
    conversation = await restored.get_conversation("conversation-1")

    assert [message["content"] for message in conversation["messages"]] == ["能力问题", "两者能力相同"]


def test_conversation_list_history_and_stream_use_persisted_messages():
    repository = ObserverRepository()
    client = TestClient(create_app(repository))
    run = client.post("/api/v1/runs", json={"task_ref": "conversation-history", "goal": "能力问题"}).json()
    asyncio.run(repository.append_message(run["run_id"], "user", "能力问题"))
    asyncio.run(repository.append_message(run["run_id"], "assistant", "两者能力相同"))

    conversations = client.get("/api/v1/conversations")
    assert conversations.status_code == 200
    assert conversations.json() == [
        {
            "conversation_ref": "conversation-history",
            "title": "能力问题",
            "run_count": 1,
            "latest_run_id": run["run_id"],
        }
    ]

    history = client.get("/api/v1/conversations/conversation-history")
    assert history.status_code == 200
    assert [message["role"] for message in history.json()["messages"]] == ["user", "assistant"]

    stream = client.get("/api/v1/conversations/conversation-history/stream")
    assert stream.status_code == 200
    assert '"phase": "message"' in stream.text
    assert "两者能力相同" in stream.text
