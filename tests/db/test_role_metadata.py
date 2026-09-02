from loom_v2.db.base import Base, SlaveBase


def test_observer_metadata_contains_authority_and_agent_state():
    assert {"runs", "idempotency", "runtime_agents", "driver_threads", "driver_requests"} <= set(Base.metadata.tables)
    assert set(SlaveBase.metadata.tables) == {"slave_attempts", "slave_replica"}
