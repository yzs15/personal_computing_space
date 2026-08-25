from loom_v2.db.base import Base, SlaveBase


def test_slave_tables_are_not_part_of_observer_metadata():
    assert set(Base.metadata.tables) == {"runs", "idempotency"}
    assert set(SlaveBase.metadata.tables) == {"slave_attempts", "slave_replica"}
