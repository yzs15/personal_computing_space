from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class SlaveBase(DeclarativeBase):
    """Metadata boundary for tables that belong only to a Slave database."""

    pass
