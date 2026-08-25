import os

from fastapi import FastAPI

from loom_v2.db.session import make_engine
from loom_v2.settings import Settings

from .service import SlaveService


def create_app(slave_id: str | None = None) -> FastAPI:
    slave_id = slave_id or os.getenv("LOOM_SERVICE_NAME", "slave-a")
    app = FastAPI(title=f"Loom v2 {slave_id}")
    settings = Settings()
    app.state.engine = None if settings.database_url.startswith("sqlite+aiosqlite:///:memory:") else make_engine(settings.database_url)
    app.state.service = SlaveService(slave_id=slave_id, engine=app.state.engine)

    @app.on_event("startup")
    async def initialize_database() -> None:
        await app.state.service.init_db()

    @app.on_event("shutdown")
    async def close_database() -> None:
        if app.state.engine is not None:
            await app.state.engine.dispose()

    @app.get("/healthz")
    async def health() -> dict[str, object]:
        service: SlaveService = app.state.service
        return {"ok": service.available, "service": service.slave_id, "replica": service.replica.state}

    return app
