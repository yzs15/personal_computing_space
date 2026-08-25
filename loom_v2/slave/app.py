from fastapi import FastAPI

from .service import SlaveService


def create_app(slave_id: str = "slave-a") -> FastAPI:
    app = FastAPI(title=f"Loom v2 {slave_id}")
    app.state.service = SlaveService(slave_id=slave_id)

    @app.get("/healthz")
    async def health() -> dict[str, object]:
        service: SlaveService = app.state.service
        return {"ok": service.available, "service": service.slave_id, "replica": service.replica.state}

    return app
