from fastapi import FastAPI


def create_app() -> FastAPI:
    app = FastAPI(title="Loom v2 Observer")

    @app.get("/healthz")
    async def health() -> dict[str, object]:
        return {"ok": True, "service": "observer"}

    return app
