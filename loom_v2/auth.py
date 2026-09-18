"""Shared authentication for internal Loom HTTP routes."""

from __future__ import annotations

import hmac

from fastapi import HTTPException, Request


class InternalAuth:
    """Callable FastAPI dependency and explicit route guard."""

    def __init__(self, secret: str = "") -> None:
        self.secret = secret.strip()

    def __call__(self, request: Request) -> None:
        if not self.secret:
            return
        provided = request.headers.get("x-loom-internal-token", "")
        if not provided:
            authorization = request.headers.get("authorization", "")
            if authorization.lower().startswith("bearer "):
                provided = authorization[7:]
        if not hmac.compare_digest(provided, self.secret):
            raise HTTPException(status_code=401, detail="invalid_internal_token")


__all__ = ["InternalAuth"]
