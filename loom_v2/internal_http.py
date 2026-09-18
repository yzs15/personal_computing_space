"""Small shared HTTP transport for Loom's internal service clients.

The client deliberately does not decide how an application error should be
mapped.  Observer, Driver and Worker APIs have different error envelopes, so
callers keep that policy while sharing connection reuse, timeout handling and
test transport injection.
"""

from __future__ import annotations

import asyncio
from typing import Any

import httpx


class InternalHttpClient:
    """Reuse one ``httpx.AsyncClient`` for a component lifetime."""

    def __init__(
        self,
        *,
        timeout: float,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.timeout = timeout
        self.transport = transport
        self._client: httpx.AsyncClient | None = None
        self._client_transport: httpx.AsyncBaseTransport | None = None

    async def _get_client(self, timeout: float) -> httpx.AsyncClient:
        # Tests and embedded composition roots commonly replace the transport
        # after construction. Rebuild only when that explicit seam changes.
        if self._client is None or self._client_transport is not self.transport:
            if self._client is not None:
                await self._client.aclose()
            self._client = httpx.AsyncClient(timeout=timeout, transport=self.transport)
            self._client_transport = self.transport
        return self._client

    async def request(
        self,
        method: str,
        url: str,
        *,
        json: Any | None = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> httpx.Response:
        request_timeout = timeout if timeout is not None else self.timeout
        client = await self._get_client(request_timeout)
        async with asyncio.timeout(request_timeout):
            return await client.request(
                method,
                url,
                json=json,
                headers=headers,
                timeout=request_timeout,
            )

    async def close(self) -> None:
        client, self._client = self._client, None
        self._client_transport = None
        if client is not None:
            await client.aclose()


__all__ = ["InternalHttpClient"]
