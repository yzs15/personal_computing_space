from __future__ import annotations

from typing import Any

import httpx

from loom_v2.internal_http import InternalHttpClient

from .repository import ObserverRepository


class ObserverDriverGateway:
    """HTTP gateway from the public Observer API to the active Driver."""

    def __init__(
        self,
        repository: ObserverRepository,
        *,
        workspace_id: str = "workspace-default",
        internal_api_secret: str = "",
        timeout: float = 10.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.repository = repository
        self.workspace_id = workspace_id
        self.internal_api_secret = internal_api_secret
        self.timeout = timeout
        self.transport = transport
        self._http = InternalHttpClient(timeout=timeout, transport=transport)

    async def close(self) -> None:
        await self._http.close()

    async def active_driver(self) -> dict[str, Any] | None:
        agents = await self.repository.list_agents(self.workspace_id, role="driver")
        return next((item for item in agents if item.get("lease_state") == "active"), None)

    async def forward(self, path: str, payload: dict[str, Any], *, extra_headers: dict[str, str] | None = None, timeout: float | None = None) -> dict[str, Any]:
        driver = await self.active_driver()
        if driver is None or not driver.get("endpoint_url"):
            raise RuntimeError("driver_unavailable")
        headers = {"X-Loom-Internal-Token": self.internal_api_secret}
        if self.internal_api_secret:
            headers["Authorization"] = f"Bearer {self.internal_api_secret}"
        if extra_headers:
            headers.update(extra_headers)
        request_timeout = timeout if timeout is not None else self.timeout
        try:
            self._http.transport = self.transport
            response = await self._http.request(
                "POST",
                f"{str(driver['endpoint_url']).rstrip('/')}{path}",
                json=payload,
                headers=headers,
                timeout=request_timeout,
            )
        except (TimeoutError, httpx.TimeoutException, httpx.TransportError) as exc:
            raise RuntimeError("driver_unavailable") from exc
        if response.status_code >= 500:
            raise RuntimeError("driver_unavailable")
        if response.status_code >= 400:
            try:
                detail = response.json().get("detail", "driver_request_failed")
            except Exception:
                detail = "driver_request_failed"
            raise RuntimeError(str(detail))
        try:
            value = response.json()
        except ValueError as exc:
            raise RuntimeError("driver_invalid_response") from exc
        return value if isinstance(value, dict) else {"result": value}
