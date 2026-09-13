"""Minimal standalone ``container-http-v1`` runtime plugin process.

The Core talks to this process over the Unix socket named by
``LOOM_RUNTIME_PLUGIN_SOCKET``.  Keeping the adapter in a separate module
means the Slave core only knows the generic plugin protocol; operators may
replace this bundle without rebuilding the core image.
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any

from loom_v2.contracts.types import CapabilityDeprovisionCommand, CapabilityPackageVersion, CapabilityProvisionCommand, HttpServiceEndpoint

from .container_http import DockerContainerHTTPRuntimeV1


MAX_REQUEST_BYTES = 8 * 1024 * 1024


def _response(status: int, body: bytes, content_type: str = "application/json") -> bytes:
    reason = {200: "OK", 400: "Bad Request", 404: "Not Found", 413: "Payload Too Large", 500: "Internal Server Error"}.get(status, "Error")
    return (f"HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n").encode() + body


class PluginServer:
    def __init__(self, runtime: DockerContainerHTTPRuntimeV1) -> None:
        self.runtime = runtime

    async def dispatch(self, method: str, path: str, payload: dict[str, Any] | None) -> tuple[int, dict[str, Any] | bytes, str]:
        if method == "GET" and path == "/healthz":
            return 200, b"ok", "text/plain"
        if method == "GET" and path == "/v1/descriptor":
            descriptor = self.runtime.descriptor
            return 200, {"plugin_id": descriptor.plugin_id, "protocol_version": descriptor.protocol_version, "supports": [support.to_mapping() for support in descriptor.supports], "runtime_descriptor_ref": descriptor.runtime_descriptor_ref, "runtime_descriptor_digest": descriptor.runtime_descriptor_digest}, "application/json"
        if method != "POST" or payload is None:
            return 404, {"error": "not_found"}, "application/json"
        request_id = str(payload.get("request_id") or "")
        try:
            if path == "/v1/provision":
                result = await self.runtime.provision(CapabilityPackageVersion.model_validate(payload["package"]), CapabilityProvisionCommand.model_validate(payload["command"]))
            elif path == "/v1/invoke":
                result = await self.runtime.invoke(CapabilityPackageVersion.model_validate(payload["package"]), HttpServiceEndpoint.model_validate(payload["endpoint"]), dict(payload.get("payload") or {}), activation=dict(payload.get("activation") or {}), attempt_id=str(payload.get("attempt_id") or request_id), deadline_seconds=float(payload["deadline_seconds"]) if payload.get("deadline_seconds") is not None else None)
            elif path == "/v1/inspect":
                result = await self.runtime.inspect(dict(payload.get("activation") or {}))
            elif path == "/v1/deprovision":
                result = await self.runtime.deprovision(CapabilityDeprovisionCommand.model_validate(payload["command"]))
            elif path == "/v1/reconcile":
                result = await self.runtime.reconcile(list(payload.get("activations") or []))
            else:
                return 404, {"error": "not_found"}, "application/json"
            return 200, {"request_id": request_id, "ok": True, "result": result}, "application/json"
        except Exception as exc:
            code = str(exc) or "runtime_plugin_error"
            return 200, {"request_id": request_id, "ok": False, "error": {"code": code, "retryable": code.endswith("timeout") or code in {"service_runtime_unavailable", "runtime_plugin_unavailable"}, "safe_message": code, "details": {}}}, "application/json"

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            header = await reader.readuntil(b"\r\n\r\n")
            if len(header) > 16384:
                writer.write(_response(413, b"request too large", "text/plain")); await writer.drain(); return
            lines = header[:-4].decode("latin-1").split("\r\n")
            method, path, _ = lines[0].split(" ", 2)
            headers = {}
            for line in lines[1:]:
                if ":" in line:
                    key, value = line.split(":", 1); headers[key.lower()] = value.strip()
            try:
                content_length = int(headers.get("content-length", "0"))
            except ValueError:
                content_length = -1
            if content_length < 0 or content_length > MAX_REQUEST_BYTES:
                writer.write(_response(413, b"request too large", "text/plain")); await writer.drain(); return
            body = await reader.readexactly(content_length) if content_length else b""
            if method == "POST" and headers.get("content-type", "").split(";", 1)[0].strip().lower() != "application/json":
                writer.write(_response(400, b"content type required", "text/plain")); await writer.drain(); return
            payload = json.loads(body) if body else None
            if payload is not None and not isinstance(payload, dict):
                raise ValueError("request_json_invalid")
            status, value, content_type = await self.dispatch(method, path, payload)
            encoded = value if isinstance(value, bytes) else json.dumps(value, separators=(",", ":")).encode()
            writer.write(_response(status, encoded, content_type)); await writer.drain()
        except Exception:
            writer.write(_response(400, b"invalid request", "text/plain"))
            with __import__("contextlib").suppress(Exception):
                await writer.drain()
        finally:
            writer.close()
            with __import__("contextlib").suppress(Exception):
                await writer.wait_closed()


async def main() -> None:
    socket_path = Path(os.environ["LOOM_RUNTIME_PLUGIN_SOCKET"])
    socket_path.parent.mkdir(parents=True, exist_ok=True)
    socket_path.unlink(missing_ok=True)
    runtime = DockerContainerHTTPRuntimeV1(
        slave_id=os.getenv("LOOM_SLAVE_ID") or os.getenv("LOOM_SERVICE_NAME") or "slave",
        network=os.getenv("LOOM_RUNTIME_PLUGIN_NETWORK", "loom-internal"),
        memory=os.getenv("LOOM_RUNTIME_PLUGIN_MEMORY", "512m"),
        cpus=float(os.getenv("LOOM_RUNTIME_PLUGIN_CPUS", "1")),
        pids_limit=int(os.getenv("LOOM_RUNTIME_PLUGIN_PIDS_LIMIT", "128")),
        tmpfs_size=os.getenv("LOOM_RUNTIME_PLUGIN_TMPFS_SIZE", "64m"),
        startup_timeout=float(os.getenv("LOOM_RUNTIME_PLUGIN_STARTUP_TIMEOUT", "30")),
        request_timeout=float(os.getenv("LOOM_RUNTIME_PLUGIN_REQUEST_TIMEOUT", "30")),
        response_max_bytes=int(os.getenv("LOOM_RUNTIME_PLUGIN_RESPONSE_MAX_BYTES", str(4 * 1024 * 1024))),
        max_concurrency=int(os.getenv("LOOM_RUNTIME_PLUGIN_MAX_CONCURRENCY", "16")),
        docker_timeout=float(os.getenv("LOOM_RUNTIME_PLUGIN_DOCKER_TIMEOUT", "120")),
    )
    server = PluginServer(runtime)
    listener = await asyncio.start_unix_server(server.handle, path=str(socket_path))
    # Restrict the protocol endpoint to the container's service group.  The
    # host and plugin normally share that group; a permissive world-readable
    # socket would expose package payloads to unrelated local processes.
    with __import__("contextlib").suppress(OSError):
        os.chmod(socket_path, 0o660)
    try:
        async with listener:
            await listener.serve_forever()
    finally:
        socket_path.unlink(missing_ok=True)


if __name__ == "__main__":
    asyncio.run(main())
