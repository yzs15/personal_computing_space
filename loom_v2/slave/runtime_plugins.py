"""Runtime-provider plugin host used by the Slave.

The Slave owns authentication, package identity, activation state and result
validation.  Runtime providers are operator-installed processes which only
own runtime-specific work (for example Docker container lifecycle and HTTP
calls).  The first implementation uses HTTP over a private Unix socket so a
provider can be written in any language without importing Slave internals.
"""

from __future__ import annotations

import asyncio
import os
import signal
import tomllib
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, Sequence
from uuid import uuid4

import httpx

from loom_v2.contracts.types import (
    CapabilityDeprovisionCommand,
    CapabilityPackageVersion,
    CapabilityProvisionCommand,
    ComputeBinding,
    HttpServiceEndpoint,
)


PLUGIN_PROTOCOL_VERSION = "loom.runtime-plugin/1"


class RuntimePluginError(RuntimeError):
    """Stable error raised by the host when a provider cannot be used."""

    def __init__(self, code: str, message: str | None = None, *, details: dict[str, Any] | None = None) -> None:
        super().__init__(message or code)
        self.code = code
        self.details = details or {}


@dataclass(frozen=True)
class RuntimePluginSupport:
    package_type: str
    execution_kind: str
    execution_version: str

    def to_mapping(self) -> dict[str, Any]:
        """Canonical wire representation used by the descriptor endpoint."""
        return {
            "package_type": self.package_type,
            "execution": {"kind": self.execution_kind, "version": self.execution_version},
        }

    @classmethod
    def from_mapping(cls, value: Any) -> "RuntimePluginSupport":
        if not isinstance(value, dict):
            raise RuntimePluginError("runtime_plugin_manifest_invalid")
        if set(value) - {"package_type", "execution_kind", "execution_version", "execution"}:
            raise RuntimePluginError("runtime_plugin_manifest_invalid")
        package_type = str(value.get("package_type") or "")
        execution_value = value.get("execution")
        execution = execution_value if isinstance(execution_value, dict) else {}
        if execution_value is not None and (not isinstance(execution_value, dict) or set(execution) - {"kind", "version"}):
            raise RuntimePluginError("runtime_plugin_manifest_invalid")
        execution_kind = str(value.get("execution_kind") or execution.get("kind") or "")
        execution_version = str(value.get("execution_version") or execution.get("version") or "1")
        if value.get("execution_kind") is not None and execution.get("kind") is not None and str(value["execution_kind"]) != str(execution["kind"]):
            raise RuntimePluginError("runtime_plugin_manifest_invalid")
        if value.get("execution_version") is not None and execution.get("version") is not None and str(value["execution_version"]) != str(execution["version"]):
            raise RuntimePluginError("runtime_plugin_manifest_invalid")
        if not package_type or not execution_kind or not execution_version:
            raise RuntimePluginError("runtime_plugin_manifest_invalid")
        return cls(package_type, execution_kind, execution_version)


@dataclass(frozen=True)
class RuntimePluginDescriptor:
    plugin_id: str
    protocol_version: str
    supports: tuple[RuntimePluginSupport, ...]
    runtime_descriptor_ref: str = ""
    runtime_descriptor_digest: str = ""

    @classmethod
    def from_mapping(cls, value: Any) -> "RuntimePluginDescriptor":
        if not isinstance(value, dict):
            raise RuntimePluginError("runtime_plugin_descriptor_invalid")
        if set(value) - {"plugin_id", "protocol_version", "supports", "runtime_descriptor_ref", "runtime_descriptor_digest"}:
            raise RuntimePluginError("runtime_plugin_descriptor_invalid")
        supports = value.get("supports")
        if not isinstance(supports, list) or not supports:
            raise RuntimePluginError("runtime_plugin_descriptor_invalid")
        descriptor = cls(
            plugin_id=str(value.get("plugin_id") or ""),
            protocol_version=str(value.get("protocol_version") or ""),
            supports=tuple(RuntimePluginSupport.from_mapping(item) for item in supports),
            runtime_descriptor_ref=str(value.get("runtime_descriptor_ref") or ""),
            runtime_descriptor_digest=str(value.get("runtime_descriptor_digest") or "").lower(),
        )
        if not descriptor.plugin_id or descriptor.protocol_version != PLUGIN_PROTOCOL_VERSION:
            raise RuntimePluginError("runtime_plugin_descriptor_invalid")
        if len(set(descriptor.supports)) != len(descriptor.supports):
            raise RuntimePluginError("runtime_plugin_descriptor_invalid")
        if descriptor.runtime_descriptor_digest and not re.fullmatch(r"[0-9a-f]{64}", descriptor.runtime_descriptor_digest):
            raise RuntimePluginError("runtime_plugin_descriptor_invalid")
        return descriptor


class RuntimePlugin(Protocol):
    descriptor: RuntimePluginDescriptor

    async def provision(self, package: CapabilityPackageVersion, command: CapabilityProvisionCommand) -> dict[str, Any]: ...

    async def invoke(
        self,
        package: CapabilityPackageVersion,
        endpoint: HttpServiceEndpoint,
        payload: dict[str, Any],
        *,
        activation: dict[str, Any],
        attempt_id: str,
        deadline_seconds: float | None = None,
    ) -> dict[str, Any]: ...

    async def inspect(self, activation: dict[str, Any]) -> dict[str, Any]: ...

    async def deprovision(self, command: CapabilityDeprovisionCommand) -> dict[str, Any]: ...

    async def reconcile(self, activations: Sequence[dict[str, Any]]) -> dict[str, Any]: ...

    async def close(self) -> None: ...


@dataclass(frozen=True)
class _PluginManifest:
    root: Path
    plugin_id: str
    protocol_version: str
    command: tuple[str, ...]
    supports: tuple[RuntimePluginSupport, ...]

    @classmethod
    def load(cls, path: Path) -> "_PluginManifest":
        try:
            raw = tomllib.loads(path.read_text(encoding="utf-8"))
        except (OSError, tomllib.TOMLDecodeError) as exc:
            raise RuntimePluginError("runtime_plugin_manifest_invalid") from exc
        allowed = {"plugin_id", "protocol_version", "command", "supports"}
        if set(raw) - allowed:
            raise RuntimePluginError("runtime_plugin_manifest_invalid")
        plugin_id = str(raw.get("plugin_id") or "")
        protocol_version = str(raw.get("protocol_version") or "")
        command = raw.get("command")
        supports = raw.get("supports")
        if not plugin_id or protocol_version != PLUGIN_PROTOCOL_VERSION or not isinstance(command, list) or not command:
            raise RuntimePluginError("runtime_plugin_manifest_invalid")
        if not isinstance(supports, list) or not supports:
            raise RuntimePluginError("runtime_plugin_manifest_invalid")
        normalized_command: list[str] = []
        for item in command:
            value = str(item)
            if not value or os.path.isabs(value) or ".." in Path(value).parts:
                raise RuntimePluginError("runtime_plugin_manifest_invalid")
            normalized_command.append(value)
        normalized_supports = tuple(RuntimePluginSupport.from_mapping(item) for item in supports)
        if len(set(normalized_supports)) != len(normalized_supports):
            raise RuntimePluginError("runtime_plugin_manifest_invalid")
        root = path.parent.resolve()
        executable = (root / normalized_command[0]).resolve()
        if root not in executable.parents and executable != root:
            raise RuntimePluginError("runtime_plugin_manifest_invalid")
        if not executable.is_file() or not os.access(executable, os.X_OK):
            raise RuntimePluginError("runtime_plugin_unavailable")
        return cls(root, plugin_id, protocol_version, tuple(normalized_command), normalized_supports)


class ProcessRuntimePlugin:
    """HTTP/JSON client and supervisor for one operator-installed process."""

    def __init__(
        self,
        manifest: _PluginManifest,
        *,
        socket_dir: Path,
        startup_timeout: float = 10.0,
        call_timeout: float = 90.0,
    ) -> None:
        self.manifest = manifest
        self.socket_dir = socket_dir
        self.startup_timeout = max(0.1, float(startup_timeout))
        self.call_timeout = max(0.1, float(call_timeout))
        self.socket_path = self.socket_dir / f"{manifest.plugin_id}-{uuid4().hex[:12]}.sock"
        self.process: asyncio.subprocess.Process | None = None
        self._client: httpx.AsyncClient | None = None
        self._restart_lock = asyncio.Lock()
        self._restart_attempt = 0
        self.descriptor = RuntimePluginDescriptor(
            plugin_id=manifest.plugin_id,
            protocol_version=manifest.protocol_version,
            supports=manifest.supports,
        )

    async def start(self) -> None:
        # ``start`` is also used by the bounded crash-restart path.  Tear down
        # any stale transport/process before creating a fresh instance.
        if self.process is not None or self._client is not None:
            await self.close()
        self.socket_dir.mkdir(parents=True, exist_ok=True, mode=0o750)
        with __import__("contextlib").suppress(OSError):
            os.chmod(self.socket_dir, 0o750)
        self.socket_path.unlink(missing_ok=True)
        # Do not inherit the Slave's credentials/database URL into a
        # Docker-capable provider.  Only runtime tuning knobs and identity
        # needed to bind the provider to this Slave cross the process
        # boundary.
        env = {
            "PATH": os.getenv("PATH", ""),
            "PYTHONPATH": os.getenv("PYTHONPATH", ""),
            "LANG": "C.UTF-8",
            "LOOM_RUNTIME_PLUGIN_ID": self.manifest.plugin_id,
            "LOOM_RUNTIME_PLUGIN_SOCKET": str(self.socket_path),
            "LOOM_SERVICE_NAME": os.getenv("LOOM_SERVICE_NAME", ""),
            "LOOM_SLAVE_ID": os.getenv("LOOM_SLAVE_ID", os.getenv("LOOM_SERVICE_NAME", "")),
        }
        env.update({key: value for key, value in os.environ.items() if key.startswith("LOOM_RUNTIME_PLUGIN_") and key not in {"LOOM_RUNTIME_PLUGIN_ID", "LOOM_RUNTIME_PLUGIN_SOCKET"}})
        try:
            command = list(self.manifest.command)
            command[0] = str((self.manifest.root / command[0]).resolve())
            self.process = await asyncio.create_subprocess_exec(
                *command,
                cwd=str(self.manifest.root),
                env=env,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.DEVNULL,
                # The protocol is exclusively on the Unix socket.  Do not
                # pipe stderr without draining it: a noisy operator plugin
                # could otherwise block once the OS pipe buffer fills.
                stderr=asyncio.subprocess.DEVNULL,
                start_new_session=True,
            )
        except OSError as exc:
            raise RuntimePluginError("runtime_plugin_unavailable") from exc
        transport = httpx.AsyncHTTPTransport(uds=str(self.socket_path), retries=0)
        self._client = httpx.AsyncClient(transport=transport, base_url="http://runtime-plugin", timeout=self.call_timeout)
        deadline = asyncio.get_running_loop().time() + self.startup_timeout
        while True:
            if self.process.returncode is not None:
                await self.close()
                raise RuntimePluginError("runtime_plugin_unavailable")
            try:
                response = await self._client.get("/healthz", timeout=min(1.0, self.call_timeout))
                if response.status_code < 400:
                    descriptor_response = await self._client.get("/v1/descriptor", timeout=min(1.0, self.call_timeout))
                    if descriptor_response.status_code < 400:
                        descriptor = RuntimePluginDescriptor.from_mapping(descriptor_response.json())
                        self._validate_descriptor(descriptor)
                        self.descriptor = descriptor
                        self._restart_attempt = 0
                        return
            except (httpx.HTTPError, RuntimePluginError, ValueError):
                pass
            if asyncio.get_running_loop().time() >= deadline:
                await self.close()
                raise RuntimePluginError("runtime_plugin_start_timeout")
            await asyncio.sleep(0.05)

    def _validate_descriptor(self, descriptor: RuntimePluginDescriptor) -> None:
        if descriptor.plugin_id != self.manifest.plugin_id or descriptor.protocol_version != self.manifest.protocol_version:
            raise RuntimePluginError("runtime_plugin_descriptor_mismatch")
        if set(descriptor.supports) != set(self.manifest.supports):
            raise RuntimePluginError("runtime_plugin_descriptor_mismatch")

    async def _request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        if self._client is None or self.process is None or self.process.returncode is not None:
            await self._restart_if_needed()
        if self._client is None or self.process is None or self.process.returncode is not None:
            raise RuntimePluginError("runtime_plugin_unavailable")
        request_id = uuid4().hex
        request_payload = dict(payload or {})
        request_payload["request_id"] = request_id
        try:
            async with asyncio.timeout(self.call_timeout):
                response = await self._client.request(method, path, json=request_payload)
        except (TimeoutError, httpx.TimeoutException) as exc:
            raise RuntimePluginError("runtime_plugin_timeout") from exc
        except httpx.HTTPError as exc:
            raise RuntimePluginError("runtime_plugin_unavailable") from exc
        if len(response.content) > 4 * 1024 * 1024:
            raise RuntimePluginError("runtime_plugin_protocol_error")
        try:
            body = response.json()
        except ValueError as exc:
            raise RuntimePluginError("runtime_plugin_protocol_error") from exc
        if not isinstance(body, dict) or body.get("request_id") != request_id or body.get("ok") is not True:
            error = body.get("error") if isinstance(body, dict) else None
            if isinstance(error, dict):
                raise RuntimePluginError(str(error.get("code") or "runtime_plugin_error"), str(error.get("safe_message") or "runtime plugin failed"), details=dict(error.get("details") or {}))
            raise RuntimePluginError("runtime_plugin_protocol_error")
        result = body.get("result")
        if not isinstance(result, dict):
            raise RuntimePluginError("runtime_plugin_protocol_error")
        return result

    async def _restart_if_needed(self) -> None:
        async with self._restart_lock:
            if self.process is not None and self.process.returncode is None and self._client is not None:
                return
            # A crashed provider is retried with a bounded exponential delay;
            # callers still receive a stable unavailable error if it cannot
            # come back within the startup deadline.
            delay = min(2.0, 0.1 * (2 ** min(self._restart_attempt, 5)))
            self._restart_attempt += 1
            if delay:
                await asyncio.sleep(delay)
            try:
                await self.start()
            except RuntimePluginError:
                return

    async def provision(self, package: CapabilityPackageVersion, command: CapabilityProvisionCommand) -> dict[str, Any]:
        return await self._request(
            "POST",
            "/v1/provision",
            {
                "package": package.model_dump(mode="json"),
                "command": command.model_dump(mode="json"),
                "workspace_id": command.workspace_id,
                "target_slave": command.target_slave,
                "package_version_ref": package.version_ref,
                "package_digest": package.package_digest,
                "deadline_seconds": self.call_timeout,
            },
        )

    async def invoke(
        self,
        package: CapabilityPackageVersion,
        endpoint: HttpServiceEndpoint,
        payload: dict[str, Any],
        *,
        activation: dict[str, Any],
        attempt_id: str,
        deadline_seconds: float | None = None,
    ) -> dict[str, Any]:
        return await self._request(
            "POST",
            "/v1/invoke",
            {
                "package": package.model_dump(mode="json"),
                "endpoint": endpoint.model_dump(mode="json"),
                "payload": payload,
                "activation": activation,
                "workspace_id": activation.get("workspace_id"),
                "target_slave": activation.get("target_slave") or activation.get("slave_id"),
                "package_version_ref": package.version_ref,
                "package_digest": package.package_digest,
                "attempt_id": attempt_id,
                "deadline_seconds": deadline_seconds,
            },
        )

    async def inspect(self, activation: dict[str, Any]) -> dict[str, Any]:
        return await self._request("POST", "/v1/inspect", {"activation": activation})

    async def deprovision(self, command: CapabilityDeprovisionCommand) -> dict[str, Any]:
        return await self._request("POST", "/v1/deprovision", {"command": command.model_dump(mode="json")})

    async def reconcile(self, activations: Sequence[dict[str, Any]]) -> dict[str, Any]:
        return await self._request("POST", "/v1/reconcile", {"activations": list(activations)})

    async def close(self) -> None:
        client, self._client = self._client, None
        if client is not None:
            await client.aclose()
        process, self.process = self.process, None
        if process is not None and process.returncode is None:
            try:
                os.killpg(process.pid, signal.SIGTERM)
                await asyncio.wait_for(process.wait(), timeout=2.0)
            except (ProcessLookupError, TimeoutError, asyncio.TimeoutError):
                with __import__("contextlib").suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGKILL)
        self.socket_path.unlink(missing_ok=True)


@dataclass
class RuntimePluginHost:
    """Route package execution contracts to operator-installed providers."""

    plugin_dir: Path | str | None = None
    socket_dir: Path | str = "/run/loom/runtime-plugins"
    startup_timeout: float = 10.0
    call_timeout: float = 90.0
    plugins: dict[tuple[str, str, str], RuntimePlugin] = field(default_factory=dict)
    _process_plugins: list[ProcessRuntimePlugin] = field(default_factory=list, init=False, repr=False)

    def __post_init__(self) -> None:
        supplied = self.plugins
        self.plugins = {}
        if isinstance(supplied, dict):
            for plugin in supplied.values():
                self.register(plugin)
        else:
            for plugin in supplied:  # type: ignore[union-attr]
                self.register(plugin)

    def register(self, plugin: RuntimePlugin) -> None:
        descriptor = plugin.descriptor
        if not isinstance(descriptor, RuntimePluginDescriptor):
            descriptor = RuntimePluginDescriptor.from_mapping(descriptor)
            plugin.descriptor = descriptor  # type: ignore[attr-defined]
        for support in descriptor.supports:
            key = (support.package_type, support.execution_kind, support.execution_version)
            if key in self.plugins and self.plugins[key] is not plugin:
                raise RuntimePluginError("runtime_plugin_conflict")
            self.plugins[key] = plugin

    def _discover_manifests(self) -> list[_PluginManifest]:
        if self.plugin_dir is None:
            return []
        root = Path(self.plugin_dir)
        if not root.exists():
            return []
        if not root.is_dir():
            raise RuntimePluginError("runtime_plugin_manifest_invalid")
        manifests: list[_PluginManifest] = []
        for child in sorted(root.iterdir(), key=lambda item: item.name):
            if not child.is_dir():
                continue
            manifest_path = child / "plugin.toml"
            if manifest_path.exists():
                manifests.append(_PluginManifest.load(manifest_path))
        return manifests

    async def start(self) -> None:
        if self._process_plugins:
            return
        try:
            for manifest in self._discover_manifests():
                plugin = ProcessRuntimePlugin(
                    manifest,
                    socket_dir=Path(self.socket_dir),
                    startup_timeout=self.startup_timeout,
                    call_timeout=self.call_timeout,
                )
                await plugin.start()
                self.register(plugin)
                self._process_plugins.append(plugin)
        except Exception:
            await self.close()
            raise

    async def close(self) -> None:
        process_plugins = list(self._process_plugins)
        for plugin in reversed(process_plugins):
            await plugin.close()
        self._process_plugins.clear()
        unique = {id(plugin): plugin for plugin in self.plugins.values()}
        for plugin in unique.values():
            if all(plugin is not process_plugin for process_plugin in process_plugins):
                close = getattr(plugin, "close", None)
                if close is not None:
                    result = close()
                    if asyncio.iscoroutine(result):
                        await result
        self.plugins.clear()

    def get(self, package: CapabilityPackageVersion) -> RuntimePlugin:
        key = (package.package_type, package.execution.kind, package.execution.version)
        try:
            return self.plugins[key]
        except KeyError as exc:
            raise RuntimePluginError("runtime_plugin_not_found") from exc

    def descriptors(self) -> list[RuntimePluginDescriptor]:
        unique = {id(plugin): plugin for plugin in self.plugins.values()}
        return [plugin.descriptor for plugin in unique.values()]

    async def provision(self, package: CapabilityPackageVersion, command: CapabilityProvisionCommand) -> dict[str, Any]:
        return await self.get(package).provision(package, command)

    async def invoke(
        self,
        package: CapabilityPackageVersion,
        endpoint: HttpServiceEndpoint,
        payload: dict[str, Any],
        *,
        activation: dict[str, Any],
        attempt_id: str,
        deadline_seconds: float | None = None,
    ) -> dict[str, Any]:
        return await self.get(package).invoke(package, endpoint, payload, activation=activation, attempt_id=attempt_id, deadline_seconds=deadline_seconds)

    async def inspect(self, package: CapabilityPackageVersion, activation: dict[str, Any]) -> dict[str, Any]:
        return await self.get(package).inspect(activation)

    async def deprovision(self, package: CapabilityPackageVersion, command: CapabilityDeprovisionCommand) -> dict[str, Any]:
        return await self.get(package).deprovision(command)

    async def reconcile(self, activations: Sequence[dict[str, Any]]) -> dict[str, Any]:
        grouped: dict[int, tuple[RuntimePlugin, list[dict[str, Any]]]] = {}
        for activation in activations:
            package_type = str(activation.get("package_type") or "")
            execution = activation.get("execution") or {}
            package_key = (package_type, str(execution.get("kind") or ""), str(execution.get("version") or "1"))
            plugin = self.plugins.get(package_key)
            if plugin is not None:
                entry = grouped.get(id(plugin))
                if entry is None:
                    grouped[id(plugin)] = (plugin, [activation])
                else:
                    entry[1].append(activation)
        result: dict[str, Any] = {}
        for plugin, items in grouped.values():
            response = await plugin.reconcile(items)
            result[plugin.descriptor.plugin_id] = response
        return result


__all__ = [
    "PLUGIN_PROTOCOL_VERSION",
    "RuntimePluginError",
    "RuntimePluginSupport",
    "RuntimePluginDescriptor",
    "RuntimePlugin",
    "ProcessRuntimePlugin",
    "RuntimePluginHost",
]
