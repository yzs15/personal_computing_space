"""Docker/HTTP implementation owned by the ``container-http-v1`` bundle.

This module deliberately consumes plain protocol JSON.  Package contract and
digest validation belong to Slave Core; the plugin checks only the execution
contract it implements and the runtime identities needed before Docker or HTTP
operations.  Consequently the bundle neither imports Slave internals nor
maintains a second package-digest implementation.
"""

from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from typing import Any, Protocol

import httpx


IMAGE_RE = re.compile(
    r"^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?(?::[0-9]+)?/"
    r"(?:[a-z0-9]+(?:[._-][a-z0-9]+)*/)*"
    r"[a-z0-9]+(?:[._-][a-z0-9]+)*@sha256:[0-9a-f]{64}$"
)
DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")

RUNTIME_DESCRIPTOR: dict[str, Any] = {
    "plugin_id": "container-http-v1",
    "protocol_version": "loom.runtime-plugin/1",
    "supports": [
        {
            "package_type": "service",
            "execution": {"kind": "container:http", "version": "1"},
        }
    ],
    "runtime_descriptor_ref": "runtime://service/container:http/1",
    "runtime_descriptor_digest": (
        "58a09aa11f2e5beea73c821b780eb38b3f65c33f96e8df54359e2751dde3cf33"
    ),
}


@dataclass(frozen=True)
class DockerCommandResult:
    returncode: int
    stdout: str = ""
    stderr: str = ""


class DockerRunner(Protocol):
    async def run(
        self, args: list[str], *, timeout: float
    ) -> DockerCommandResult: ...


class SubprocessDockerRunner:
    """Run Docker CLI commands without a shell."""

    async def run(
        self, args: list[str], *, timeout: float
    ) -> DockerCommandResult:
        try:
            process = await asyncio.create_subprocess_exec(
                "docker",
                *args,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
            )
        except OSError as exc:
            raise RuntimeError("service_runtime_unavailable") from exc
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(), timeout=timeout
            )
        except asyncio.TimeoutError as exc:
            process.kill()
            await process.wait()
            raise RuntimeError("service_docker_timeout") from exc
        return DockerCommandResult(
            returncode=int(process.returncode or 0),
            stdout=stdout.decode("utf-8", errors="replace"),
            stderr=stderr.decode("utf-8", errors="replace"),
        )


class DockerContainerHTTPRuntimeV1:
    """Runtime provider for ``service + container:http/1``."""

    descriptor = RUNTIME_DESCRIPTOR

    def __init__(
        self,
        *,
        slave_id: str,
        network: str,
        memory: str = "512m",
        cpus: float = 1.0,
        pids_limit: int = 128,
        tmpfs_size: str = "64m",
        startup_timeout: float = 30.0,
        request_timeout: float = 30.0,
        response_max_bytes: int = 4 * 1024 * 1024,
        max_concurrency: int = 16,
        docker_timeout: float = 120.0,
        runner: DockerRunner | None = None,
        http_transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if not slave_id or not network:
            raise ValueError("service_runtime_configuration_missing")
        numeric_values = (
            cpus,
            pids_limit,
            startup_timeout,
            request_timeout,
            response_max_bytes,
            max_concurrency,
            docker_timeout,
        )
        if any(float(value) <= 0 for value in numeric_values):
            raise ValueError("service_runtime_configuration_invalid")
        if not memory or not tmpfs_size:
            raise ValueError("service_runtime_configuration_invalid")
        self.slave_id = slave_id
        self.network = network
        self.memory = memory
        self.cpus = float(cpus)
        self.pids_limit = int(pids_limit)
        self.tmpfs_size = tmpfs_size
        self.startup_timeout = float(startup_timeout)
        self.request_timeout = float(request_timeout)
        self.response_max_bytes = int(response_max_bytes)
        self.max_concurrency = int(max_concurrency)
        self.docker_timeout = float(docker_timeout)
        self.runner = runner or SubprocessDockerRunner()
        self.http_transport = http_transport
        self._semaphores: dict[str, asyncio.Semaphore] = {}

    def _container_name(self, workspace_id: str, package_digest: str) -> str:
        safe_workspace = re.sub(
            r"[^a-z0-9_.-]", "-", workspace_id.lower()
        ).strip("-")[:40] or "workspace"
        safe_slave = re.sub(
            r"[^a-z0-9_.-]", "-", self.slave_id.lower()
        ).strip("-")[:24] or "slave"
        return f"loom-cap-{safe_workspace}-{safe_slave}-{package_digest[:16]}"

    @staticmethod
    def _execution_identity(package: dict[str, Any]) -> tuple[str, str]:
        execution = package.get("execution")
        if (
            package.get("package_type") != "service"
            or not isinstance(execution, dict)
            or execution.get("kind") != "container:http"
            or str(execution.get("version") or "") != "1"
        ):
            raise RuntimeError("runtime_plugin_capability_mismatch")
        package_id = package.get("package_id")
        package_version = package.get("package_version")
        digest = str(package.get("package_digest") or "").lower()
        if (
            not isinstance(package_id, str)
            or not package_id
            or not isinstance(package_version, str)
            or not package_version
            or not DIGEST_RE.fullmatch(digest)
        ):
            raise RuntimeError("runtime_plugin_identity_mismatch")
        return f"capability-package://{package_id}/{package_version}", digest

    @classmethod
    def _service_values(
        cls, package: dict[str, Any]
    ) -> tuple[str, int, str, list[dict[str, Any]]]:
        cls._execution_identity(package)
        body = package.get("body")
        exports = package.get("capability_exports")
        if not isinstance(body, dict) or not isinstance(exports, list) or not exports:
            raise RuntimeError("runtime_plugin_capability_mismatch")
        image_ref = body.get("image_ref")
        container_port = body.get("container_port")
        health_path = body.get("health_path")
        if (
            not isinstance(image_ref, str)
            or not IMAGE_RE.fullmatch(image_ref)
            or isinstance(container_port, bool)
            or not isinstance(container_port, int)
            or not 1 <= container_port <= 65535
            or not isinstance(health_path, str)
        ):
            raise RuntimeError("runtime_plugin_capability_mismatch")
        normalized_exports: list[dict[str, Any]] = []
        paths: list[str] = []
        for capability_export in exports:
            if not isinstance(capability_export, dict):
                raise RuntimeError("runtime_plugin_capability_mismatch")
            runtime_binding = capability_export.get("runtime_binding")
            path = (
                runtime_binding.get("path")
                if isinstance(runtime_binding, dict)
                else None
            )
            if not isinstance(path, str):
                raise RuntimeError("runtime_plugin_capability_mismatch")
            normalized_exports.append(capability_export)
            paths.append(path)
        if len(paths) != len(set(paths)):
            raise RuntimeError("service_endpoint_duplicate")
        if health_path in paths:
            raise RuntimeError("service_health_endpoint_conflict")
        return image_ref, container_port, health_path, normalized_exports

    def _command_context(
        self, package: dict[str, Any], command: dict[str, Any]
    ) -> tuple[str, str, str]:
        package_ref, package_digest = self._execution_identity(package)
        workspace_id = command.get("workspace_id")
        if (
            not isinstance(workspace_id, str)
            or not workspace_id
            or command.get("target_slave") != self.slave_id
            or command.get("package_version_ref") != package_ref
            or str(command.get("package_digest") or "").lower() != package_digest
        ):
            raise RuntimeError("runtime_plugin_identity_mismatch")
        return workspace_id, package_ref, package_digest

    def _labels(
        self, workspace_id: str, package_ref: str, package_digest: str
    ) -> dict[str, str]:
        # The package digest is the immutable execution identity.  A promoted
        # reusable coordinate intentionally keeps the same digest as its
        # run-bound candidate, so the lifecycle coordinate must not fence the
        # Docker container itself.
        return {
            "io.loom.managed": "container-http-v1",
            "io.loom.workspace": workspace_id,
            "io.loom.slave": self.slave_id,
            "io.loom.package-digest": package_digest,
        }

    async def _docker(self, args: list[str]) -> DockerCommandResult:
        result = await self.runner.run(args, timeout=self.docker_timeout)
        if isinstance(result, DockerCommandResult):
            return result
        if isinstance(result, tuple):
            return DockerCommandResult(
                int(result[0]),
                str(result[1] if len(result) > 1 else ""),
                str(result[2] if len(result) > 2 else ""),
            )
        return DockerCommandResult(
            int(getattr(result, "returncode", 0)),
            str(getattr(result, "stdout", "")),
            str(getattr(result, "stderr", "")),
        )

    async def _image_ready(self, image_ref: str) -> None:
        inspect = await self._docker(
            ["image", "inspect", image_ref, "--format", "{{json .RepoDigests}}"]
        )
        if inspect.returncode != 0:
            pulled = await self._docker(["pull", image_ref])
            if pulled.returncode != 0:
                raise RuntimeError("service_image_unavailable")
            inspect = await self._docker(
                [
                    "image",
                    "inspect",
                    image_ref,
                    "--format",
                    "{{json .RepoDigests}}",
                ]
            )
        if inspect.returncode != 0:
            raise RuntimeError("service_image_digest_mismatch")
        try:
            repo_digests = json.loads(inspect.stdout or "[]")
        except json.JSONDecodeError as exc:
            raise RuntimeError("service_image_digest_mismatch") from exc
        if (
            not isinstance(repo_digests, list)
            or image_ref not in {str(item) for item in repo_digests}
        ):
            raise RuntimeError("service_image_digest_mismatch")

    async def _inspect_container(self, name: str) -> dict[str, Any] | None:
        result = await self._docker(["inspect", name])
        if result.returncode != 0:
            return None
        try:
            payload = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError("service_container_inspect_failed") from exc
        if not isinstance(payload, list) or not payload or not isinstance(payload[0], dict):
            raise RuntimeError("service_container_inspect_failed")
        return payload[0]

    async def _remove_container(self, name: str) -> None:
        result = await self._docker(["rm", "--force", name])
        if result.returncode != 0 and "No such container" not in result.stderr:
            raise RuntimeError("service_container_remove_failed")

    @staticmethod
    def _labels_match(
        container: dict[str, Any], labels: dict[str, str]
    ) -> bool:
        actual = (
            container.get("Config", {}).get("Labels")
            or container.get("Labels")
            or {}
        )
        return isinstance(actual, dict) and all(
            str(actual.get(key, "")) == value for key, value in labels.items()
        )

    async def _matching_container(
        self,
        container: dict[str, Any],
        *,
        labels: dict[str, str],
        image_ref: str,
    ) -> bool:
        if not self._labels_match(container, labels):
            return False
        refs = {
            str(item)
            for item in (
                container.get("RepoDigests")
                or container.get("ImageDigest")
                or []
            )
        }
        configured = str((container.get("Config") or {}).get("Image") or "")
        actual = str(container.get("Image") or "")
        if image_ref in refs or configured == image_ref or actual == image_ref:
            return True
        if not actual:
            return False
        inspected = await self._docker(
            ["image", "inspect", image_ref, "--format", "{{.Id}}"]
        )
        return inspected.returncode == 0 and inspected.stdout.strip() == actual

    async def _health(self, container_name: str, port: int, path: str) -> bool:
        try:
            async with httpx.AsyncClient(
                transport=self.http_transport,
                timeout=self.request_timeout,
                follow_redirects=False,
            ) as client:
                async with client.stream(
                    "GET", f"http://{container_name}:{port}{path}"
                ) as response:
                    return 200 <= response.status_code < 300
        except (httpx.HTTPError, TimeoutError):
            return False

    async def provision(
        self, package: dict[str, Any], command: dict[str, Any]
    ) -> dict[str, Any]:
        image_ref, container_port, health_path, _exports = self._service_values(
            package
        )
        workspace_id, package_ref, package_digest = self._command_context(
            package, command
        )
        # All cross-export path checks above happen before the first Docker
        # read or mutation.
        await self._image_ready(image_ref)
        container_name = self._container_name(workspace_id, package_digest)
        labels = self._labels(workspace_id, package_ref, package_digest)
        existing = await self._inspect_container(container_name)
        if existing is not None and not await self._matching_container(
            existing, labels=labels, image_ref=image_ref
        ):
            actual_labels = (
                existing.get("Config", {}).get("Labels")
                or existing.get("Labels")
                or {}
            )
            owned_identity = isinstance(actual_labels, dict) and all(
                str(actual_labels.get(key, "")) == value
                for key, value in {
                    "io.loom.managed": "container-http-v1",
                    "io.loom.workspace": workspace_id,
                    "io.loom.slave": self.slave_id,
                    "io.loom.package-digest": package_digest,
                }.items()
            )
            if not owned_identity:
                raise RuntimeError("service_container_identity_conflict")
            await self._remove_container(container_name)
            existing = None

        idempotent = False
        if existing is None:
            args = [
                "create",
                "--name",
                container_name,
                "--pull=never",
                "--restart",
                "unless-stopped",
                "--network",
                self.network,
                "--read-only",
                "--user",
                "65534:65534",
                "--cap-drop",
                "ALL",
                "--security-opt",
                "no-new-privileges",
                "--pids-limit",
                str(self.pids_limit),
                "--memory",
                self.memory,
                "--cpus",
                str(self.cpus),
                "--tmpfs",
                f"/tmp:rw,noexec,nosuid,size={self.tmpfs_size}",
            ]
            for key, value in labels.items():
                args.extend(["--label", f"{key}={value}"])
            args.append(image_ref)
            created = await self._docker(args)
            if created.returncode != 0:
                raise RuntimeError("service_container_start_failed")
            started = await self._docker(["start", container_name])
            if started.returncode != 0:
                await self._remove_container(container_name)
                raise RuntimeError("service_container_start_failed")
        else:
            state = existing.get("State") or {}
            if bool(state.get("Running")):
                idempotent = True
            else:
                started = await self._docker(["start", container_name])
                if started.returncode != 0:
                    raise RuntimeError("service_container_start_failed")

        deadline = asyncio.get_running_loop().time() + self.startup_timeout
        while asyncio.get_running_loop().time() < deadline:
            if await self._health(container_name, container_port, health_path):
                return {
                    "activation_state": "ready",
                    "runtime_plugin_id": "container-http-v1",
                    "runtime_profile": {
                        "container_name": container_name,
                        "container_port": container_port,
                    },
                    "evidence_refs": [
                        f"container:{container_name}",
                        f"health:{package_digest[:16]}",
                    ],
                    "details": {"container_name": container_name},
                    "idempotent": idempotent,
                }
            await asyncio.sleep(0.05)
        return {
            "activation_state": "failed",
            "runtime_plugin_id": "container-http-v1",
            "runtime_profile": {"container_name": container_name},
            "details": {"code": "service_health_check_failed"},
            "idempotent": idempotent,
        }

    async def invoke(
        self,
        package: dict[str, Any],
        capability_export: dict[str, Any],
        payload: dict[str, Any],
        *,
        activation: dict[str, Any],
        attempt_id: str,
        deadline_seconds: float | None = None,
    ) -> dict[str, Any]:
        image_ref, container_port, _health_path, exports = self._service_values(
            package
        )
        package_ref, package_digest = self._execution_identity(package)
        workspace_id = activation.get("workspace_id")
        target_slave = activation.get("target_slave") or activation.get("slave_id")
        if (
            not isinstance(workspace_id, str)
            or not workspace_id
            or target_slave != self.slave_id
            or activation.get("package_version_ref") != package_ref
            or str(activation.get("package_digest") or "").lower()
            != package_digest
        ):
            raise RuntimeError("runtime_plugin_identity_mismatch")
        if capability_export not in exports:
            raise RuntimeError("service_endpoint_binding_mismatch")
        runtime_binding = capability_export.get("runtime_binding") or {}
        path = str(runtime_binding.get("path") or "")
        expected_name = self._container_name(workspace_id, package_digest)
        profile = activation.get("runtime_profile") or {}
        container_name = str(profile.get("container_name") or expected_name)
        if container_name != expected_name:
            raise RuntimeError("service_container_identity_conflict")
        container = await self._inspect_container(container_name)
        labels = self._labels(workspace_id, package_ref, package_digest)
        if container is None or not await self._matching_container(
            container, labels=labels, image_ref=image_ref
        ):
            raise RuntimeError("service_container_identity_conflict")
        if not bool((container.get("State") or {}).get("Running")):
            raise RuntimeError("service_activation_not_ready")

        capacity_timeout = (
            min(self.request_timeout, float(deadline_seconds))
            if deadline_seconds is not None
            else self.request_timeout
        )
        if capacity_timeout <= 0:
            raise RuntimeError("capability_service_busy")
        semaphore = self._semaphores.setdefault(
            package_digest, asyncio.Semaphore(self.max_concurrency)
        )
        try:
            await asyncio.wait_for(semaphore.acquire(), timeout=capacity_timeout)
        except asyncio.TimeoutError as exc:
            raise RuntimeError("capability_service_busy") from exc
        try:
            try:
                async with httpx.AsyncClient(
                    transport=self.http_transport,
                    timeout=capacity_timeout,
                    follow_redirects=False,
                ) as client:
                    async with client.stream(
                        "POST",
                        f"http://{container_name}:{container_port}{path}",
                        json=payload,
                        headers={
                            "Content-Type": "application/json",
                            "Accept": "application/json",
                            "X-Loom-Attempt-Id": attempt_id,
                        },
                    ) as response:
                        if not 200 <= response.status_code < 300:
                            raise RuntimeError("capability_service_http_error")
                        body = bytearray()
                        async for chunk in response.aiter_bytes():
                            body.extend(chunk)
                            if len(body) > self.response_max_bytes:
                                raise RuntimeError(
                                    "capability_service_response_too_large"
                                )
            except (httpx.TimeoutException, TimeoutError) as exc:
                raise RuntimeError("capability_service_timeout") from exc
            except httpx.HTTPError as exc:
                raise RuntimeError("capability_service_http_error") from exc
            try:
                value = json.loads(body)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise RuntimeError("capability_service_invalid_json") from exc
            return {
                "value": value,
                "replay_safety": str(capability_export.get("replay_safety") or ""),
                "runtime_plugin_id": "container-http-v1",
            }
        finally:
            semaphore.release()

    async def inspect(self, activation: dict[str, Any]) -> dict[str, Any]:
        package = activation.get("package_payload") or activation.get("package")
        if not isinstance(package, dict):
            raise RuntimeError("runtime_plugin_identity_mismatch")
        image_ref, port, health_path, _exports = self._service_values(package)
        package_ref, package_digest = self._execution_identity(package)
        workspace_id = activation.get("workspace_id")
        target_slave = activation.get("target_slave") or activation.get("slave_id")
        if (
            not isinstance(workspace_id, str)
            or not workspace_id
            or target_slave != self.slave_id
            or activation.get("package_version_ref") != package_ref
            or str(activation.get("package_digest") or "").lower()
            != package_digest
        ):
            raise RuntimeError("runtime_plugin_identity_mismatch")
        expected_name = self._container_name(workspace_id, package_digest)
        profile = activation.get("runtime_profile") or {}
        name = str(profile.get("container_name") or expected_name)
        if name != expected_name:
            raise RuntimeError("service_container_identity_conflict")
        container = await self._inspect_container(name)
        if container is None:
            return {
                "exists": False,
                "running": False,
                "identity_match": False,
                "healthy": False,
            }
        identity_match = await self._matching_container(
            container,
            labels=self._labels(workspace_id, package_ref, package_digest),
            image_ref=image_ref,
        )
        running = bool((container.get("State") or {}).get("Running"))
        healthy = bool(
            identity_match
            and running
            and await self._health(name, port, health_path)
        )
        return {
            "exists": True,
            "running": running,
            "identity_match": identity_match,
            "healthy": healthy,
        }

    async def deprovision(self, command: dict[str, Any]) -> dict[str, Any]:
        workspace_id = command.get("workspace_id")
        package_ref = command.get("package_version_ref")
        package_digest = str(command.get("package_digest") or "").lower()
        if (
            not isinstance(workspace_id, str)
            or not workspace_id
            or not isinstance(package_ref, str)
            or not package_ref.startswith("capability-package://")
            or not DIGEST_RE.fullmatch(package_digest)
            or command.get("target_slave") != self.slave_id
        ):
            raise RuntimeError("runtime_plugin_identity_mismatch")
        name = self._container_name(workspace_id, package_digest)
        existing = await self._inspect_container(name)
        if existing is None:
            return {
                "activation_state": "stopped",
                "runtime_plugin_id": "container-http-v1",
                "idempotent": True,
            }
        if not self._labels_match(
            existing, self._labels(workspace_id, package_ref, package_digest)
        ):
            raise RuntimeError("service_container_identity_conflict")
        await self._remove_container(name)
        return {
            "activation_state": "stopped",
            "runtime_plugin_id": "container-http-v1",
            "idempotent": False,
        }

    async def reconcile(
        self, activations: list[dict[str, Any]]
    ) -> dict[str, Any]:
        results: list[dict[str, Any]] = []
        for activation in activations:
            activation_key = activation.get("activation_key")
            package = activation.get("package_payload") or activation.get("package")
            try:
                if not isinstance(package, dict):
                    raise RuntimeError("runtime_plugin_identity_mismatch")
                package_ref, package_digest = self._execution_identity(package)
                workspace_id = activation.get("workspace_id")
                target_slave = activation.get("target_slave") or activation.get(
                    "slave_id"
                )
                if (
                    not isinstance(workspace_id, str)
                    or not workspace_id
                    or target_slave != self.slave_id
                    or activation.get("package_version_ref") != package_ref
                    or str(activation.get("package_digest") or "").lower()
                    != package_digest
                ):
                    raise RuntimeError("runtime_plugin_identity_mismatch")
                desired_state = str(activation.get("desired_state") or "running")
                if desired_state == "stopped":
                    expected_name = self._container_name(
                        workspace_id, package_digest
                    )
                    profile = activation.get("runtime_profile") or {}
                    name = str(profile.get("container_name") or expected_name)
                    if name != expected_name:
                        raise RuntimeError("service_container_identity_conflict")
                    existing = await self._inspect_container(name)
                    if existing is not None:
                        labels = self._labels(
                            workspace_id, package_ref, package_digest
                        )
                        if not self._labels_match(existing, labels):
                            raise RuntimeError(
                                "service_container_identity_conflict"
                            )
                        await self._remove_container(name)
                    results.append(
                        {
                            "activation_key": activation_key,
                            "activation_state": "stopped",
                            "runtime_plugin_id": "container-http-v1",
                        }
                    )
                    continue

                inspected = await self.inspect(activation)
                if inspected.get("identity_match") and inspected.get("healthy"):
                    results.append(
                        {
                            "activation_key": activation_key,
                            "inspect": inspected,
                            "activation_state": "ready",
                            "runtime_plugin_id": "container-http-v1",
                        }
                    )
                    continue
                if inspected.get("exists") and not inspected.get("identity_match"):
                    raise RuntimeError("service_container_identity_conflict")
                command = {
                    "command_id": f"reconcile-{activation_key or package_digest}",
                    "package_version_ref": package_ref,
                    "package_digest": package_digest,
                    "target_slave": self.slave_id,
                    "workspace_id": workspace_id,
                    "idempotency_key": f"reconcile-{package_digest}",
                }
                provisioned = await self.provision(package, command)
                results.append({"activation_key": activation_key, **provisioned})
            except Exception as exc:
                results.append(
                    {
                        "activation_key": activation_key,
                        "activation_state": "failed",
                        "runtime_plugin_id": "container-http-v1",
                        "details": {
                            "code": str(exc) or "runtime_plugin_error"
                        },
                    }
                )
        return {"activations": results}

    async def close(self) -> None:
        self._semaphores.clear()


__all__ = [
    "DockerCommandResult",
    "DockerRunner",
    "SubprocessDockerRunner",
    "DockerContainerHTTPRuntimeV1",
    "RUNTIME_DESCRIPTOR",
]
