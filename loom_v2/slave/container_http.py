"""The operator-installed Docker/HTTP runtime plugin.

This module intentionally knows nothing about the Slave HTTP API.  It is a
small provider used through :mod:`loom_v2.slave.runtime_plugins`; tests can
inject a recording Docker runner and an httpx transport without requiring a
Docker daemon.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
from dataclasses import dataclass
from typing import Any, Protocol

import httpx

from loom_v2.contracts.types import (
    CapabilityDeprovisionCommand,
    CapabilityPackageVersion,
    CapabilityProvisionCommand,
    HttpServiceEndpoint,
)
from loom_v2.digest import digest_json

from .runtime_plugins import RuntimePluginDescriptor, RuntimePluginSupport


_IMAGE_RE = re.compile(r"^[^@/]+(?:/[^@/]+)*/[^@/]+@sha256:[0-9a-f]{64}$")


@dataclass(frozen=True)
class DockerCommandResult:
    returncode: int
    stdout: str = ""
    stderr: str = ""


class DockerRunner(Protocol):
    async def run(self, args: list[str], *, timeout: float) -> DockerCommandResult: ...


class SubprocessDockerRunner:
    """Run the Docker CLI without a shell."""

    async def run(self, args: list[str], *, timeout: float) -> DockerCommandResult:
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
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)
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

    descriptor = RuntimePluginDescriptor(
        plugin_id="container-http-v1",
        protocol_version="loom.runtime-plugin/1",
        supports=(RuntimePluginSupport("service", "container:http", "1"),),
        runtime_descriptor_ref="runtime://service/container:http/1",
        runtime_descriptor_digest=digest_json({"runtime_descriptor_ref": "runtime://service/container:http/1", "supports": [{"package_type": "service", "execution_kind": "container:http", "execution_version": "1"}]}, domain="loom/runtime-descriptor/v1"),
    )

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
        for value in (startup_timeout, request_timeout, response_max_bytes, max_concurrency, docker_timeout):
            if float(value) <= 0:
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

    @staticmethod
    def _key(workspace_id: str, package_digest: str, target_slave: str) -> str:
        return f"{workspace_id}:{target_slave}:{package_digest.lower()}"

    def _container_name(self, workspace_id: str, package_digest: str) -> str:
        safe_workspace = re.sub(r"[^a-z0-9_.-]", "-", workspace_id.lower()).strip("-")[:40] or "workspace"
        safe_slave = re.sub(r"[^a-z0-9_.-]", "-", self.slave_id.lower()).strip("-")[:24] or "slave"
        return f"loom-cap-{safe_workspace}-{safe_slave}-{package_digest[:16]}"

    async def _docker(self, args: list[str]) -> DockerCommandResult:
        result = await self.runner.run(args, timeout=self.docker_timeout)
        if not isinstance(result, DockerCommandResult):
            if isinstance(result, tuple):
                result = DockerCommandResult(int(result[0]), str(result[1] if len(result) > 1 else ""), str(result[2] if len(result) > 2 else ""))
            else:
                result = DockerCommandResult(int(getattr(result, "returncode", 0)), str(getattr(result, "stdout", "")), str(getattr(result, "stderr", "")))
        return result

    async def _image_ready(self, image_ref: str) -> None:
        if not _IMAGE_RE.fullmatch(image_ref):
            raise RuntimeError("service_image_ref_invalid")
        inspect = await self._docker(["image", "inspect", image_ref, "--format", "{{json .RepoDigests}}"])
        if inspect.returncode != 0:
            pulled = await self._docker(["pull", image_ref])
            if pulled.returncode != 0:
                raise RuntimeError("service_image_unavailable")
            inspect = await self._docker(["image", "inspect", image_ref, "--format", "{{json .RepoDigests}}"])
        if inspect.returncode != 0:
            raise RuntimeError("service_image_digest_mismatch")
        try:
            repo_digests = json.loads(inspect.stdout or "[]")
        except json.JSONDecodeError as exc:
            raise RuntimeError("service_image_digest_mismatch") from exc
        if not isinstance(repo_digests, list) or image_ref not in {str(item) for item in repo_digests}:
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

    async def _remove_container(self, name: str, *, force: bool = True) -> None:
        args = ["rm"]
        if force:
            args.append("--force")
        args.append(name)
        result = await self._docker(args)
        if result.returncode != 0 and "No such container" not in result.stderr:
            raise RuntimeError("service_container_remove_failed")

    @staticmethod
    def _matching_container(container: dict[str, Any], *, labels: dict[str, str], image_ref: str) -> bool:
        actual_labels = container.get("Config", {}).get("Labels") or container.get("Labels") or {}
        if not isinstance(actual_labels, dict):
            return False
        if not all(str(actual_labels.get(key, "")) == value for key, value in labels.items()):
            return False
        # ``docker inspect <container>`` commonly exposes an image ID in
        # ``Image`` and the original reference in ``Config.Image``; it does
        # not reliably include RepoDigests.  Accept an exact configured ref or
        # an explicit digest list here.  Provision performs a stronger image
        # ID comparison when only an ID is available.
        refs = {str(item) for item in (container.get("RepoDigests") or container.get("ImageDigest") or [])}
        configured = str((container.get("Config") or {}).get("Image") or "")
        actual = str(container.get("Image") or "")
        return image_ref in refs or configured == image_ref or actual == image_ref

    async def _matching_container_async(self, container: dict[str, Any], *, labels: dict[str, str], image_ref: str) -> bool:
        if not self._matching_container(container, labels=labels, image_ref=image_ref):
            actual_labels = container.get("Config", {}).get("Labels") or container.get("Labels") or {}
            if not isinstance(actual_labels, dict) or not all(str(actual_labels.get(key, "")) == value for key, value in labels.items()):
                return False
            # Compare the image ID returned by the daemon with the immutable
            # image reference after resolving the reference through
            # ``docker image inspect``.
            image_id = str(container.get("Image") or "")
            if not image_id:
                return False
            inspected = await self._docker(["image", "inspect", image_ref, "--format", "{{.Id}}"])
            return inspected.returncode == 0 and inspected.stdout.strip() == image_id
        return True

    async def _health(self, container_name: str, port: int, path: str) -> bool:
        try:
            async with httpx.AsyncClient(
                transport=self.http_transport,
                timeout=self.request_timeout,
                follow_redirects=False,
            ) as client:
                response = await client.get(f"http://{container_name}:{port}{path}")
            return 200 <= response.status_code < 300
        except (httpx.HTTPError, TimeoutError):
            return False

    async def provision(self, package: CapabilityPackageVersion, command: CapabilityProvisionCommand) -> dict[str, Any]:
        if package.package_type != "service" or package.execution.kind != "container:http" or package.execution.version != "1":
            raise RuntimeError("runtime_plugin_capability_mismatch")
        body = package.body
        image_ref = body.image_ref
        await self._image_ready(image_ref)
        container_name = self._container_name(command.workspace_id, package.package_digest)
        labels = {
            "io.loom.managed": "container-http-v1",
            "io.loom.workspace": command.workspace_id,
            "io.loom.slave": self.slave_id,
            "io.loom.package-ref": package.version_ref,
            "io.loom.package-digest": package.package_digest,
        }
        existing = await self._inspect_container(container_name)
        if existing is not None:
            labels_match = self._matching_container(existing, labels=labels, image_ref=image_ref)
            if not labels_match:
                actual_labels = existing.get("Config", {}).get("Labels") or existing.get("Labels") or {}
                managed_identity = isinstance(actual_labels, dict) and all(
                    str(actual_labels.get(key, "")) == value
                    for key, value in {
                        "io.loom.managed": "container-http-v1",
                        "io.loom.workspace": command.workspace_id,
                        "io.loom.slave": self.slave_id,
                        "io.loom.package-digest": package.package_digest,
                    }.items()
                )
                if managed_identity and await self._matching_container_async(existing, labels=labels, image_ref=image_ref) is False:
                    await self._remove_container(container_name)
                    existing = None
                elif not managed_identity:
                    raise RuntimeError("service_container_identity_conflict")
                else:
                    raise RuntimeError("service_container_identity_conflict")
        idempotent = False
        if existing is None:
            args = [
                "create",
                "--name", container_name,
                "--pull=never",
                "--restart", "unless-stopped",
                "--network", self.network,
                "--read-only",
                "--user", "65534:65534",
                "--cap-drop", "ALL",
                "--security-opt", "no-new-privileges",
                "--pids-limit", str(self.pids_limit),
                "--memory", self.memory,
                "--cpus", str(self.cpus),
                "--tmpfs", f"/tmp:rw,noexec,nosuid,size={self.tmpfs_size}",
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
        healthy = False
        while asyncio.get_running_loop().time() < deadline:
            healthy = await self._health(container_name, body.container_port, body.health_path)
            if healthy:
                break
            await asyncio.sleep(0.05)
        if not healthy:
            return {
                "activation_state": "failed",
                "runtime_plugin_id": self.descriptor.plugin_id,
                "runtime_profile": {"container_name": container_name},
                "details": {"code": "service_health_check_failed"},
                "idempotent": idempotent,
            }
        return {
            "activation_state": "ready",
            "runtime_plugin_id": self.descriptor.plugin_id,
            "runtime_profile": {"container_name": container_name, "container_port": body.container_port},
            "evidence_refs": [f"container:{container_name}", f"health:{package.package_digest[:16]}"],
            "details": {"container_name": container_name},
            "idempotent": idempotent,
        }

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
        body = package.body
        container_name = str((activation.get("runtime_profile") or {}).get("container_name") or self._container_name(str(activation.get("workspace_id") or "workspace-default"), package.package_digest))
        container = await self._inspect_container(container_name)
        expected_labels = {
            "io.loom.managed": "container-http-v1",
            "io.loom.workspace": str(activation.get("workspace_id") or "workspace-default"),
            "io.loom.slave": self.slave_id,
            "io.loom.package-ref": package.version_ref,
            "io.loom.package-digest": package.package_digest,
        }
        if container is None or not await self._matching_container_async(container, labels=expected_labels, image_ref=body.image_ref):
            raise RuntimeError("service_container_identity_conflict")
        if not bool((container.get("State") or {}).get("Running")):
            raise RuntimeError("service_activation_not_ready")
        semaphore = self._semaphores.setdefault(str(activation.get("package_digest") or package.package_digest), asyncio.Semaphore(self.max_concurrency))
        try:
            await asyncio.wait_for(semaphore.acquire(), timeout=deadline_seconds or self.request_timeout)
        except asyncio.TimeoutError as exc:
            raise RuntimeError("capability_service_busy") from exc
        try:
            timeout = min(self.request_timeout, float(deadline_seconds)) if deadline_seconds is not None else self.request_timeout
            async with httpx.AsyncClient(transport=self.http_transport, timeout=timeout, follow_redirects=False) as client:
                try:
                    response = await client.post(
                        f"http://{container_name}:{body.container_port}{endpoint.path}",
                        json=payload,
                        headers={"Content-Type": "application/json", "Accept": "application/json", "X-Loom-Attempt-Id": attempt_id},
                    )
                except (httpx.TimeoutException, TimeoutError) as exc:
                    raise RuntimeError("capability_service_timeout") from exc
                except httpx.HTTPError as exc:
                    raise RuntimeError("capability_service_http_error") from exc
            if response.status_code < 200 or response.status_code >= 300:
                raise RuntimeError("capability_service_http_error")
            if len(response.content) > self.response_max_bytes:
                raise RuntimeError("capability_service_response_too_large")
            try:
                value = response.json()
            except ValueError as exc:
                raise RuntimeError("capability_service_invalid_json") from exc
            return {"value": value, "replay_safety": endpoint.replay_safety, "runtime_plugin_id": self.descriptor.plugin_id}
        finally:
            semaphore.release()

    async def inspect(self, activation: dict[str, Any]) -> dict[str, Any]:
        profile = activation.get("runtime_profile") or {}
        name = str(profile.get("container_name") or "")
        if not name:
            return {"exists": False, "healthy": False}
        container = await self._inspect_container(name)
        return {"exists": container is not None, "running": bool(container and (container.get("State") or {}).get("Running"))}

    async def deprovision(self, command: CapabilityDeprovisionCommand) -> dict[str, Any]:
        name = self._container_name(command.workspace_id, command.package_digest)
        existing = await self._inspect_container(name)
        if existing is None:
            return {"activation_state": "stopped", "runtime_plugin_id": self.descriptor.plugin_id, "idempotent": True}
        labels = existing.get("Config", {}).get("Labels") or existing.get("Labels") or {}
        if labels.get("io.loom.managed") != "container-http-v1" or labels.get("io.loom.workspace") != command.workspace_id or labels.get("io.loom.slave") != self.slave_id or labels.get("io.loom.package-digest") != command.package_digest:
            raise RuntimeError("service_container_identity_conflict")
        await self._remove_container(name)
        return {"activation_state": "stopped", "runtime_plugin_id": self.descriptor.plugin_id, "idempotent": False}

    async def reconcile(self, activations: list[dict[str, Any]]) -> dict[str, Any]:
        results: list[dict[str, Any]] = []
        for activation in activations:
            desired_state = str(activation.get("desired_state") or "running")
            package_payload = activation.get("package_payload") or activation.get("package")
            if desired_state == "stopped":
                profile = activation.get("runtime_profile") or {}
                name = str(profile.get("container_name") or "")
                if name:
                    container = await self._inspect_container(name)
                    if container is not None:
                        labels = container.get("Config", {}).get("Labels") or container.get("Labels") or {}
                        payload = package_payload if isinstance(package_payload, dict) else {}
                        expected_digest = str(payload.get("package_digest") or activation.get("package_digest") or "")
                        expected_ref = str(payload.get("package_version_ref") or activation.get("package_version_ref") or "")
                        if isinstance(labels, dict) and labels.get("io.loom.managed") == self.descriptor.plugin_id and labels.get("io.loom.slave") == self.slave_id and labels.get("io.loom.workspace") == str(activation.get("workspace_id") or "workspace-default") and (not expected_digest or labels.get("io.loom.package-digest") == expected_digest) and (not expected_ref or labels.get("io.loom.package-ref") == expected_ref):
                            await self._remove_container(name)
                results.append({"activation_key": activation.get("activation_key"), "activation_state": "stopped"})
                continue
            profile = activation.get("runtime_profile") or {}
            name = str(profile.get("container_name") or "")
            inspected = await self.inspect(activation) if name else {"exists": False, "running": False}
            if inspected.get("exists") and inspected.get("running"):
                results.append({"activation_key": activation.get("activation_key"), "inspect": inspected, "activation_state": "ready"})
                continue
            if isinstance(package_payload, dict):
                try:
                    package = CapabilityPackageVersion.model_validate(package_payload)
                    command = CapabilityProvisionCommand(
                        command_id=f"reconcile-{activation.get('activation_key') or package.package_digest}",
                        package_version_ref=package.version_ref,
                        package_digest=package.package_digest,
                        target_slave=self.slave_id,
                        workspace_id=str(activation.get("workspace_id") or "workspace-default"),
                        idempotency_key=f"reconcile-{package.package_digest}",
                    )
                    provisioned = await self.provision(package, command)
                    results.append({"activation_key": activation.get("activation_key"), **provisioned})
                    continue
                except Exception as exc:
                    results.append({"activation_key": activation.get("activation_key"), "activation_state": "failed", "details": {"code": str(exc)}})
                    continue
            results.append({"activation_key": activation.get("activation_key"), "inspect": inspected, "activation_state": "lost"})
        return {"activations": results}

    async def close(self) -> None:
        self._semaphores.clear()


__all__ = ["DockerCommandResult", "DockerRunner", "SubprocessDockerRunner", "DockerContainerHTTPRuntimeV1"]
