from dataclasses import dataclass, field
import json
from typing import Any

import httpx
import pytest

from loom_v2.contracts.types import (
    CapabilityPackageVersion,
    CapabilityProvisionCommand,
    ResourceRef,
)
from runtime_plugins.container_http_v1.runtime import (
    DockerCommandResult,
    DockerContainerHTTPRuntimeV1,
)


IMAGE_REF = "registry.example/loom/http-test@sha256:" + "d" * 64


def _ref(resource_id: str, *, descriptor_digest: str | None = None) -> dict[str, Any]:
    return ResourceRef(
        resource_id=resource_id,
        version_or_digest=descriptor_digest,
        identity_criterion="descriptor_digest" if descriptor_digest else "content_digest",
    ).model_dump(mode="json")


def _package(paths: list[str], *, health_path: str = "/healthz") -> CapabilityPackageVersion:
    return CapabilityPackageVersion(
        package_type="service",
        package_id="pkg-http-runtime",
        package_version="v1",
        package_closure_version_ref="closure-1",
        source_run_ref="run-1",
        source_closure_version_ref="version-1",
        execution={"kind": "container:http", "version": "1"},
        capability_exports=[
            {
                "capability_descriptor_ref": _ref(
                    f"loom://http/operation-{index}",
                    descriptor_digest=f"{index + 1:064x}",
                ),
                "io_contract_ref": _ref("content://sha256/" + f"{index + 20:064x}"),
                "effect_class": "NetworkService",
                "permissions": [],
                "replay_safety": "DeclaredByPackage",
                "runtime_binding": {"path": path},
            }
            for index, path in enumerate(paths)
        ],
        body={
            "image_ref": IMAGE_REF,
            "container_port": 8080,
            "health_path": health_path,
        },
    )


@dataclass
class RecordingRunner:
    calls: list[list[str]] = field(default_factory=list)

    async def run(self, args: list[str], *, timeout: float) -> DockerCommandResult:
        self.calls.append(list(args))
        if args[:2] == ["image", "inspect"]:
            return DockerCommandResult(0, json.dumps([IMAGE_REF]))
        if args[0] == "inspect":
            return DockerCommandResult(1, stderr="No such container")
        return DockerCommandResult(0)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("paths", "health_path", "error"),
    [
        (["/v1/echo", "/v1/echo"], "/healthz", "service_endpoint_duplicate"),
        (["/healthz"], "/healthz", "service_health_endpoint_conflict"),
    ],
)
async def test_endpoint_conflicts_are_rejected_before_docker_mutation(
    paths: list[str], health_path: str, error: str
) -> None:
    runner = RecordingRunner()
    runtime = DockerContainerHTTPRuntimeV1(
        slave_id="slave-test", network="loom-test-internal", runner=runner
    )

    with pytest.raises(RuntimeError, match=error):
        await runtime.provision(
            _package(paths, health_path=health_path).model_dump(mode="json"),
            CapabilityProvisionCommand(
                command_id="provision-1",
                package_version_ref="pkg-http-runtime:v1",
                package_digest="0" * 64,
                target_slave="slave-test",
                idempotency_key="provision-1",
                activation_revision=1,
            ).model_dump(mode="json"),
        )

    assert runner.calls == []


@pytest.mark.asyncio
async def test_provision_uses_fixed_container_security_arguments() -> None:
    runner = RecordingRunner()

    async def respond(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/healthz"
        return httpx.Response(204)

    runtime = DockerContainerHTTPRuntimeV1(
        slave_id="slave-test",
        network="loom-test-internal",
        runner=runner,
        http_transport=httpx.MockTransport(respond),
    )
    package = _package(["/v1/echo"])
    report = await runtime.provision(
        package.model_dump(mode="json"),
        CapabilityProvisionCommand(
            command_id="provision-1",
            package_version_ref=package.version_ref,
            package_digest=package.package_digest,
            target_slave="slave-test",
            idempotency_key="provision-1",
            activation_revision=1,
        ).model_dump(mode="json"),
    )

    create = next(args for args in runner.calls if args[0] == "create")
    assert report["activation_state"] == "ready"
    assert ["--network", "loom-test-internal"] == create[
        create.index("--network") : create.index("--network") + 2
    ]
    for required in ("--read-only", "--cap-drop", "--security-opt", "--pids-limit", "--memory", "--cpus", "--tmpfs"):
        assert required in create
    for forbidden in ("--publish", "--mount", "--volume", "--device", "--privileged"):
        assert forbidden not in create
    assert all("docker.sock" not in item for item in create)
    assert create[-1] == IMAGE_REF


def test_promoted_coordinate_reuses_digest_backed_container_identity() -> None:
    package = _package(["/v1/echo"])
    promoted_payload = package.model_dump(mode="json")
    promoted_payload["package_version"] = "v1-reusable"
    promoted = CapabilityPackageVersion.model_validate(promoted_payload)
    assert promoted.package_digest == package.package_digest

    runtime = DockerContainerHTTPRuntimeV1(
        slave_id="slave-test", network="loom-test-internal"
    )
    assert runtime._container_name("workspace-default", package.package_digest) == runtime._container_name(
        "workspace-default", promoted.package_digest
    )
    assert runtime._labels(
        "workspace-default", package.version_ref, package.package_digest
    ) == runtime._labels(
        "workspace-default", promoted.version_ref, promoted.package_digest
    )


@pytest.mark.asyncio
async def test_http_redirect_is_not_followed_or_replayed() -> None:
    package = _package(["/v1/echo"])
    labels = {
        "io.loom.managed": "container-http-v1",
        "io.loom.workspace": "workspace-default",
        "io.loom.slave": "slave-test",
        "io.loom.package-digest": package.package_digest,
    }
    calls = 0

    @dataclass
    class InvokeRunner:
        async def run(self, args: list[str], *, timeout: float) -> DockerCommandResult:
            if args[0] == "inspect":
                return DockerCommandResult(
                    0,
                    json.dumps(
                        [
                            {
                                "Config": {"Labels": labels, "Image": IMAGE_REF},
                                "State": {"Running": True},
                            }
                        ]
                    ),
                )
            raise AssertionError(args)

    async def redirect(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(307, headers={"location": "/v1/other"})

    runtime = DockerContainerHTTPRuntimeV1(
        slave_id="slave-test",
        network="loom-test-internal",
        runner=InvokeRunner(),
        http_transport=httpx.MockTransport(redirect),
    )

    with pytest.raises(RuntimeError, match="capability_service_http_error"):
        await runtime.invoke(
            package.model_dump(mode="json"),
            package.capability_exports[0].model_dump(mode="json"),
            {"message": "hello"},
            activation={
                "workspace_id": "workspace-default",
                "target_slave": "slave-test",
                "package_version_ref": package.version_ref,
                "package_digest": package.package_digest,
                "runtime_profile": {
                    "container_name": runtime._container_name(
                        "workspace-default", package.package_digest
                    )
                },
            },
            attempt_id="attempt-1",
        )

    assert calls == 1


@pytest.mark.asyncio
async def test_reconcile_stopped_requires_exact_package_identity_before_remove() -> None:
    package = _package(["/v1/echo"])
    runner = RecordingRunner()
    runtime = DockerContainerHTTPRuntimeV1(
        slave_id="slave-test", network="loom-test-internal", runner=runner
    )

    result = await runtime.reconcile(
        [
            {
                "activation_key": "activation-1",
                "workspace_id": "workspace-default",
                "slave_id": "slave-test",
                "target_slave": "slave-test",
                "package_version_ref": "capability-package://wrong/v1",
                "package_digest": package.package_digest,
                "package_payload": package.model_dump(mode="json"),
                "desired_state": "stopped",
                "runtime_profile": {},
            }
        ]
    )

    assert result["activations"][0]["activation_state"] == "failed"
    assert result["activations"][0]["details"]["code"] == (
        "runtime_plugin_identity_mismatch"
    )
    assert runner.calls == []
