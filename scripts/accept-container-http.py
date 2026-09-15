#!/usr/bin/env python3
"""Exercise the real runtime-plugin process and a sibling HTTP container."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

from loom_v2.contracts.types import (
    CapabilityDeprovisionCommand,
    CapabilityPackageVersion,
    CapabilityProvisionCommand,
    ResourceRef,
)
from loom_v2.slave.runtime_plugins import RuntimePluginHost


def _content_ref(digest: str) -> ResourceRef:
    return ResourceRef(
        resource_id=f"content://sha256/{digest}",
        identity_criterion="content_digest",
    )


async def main() -> None:
    image_ref = os.environ["LOOM_TEST_HTTP_IMAGE_REF"]
    plugin_dir = Path(os.getenv("LOOM_RUNTIME_PLUGIN_DIR", "runtime_plugins"))
    socket_dir = Path(os.getenv("LOOM_RUNTIME_PLUGIN_SOCKET_DIR", "/tmp/loom-runtime-plugins"))
    target_slave = os.getenv("LOOM_SLAVE_ID", "slave-runtime-e2e")
    descriptor_ref = ResourceRef(
        resource_id="loom://e2e/http-echo",
        version_or_digest="a" * 64,
        identity_criterion="descriptor_digest",
    )
    package = CapabilityPackageVersion(
        package_type="service",
        package_id="e2e-http-service",
        package_version="v1",
        package_closure_version_ref="closure-e2e",
        source_run_ref="run-e2e",
        source_closure_version_ref="version-e2e",
        execution={"kind": "container:http", "version": "1"},
        capability_exports=[
            {
                "capability_descriptor_ref": descriptor_ref,
                "io_contract_ref": _content_ref("b" * 64),
                "effect_class": "NetworkService",
                "permissions": [],
                "replay_safety": "DeclaredByPackage",
                "runtime_binding": {"path": "/echo"},
            }
        ],
        body={
            "image_ref": image_ref,
            "container_port": 8080,
            "health_path": "/health",
        },
    )
    provision = CapabilityProvisionCommand(
        command_id="e2e-provision",
        package_version_ref=package.version_ref,
        package_digest=package.package_digest,
        target_slave=target_slave,
        idempotency_key="e2e-provision",
        activation_revision=1,
    )
    deprovision = CapabilityDeprovisionCommand(
        command_id="e2e-deprovision",
        package_version_ref=package.version_ref,
        package_digest=package.package_digest,
        target_slave=target_slave,
        idempotency_key="e2e-deprovision",
        activation_revision=2,
    )
    host = RuntimePluginHost(
        plugin_dir=plugin_dir,
        socket_dir=socket_dir,
        startup_timeout=10,
        call_timeout=30,
    )
    provisioned = False
    try:
        await host.start()
        first = await host.provision(package, provision)
        if first.get("activation_state") != "ready":
            raise RuntimeError(f"provision_failed:{first}")
        provisioned = True
        second = await host.provision(package, provision)
        if second.get("activation_state") != "ready" or not second.get("idempotent"):
            raise RuntimeError(f"provision_not_idempotent:{second}")
        activation = {
            "workspace_id": provision.workspace_id,
            "target_slave": target_slave,
            "package_version_ref": package.version_ref,
            "package_digest": package.package_digest,
            "runtime_profile": first.get("runtime_profile") or {},
        }
        result = await host.invoke(
            package,
            package.capability_exports[0],
            {"value": 7},
            activation=activation,
            attempt_id="e2e-attempt",
            deadline_seconds=10,
        )
        if result.get("value") != {"echo": {"value": 7}}:
            raise RuntimeError(f"invoke_result_mismatch:{result}")
        stopped = await host.deprovision(package, deprovision)
        provisioned = False
        if stopped.get("activation_state") != "stopped":
            raise RuntimeError(f"deprovision_failed:{stopped}")
        print(
            json.dumps(
                {
                    "package_digest": package.package_digest,
                    "first_provision": first.get("activation_state"),
                    "second_provision_idempotent": second.get("idempotent"),
                    "result": result.get("value"),
                    "deprovision": stopped.get("activation_state"),
                },
                sort_keys=True,
            )
        )
    finally:
        if provisioned:
            try:
                await host.deprovision(package, deprovision)
            except Exception:
                pass
        await host.close()


if __name__ == "__main__":
    asyncio.run(main())
