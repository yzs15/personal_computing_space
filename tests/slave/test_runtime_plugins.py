import asyncio
from copy import deepcopy
from dataclasses import dataclass, field
import os
from pathlib import Path
from typing import Any, Sequence

import pytest

from loom_v2.content_store import ContentStore, canonical_json_bytes
from loom_v2.contracts.package_contracts import (
    PROCESS_JSON_STDIO_V1_SCHEMA,
    package_contract_registry,
)
from loom_v2.contracts.types import (
    CapabilityDeprovisionCommand,
    CapabilityPackageVersion,
    CapabilityProvisionCommand,
    ComputeBinding,
    ResourceRef,
)
from loom_v2.slave.runtime_plugins import (
    PLUGIN_PROTOCOL_VERSION,
    ProcessRuntimePlugin,
    RuntimePluginDescriptor,
    RuntimePluginError,
    RuntimePluginHost,
    RuntimePluginSupport,
    _PluginManifest,
)
from loom_v2.slave.service import SlaveService


CUSTOM_KEY = ("test-generic-runtime", "test:echo", "1")


def _register_custom_contract() -> None:
    schema = deepcopy(PROCESS_JSON_STDIO_V1_SCHEMA)
    schema["properties"]["package_type"] = {"const": CUSTOM_KEY[0]}
    schema["properties"]["execution"]["properties"]["kind"] = {
        "const": CUSTOM_KEY[1]
    }
    schema["properties"]["body"] = {
        "type": "object",
        "required": ["message"],
        "properties": {"message": {"type": "string"}},
        "additionalProperties": False,
    }
    package_contract_registry.register(CUSTOM_KEY, schema)


def _store() -> ContentStore:
    return ContentStore(
        endpoint_url=os.environ["LOOM_S3_ENDPOINT_URL"],
        bucket=os.environ["LOOM_S3_BUCKET"],
        access_key=os.environ["LOOM_S3_ACCESS_KEY"],
        secret_key=os.environ["LOOM_S3_SECRET_KEY"],
    )


@dataclass
class EchoPlugin:
    descriptor: RuntimePluginDescriptor = RuntimePluginDescriptor(
        plugin_id="test-echo-v1",
        protocol_version=PLUGIN_PROTOCOL_VERSION,
        supports=(RuntimePluginSupport(*CUSTOM_KEY),),
    )
    calls: list[str] = field(default_factory=list)

    async def provision(self, package, command) -> dict[str, Any]:
        self.calls.append("provision")
        return {
            "activation_state": "ready",
            "runtime_plugin_id": self.descriptor.plugin_id,
            "runtime_profile": {"handle": package.package_digest},
        }

    async def invoke(
        self,
        package,
        capability_export,
        payload,
        *,
        activation,
        attempt_id,
        deadline_seconds=None,
    ) -> dict[str, Any]:
        self.calls.append("invoke")
        return {
            "value": {"echo": payload, "configured": package.body["message"]},
            "replay_safety": capability_export.replay_safety,
            "runtime_plugin_id": self.descriptor.plugin_id,
        }

    async def inspect(self, activation) -> dict[str, Any]:
        self.calls.append("inspect")
        return {"exists": True, "running": True}

    async def deprovision(self, command) -> dict[str, Any]:
        self.calls.append("deprovision")
        return {
            "activation_state": "stopped",
            "runtime_plugin_id": self.descriptor.plugin_id,
        }

    async def reconcile(self, activations: Sequence[dict[str, Any]]) -> dict[str, Any]:
        self.calls.append("reconcile")
        return {"activations": []}

    async def close(self) -> None:
        self.calls.append("close")


def test_runtime_plugin_requires_a_registered_package_contract() -> None:
    plugin = EchoPlugin(
        descriptor=RuntimePluginDescriptor(
            plugin_id="unregistered-v1",
            protocol_version=PLUGIN_PROTOCOL_VERSION,
            supports=(
                RuntimePluginSupport(
                    "test-unregistered-runtime", "test:unregistered-runtime", "1"
                ),
            ),
        )
    )

    with pytest.raises(RuntimePluginError, match="runtime_plugin_capability_mismatch"):
        RuntimePluginHost(plugins={"unregistered": plugin})


def test_runtime_plugin_rejects_duplicate_provider_for_same_triple() -> None:
    _register_custom_contract()
    host = RuntimePluginHost()
    host.register(EchoPlugin())

    with pytest.raises(RuntimePluginError, match="runtime_plugin_conflict"):
        host.register(EchoPlugin())


def test_runtime_plugin_support_requires_explicit_execution_version() -> None:
    with pytest.raises(
        RuntimePluginError, match="runtime_plugin_manifest_invalid"
    ):
        RuntimePluginSupport.from_mapping(
            {"package_type": "service", "execution_kind": "container:http"}
        )


@pytest.mark.asyncio
async def test_shipped_plugin_is_a_self_contained_discoverable_bundle(
    tmp_path: Path,
) -> None:
    bundle_root = Path(__file__).resolve().parents[2] / "runtime_plugins"
    bundle = bundle_root / "container_http_v1"
    implementation = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (bundle / "runtime.py", bundle / "server.py")
    )
    assert "from loom_v2" not in implementation
    assert "import loom_v2" not in implementation

    host = RuntimePluginHost(
        plugin_dir=bundle_root,
        socket_dir=tmp_path / "sockets",
        startup_timeout=2,
    )
    try:
        await host.start()
        assert [item.plugin_id for item in host.descriptors()] == [
            "container-http-v1"
        ]
    finally:
        await host.close()


@pytest.mark.asyncio
async def test_process_restart_replays_desired_activation_without_lock_recursion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    executable = tmp_path / "runtime-plugin"
    executable.write_text("#!/bin/sh\n", encoding="utf-8")
    executable.chmod(0o755)
    manifest = _PluginManifest(
        root=tmp_path,
        plugin_id="restart-test",
        protocol_version=PLUGIN_PROTOCOL_VERSION,
        command=("runtime-plugin",),
        supports=(RuntimePluginSupport("service", "container:http", "1"),),
    )
    plugin = ProcessRuntimePlugin(
        manifest, socket_dir=tmp_path / "sockets", startup_timeout=0.1
    )

    @dataclass
    class FakeProcess:
        returncode: int | None

    plugin.process = FakeProcess(1)  # type: ignore[assignment]
    desired = {"activation_key": "activation-1"}
    plugin._desired_activations = [desired]
    replayed: list[dict[str, Any]] = []

    async def fake_start() -> None:
        plugin.process = FakeProcess(None)  # type: ignore[assignment]
        plugin._client = object()  # type: ignore[assignment]

    async def fake_request_once(
        method: str, path: str, payload: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        assert method == "POST"
        assert path == "/v1/reconcile"
        replayed.extend((payload or {}).get("activations", []))
        return {"activations": []}

    monkeypatch.setattr(plugin, "start", fake_start)
    monkeypatch.setattr(plugin, "_request_once", fake_request_once)

    await asyncio.wait_for(plugin._restart_if_needed(), timeout=1)

    assert replayed == [desired]


@pytest.mark.asyncio
async def test_discovery_rejects_duplicate_provider_before_starting_processes(
    tmp_path: Path,
) -> None:
    for name in ("first", "second"):
        bundle = tmp_path / name
        executable = bundle / "bin" / "runtime-plugin"
        executable.parent.mkdir(parents=True)
        executable.write_text(
            "#!/bin/sh\ntouch \"$(dirname \"$0\")/started\"\n",
            encoding="utf-8",
        )
        executable.chmod(0o755)
        (bundle / "plugin.toml").write_text(
            f'''plugin_id = "{name}"
protocol_version = "loom.runtime-plugin/1"
command = ["bin/runtime-plugin"]

[[supports]]
package_type = "service"
execution_kind = "container:http"
execution_version = "1"
''',
            encoding="utf-8",
        )
    host = RuntimePluginHost(
        plugin_dir=tmp_path,
        socket_dir=tmp_path / "sockets",
        startup_timeout=0.1,
    )

    with pytest.raises(RuntimePluginError, match="runtime_plugin_conflict"):
        await host.start()

    assert not list(tmp_path.glob("*/bin/started"))


@pytest.mark.asyncio
async def test_generic_plugin_provision_invoke_and_deprovision_without_core_type_branch() -> None:
    _register_custom_contract()
    store = _store()
    io_contract_ref = await store.put(
        canonical_json_bytes(
            {
                "schema_version": "io.v1",
                "input_schema_ref": None,
                "output_schema_ref": None,
                "success_semantics": None,
                "success_validator_ref": None,
            }
        ),
        media_type="application/vnd.loom.io-contract+json",
    )
    descriptor_ref = ResourceRef(
        resource_id="loom://test/echo",
        version_or_digest="a" * 64,
        identity_criterion="descriptor_digest",
    )
    package = CapabilityPackageVersion(
        package_type=CUSTOM_KEY[0],
        package_id="pkg-generic-runtime",
        package_version="v1",
        package_closure_version_ref="closure-1",
        source_run_ref="run-1",
        source_closure_version_ref="version-1",
        execution={"kind": CUSTOM_KEY[1], "version": CUSTOM_KEY[2]},
        capability_exports=[
            {
                "capability_descriptor_ref": descriptor_ref,
                "io_contract_ref": io_contract_ref,
                "effect_class": "Pure",
                "permissions": [],
                "replay_safety": "Idempotent",
                "runtime_binding": {},
            }
        ],
        body={"message": "configured"},
    )
    plugin = EchoPlugin()
    host = RuntimePluginHost(plugins={"echo": plugin})
    service = SlaveService(
        "slave-test", content_store=store, runtime_plugin_host=host
    )
    binding = ComputeBinding(
        binding_id="binding-echo",
        hole_id="hole-echo",
        capability_descriptor_ref=descriptor_ref,
        capability_package_ref=ResourceRef(
            resource_id=package.version_ref,
            version_or_digest=package.package_digest,
        ),
        target_resource_ref=ResourceRef(resource_id="slave-test"),
    )
    provision_command = CapabilityProvisionCommand(
        command_id="provision-echo",
        package_version_ref=package.version_ref,
        package_digest=package.package_digest,
        target_slave="slave-test",
        idempotency_key="provision-echo",
        activation_revision=1,
    )

    report = await service.provision(provision_command, package)
    result = await service.run(
        "attempt-echo", "echo", {"value": 3}, binding=binding
    )
    stopped = await service.deprovision(
        CapabilityDeprovisionCommand(
            command_id="deprovision-echo",
            package_version_ref=package.version_ref,
            package_digest=package.package_digest,
            target_slave="slave-test",
            idempotency_key="deprovision-echo",
            activation_revision=2,
        ),
        package,
    )

    assert report.activation_state == "ready"
    assert result.value == {"echo": {"value": 3}, "configured": "configured"}
    stored = await store.get(result.resource_ref)
    assert stored == canonical_json_bytes(result.value)
    assert result.resource_ref.digest == store.digest(stored)
    assert stopped.activation_state == "stopped"
    assert plugin.calls == ["provision", "invoke", "deprovision"]
