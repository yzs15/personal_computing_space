import json
import httpx
import pytest

from loom_v2.driver.control_client import ObserverControlClient
from loom_v2.contracts.errors import DomainError


class RecordingTransport(httpx.AsyncBaseTransport):
    def __init__(self, response: httpx.Response):
        self.response = response
        self.requests = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return self.response


@pytest.mark.asyncio
async def test_control_client_sends_lease_fenced_command():
    transport = RecordingTransport(httpx.Response(200, json={"run_id": "run-1"}))
    client = ObserverControlClient(
        "http://observer:8080",
        driver_id="driver-default",
        instance_id="instance-1",
        lease_id="lease-1",
        driver_epoch=2,
        transport=transport,
    )
    result = await client.command("run.get", {"run_id": "run-1"})
    assert result["run_id"] == "run-1"
    request = transport.requests[-1]
    assert request.url.path == "/internal/v1/driver/commands"
    assert json.loads(request.content)["driver_epoch"] == 2


@pytest.mark.asyncio
async def test_control_client_surfaces_stale_epoch():
    transport = RecordingTransport(httpx.Response(409, json={"detail": "stale_driver_epoch"}))
    client = ObserverControlClient("http://observer:8080", driver_id="driver-default", instance_id="one", lease_id="lease", driver_epoch=1, transport=transport)
    with pytest.raises(RuntimeError, match="stale_driver_epoch"):
        await client.command("run.get", {"run_id": "run-1"})


@pytest.mark.asyncio
async def test_control_client_preserves_structured_readiness_error():
    detail = {
        "code": "readiness_blocked",
        "category": "domain",
        "retryable": False,
        "operation_ref": None,
        "details": {
            "blockers": [
                {
                    "code": "orchestration_program_unresolved_name",
                    "diagnostics": [{"file": "orchestration.py", "line": 7, "column": 38, "end_line": 7, "end_column": 42, "severity": "error", "rule": "reportUndefinedVariable", "message": "null is not defined"}],
                }
            ]
        },
    }
    transport = RecordingTransport(httpx.Response(409, json={"detail": detail}))
    client = ObserverControlClient("http://observer:8080", driver_id="driver-default", instance_id="one", lease_id="lease", driver_epoch=1, transport=transport)
    with pytest.raises(DomainError) as caught:
        await client.command("run.commit", {"run_id": "run-1"})
    assert caught.value.envelope.code == "readiness_blocked"
    assert caught.value.envelope.details["blockers"][0]["diagnostics"][0]["rule"] == "reportUndefinedVariable"


@pytest.mark.asyncio
async def test_control_client_lists_capability_packages():
    package = {
        "package_id": "summarize",
        "package_version": "v1",
        "package_closure_version_ref": "package-closure-1",
        "source_run_ref": "run-1",
        "source_closure_version_ref": "committed-1",
        "operation_descriptor_ref": "loom://summarize",
        "operation_descriptor_digest": "op-digest",
        "program_content_ref": {"resource_id": "content://sha256/program", "version_or_digest": "program-digest"},
        "program_digest": "program-digest",
        "io_contract_ref": {"resource_id": "content://sha256/io", "version_or_digest": "io-digest"},
        "executor_kind": "subprocess_json_v1",
        "executor_operation": "run_code",
        "package_digest": "package-digest",
    }
    transport = RecordingTransport(httpx.Response(200, json={"packages": [package]}))
    client = ObserverControlClient(
        "http://observer:8080",
        driver_id="driver-default",
        instance_id="instance-1",
        lease_id="lease-1",
        driver_epoch=2,
        transport=transport,
    )
    packages = await client.list_capability_packages(run_id="run-1", include_abandoned=True)
    assert packages[0].package_id == "summarize"
    request = transport.requests[-1]
    envelope = json.loads(request.content)
    assert envelope["command"] == "capability.list"
    assert envelope["arguments"]["run_id"] == "run-1"
    assert envelope["arguments"]["include_abandoned"] is True
