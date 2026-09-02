import httpx
import pytest

from loom_v2.coding_agents.base import AgentEvent
from loom_v2.contracts.agents import DriverCommand
from loom_v2.contracts.types import ResourceRef
from loom_v2.driver.service import DriverService
from loom_v2.driver.worker import WorkerSession
from loom_v2.coding_agents.fake import FakeCodingAgentProvider
from loom_v2.observer.repository import ObserverRepository
from loom_v2.slave.app import create_app as create_slave_app


class DynamicToolProvider:
    def set_tool_handler(self, tools, handler):
        self.tool_handler = handler
        self.tool_names = {tool["name"] for tool in tools}

    async def start(self, conversation_ref: str, workspace_root: str) -> str:
        return "dynamic-tool-thread"

    async def send_turn(self, user_message: str):
        assert "loom_open_run" in self.tool_names
        await self.tool_handler(
            "loom_open_run",
            {
                "closure_contract": {
                    "closure_id": "closure-dynamic-remote",
                    "goal": user_message,
                    "body": {
                        "closure_id": "closure-dynamic-remote",
                        "program": {"operation_ref": "loom://echo"},
                        "compute": {"operation_ref": "loom://echo"},
                    },
                }
            },
        )
        yield AgentEvent("tool_call", {"tool": "loom_open_run"})
        await self.tool_handler("loom_commit_plan", {})
        yield AgentEvent("tool_call", {"tool": "loom_commit_plan"})
        await self.tool_handler("loom_start_run", {})
        yield AgentEvent("tool_call", {"tool": "loom_start_run"})

    async def interrupt(self, turn_ref: str | None = None) -> None:
        return None

    async def close(self) -> None:
        return None


class ContentToolProvider(DynamicToolProvider):
    async def send_turn(self, user_message: str):
        await self.tool_handler(
            "loom_put_content",
            {"content": {"text": user_message}, "media_type": "application/json"},
        )
        yield AgentEvent("assistant_text", {"text": "content stored"})


class RecordingContentStore:
    def __init__(self):
        self.contents = []

    async def put(self, content: bytes, *, media_type: str):
        self.contents.append((content, media_type))
        digest = "a" * 64
        return ResourceRef(
            resource_id=f"content://sha256/{digest}",
            version_or_digest=digest,
            identity_criterion="content_digest",
        )


class RepositoryControl:
    driver_id = "driver-default"
    instance_id = "driver-instance"
    workspace_id = "workspace-default"
    driver_epoch = 1
    internal_api_secret = ""

    def __init__(self, repository):
        self.repository = repository
        self.lease = None
        self.sequence = 0

    async def register(self):
        self.lease = await self.repository.register_agent({
            "role": "driver",
            "agent_id": self.driver_id,
            "instance_id": self.instance_id,
            "workspace_id": self.workspace_id,
            "endpoint_url": "http://driver:8090",
            "protocol_version": "loom.v1",
        })
        self.driver_epoch = self.lease.epoch

    async def command(self, command, arguments=None, **kwargs):
        self.sequence += 1
        envelope_request_id = kwargs.get("request_id") or f"request-{command}-{self.sequence}"
        if command == "turn.state":
            envelope_request_id = f"turn-state:{arguments.get('conversation_ref')}:{arguments.get('turn_state')}:{envelope_request_id}"
        return await self.repository.execute_driver_command(DriverCommand(
            request_id=envelope_request_id,
            driver_id=self.driver_id,
            instance_id=self.instance_id,
            lease_id=self.lease.lease_id,
            driver_epoch=self.driver_epoch,
            command=command,
            arguments={**(arguments or {}), "workspace_id": self.workspace_id},
        ))

    async def thread_binding(self, conversation_ref):
        return await self.repository.get_thread(self.workspace_id, conversation_ref)

    async def thread_bind(self, binding):
        return await self.command("thread.bind", binding)

    async def turn_state(self, conversation_ref, state, **kwargs):
        return await self.command("turn.state", {"conversation_ref": conversation_ref, "turn_state": state, **kwargs}, request_id=kwargs.get("request_id"))


@pytest.mark.asyncio
async def test_remote_driver_runs_built_in_operation_through_slave_http():
    repository = ObserverRepository()
    control = RepositoryControl(repository)
    await control.register()
    slave_app = create_slave_app("slave-a")
    worker = WorkerSession("slave-a", "http://slave-a", transport=httpx.ASGITransport(app=slave_app))
    service = DriverService(control, FakeCodingAgentProvider(), workers={"slave-a": worker})
    result = await service.run_prompt("conversation-remote", "echo hello", request_id="request-remote")
    assert result["status"] == "completed"
    assert result["state"] == "completed"
    assert result["thread_id"].startswith("fake-session:")


@pytest.mark.asyncio
async def test_remote_driver_tracks_dynamic_tool_run_and_dispatches_once():
    repository = ObserverRepository()
    control = RepositoryControl(repository)
    await control.register()
    slave_app = create_slave_app("slave-a")
    worker = WorkerSession("slave-a", "http://slave-a", transport=httpx.ASGITransport(app=slave_app))
    service = DriverService(control, DynamicToolProvider(), workers={"slave-a": worker})

    result = await service.run_prompt("conversation-dynamic-remote", "echo hello", request_id="request-dynamic-remote")

    assert result["run_id"]
    assert result["state"] == "completed"
    assert result["status"] == "completed"
    record = await repository.get_run(result["run_id"])
    assert record.execution_id is not None
    assert record.outcome["value"]["text"] == "echo hello"


@pytest.mark.asyncio
async def test_remote_driver_passes_content_store_to_dynamic_tools():
    repository = ObserverRepository()
    control = RepositoryControl(repository)
    await control.register()
    content_store = RecordingContentStore()
    service = DriverService(control, ContentToolProvider(), content_store=content_store)

    result = await service.run_prompt("conversation-content-remote", "hello", request_id="request-content-remote")

    assert result["status"] == "completed"
    assert result["assistant_text"] == "content stored"
    assert content_store.contents == [(b'{"text":"hello"}', "application/json")]


@pytest.mark.asyncio
async def test_remote_thread_initialization_failure_marks_claim_failed():
    class FailingProvider:
        model = "deepseek-v4-flash"

        async def begin_turn(self, *args, **kwargs):
            raise RuntimeError("codex_thread_unavailable")

        async def close(self):
            return None

        async def force_shutdown(self):
            return None

    class ClaimingRepositoryControl(RepositoryControl):
        async def claim_message(self, request_id, payload_digest, *, conversation_ref=None, claim_token):
            return await self.command(
                "message.claim",
                {
                    "request_id": request_id,
                    "payload_digest": payload_digest,
                    "conversation_ref": conversation_ref,
                    "claim_token": claim_token,
                },
            )

        async def update_message(self, request_id, *, claim_token, state=None, assistant_text=None, run_id=None, outcome=None):
            return await self.command(
                "message.update",
                {
                    "request_id": request_id,
                    "claim_token": claim_token,
                    "state": state,
                    "assistant_text": assistant_text,
                    "run_id": run_id,
                    "outcome": outcome,
                },
            )

    repository = ObserverRepository()
    control = ClaimingRepositoryControl(repository)
    await control.register()
    request_id = "request-thread-init-failure"
    conversation_ref = "conversation-thread-init-failure"
    receipt = await repository.create_or_get_message_receipt(
        control.workspace_id,
        request_id,
        conversation_ref,
        "resume this thread",
    )

    service = DriverService(control, FailingProvider())
    with pytest.raises(RuntimeError, match="codex_thread_unavailable"):
        await service.run_prompt(conversation_ref, receipt.prompt, request_id=request_id, payload_digest=receipt.payload_digest)

    persisted = await repository.get_message_receipt(control.workspace_id, request_id)
    assert persisted is not None
    assert persisted.state == "failed"
    assert persisted.outcome == {"error": {"code": "codex_thread_unavailable", "message": "codex_thread_unavailable"}}
