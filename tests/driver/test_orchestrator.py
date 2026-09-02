import pytest

from loom_v2.contracts.types import ResourceRef
from loom_v2.driver.orchestrator import (
    DockerOrchestrationExecutor,
    OrchestrationExecutorError,
    OrchestrationProgramError,
)


def _ref(resource_id: str, digest: str) -> ResourceRef:
    return ResourceRef(resource_id=resource_id, version_or_digest=digest)


def test_docker_executor_uses_os_level_sandbox_flags():
    executor = DockerOrchestrationExecutor(image="python:3.12-slim")
    args = executor.docker_args()

    assert args[:2] == ["docker", "run"]
    assert "--interactive" in args
    assert "--pull=never" in args
    assert "--network" in args and args[args.index("--network") + 1] == "none"
    assert "--read-only" in args
    assert "--user" in args and args[args.index("--user") + 1] == "65534:65534"
    assert "--cap-drop" in args and args[args.index("--cap-drop") + 1] == "ALL"
    assert "--security-opt" in args and args[args.index("--security-opt") + 1] == "no-new-privileges"
    assert "--memory" in args
    assert "--cpus" in args
    assert "--pids-limit" in args
    assert "python:3.12-slim" in args


def test_orchestration_sandbox_environment_does_not_inherit_secrets(monkeypatch):
    monkeypatch.setenv("LOOM_INTERNAL_API_SECRET", "internal-secret")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "codex-secret")
    executor = DockerOrchestrationExecutor()
    environment = executor.sandbox_environment()
    assert "LOOM_INTERNAL_API_SECRET" not in environment
    assert "DEEPSEEK_API_KEY" not in environment


def test_orchestration_program_rejects_imports_and_side_effect_calls():
    executor = DockerOrchestrationExecutor()
    with pytest.raises(OrchestrationExecutorError, match="orchestration_program_import_forbidden"):
        executor._validate_program(b"import os\nasync def orchestrate(ctx, input_ref): return input_ref\n")
    with pytest.raises(OrchestrationExecutorError, match="orchestration_program_side_effect_forbidden"):
        executor._validate_program(b"async def orchestrate(ctx, input_ref): return open('/tmp/x')\n")


@pytest.mark.integration
async def test_docker_executor_runs_orchestration_protocol():
    input_ref = _ref("content://sha256/" + "a" * 64, "a" * 64)
    package_ref = _ref("capability-package://summarize/v1", "b" * 64)
    final_ref = _ref("content://sha256/" + "c" * 64, "c" * 64)
    program = (
        b'''
PACKAGE_REF = {
    "resource_id": "capability-package://summarize/v1",
    "version_or_digest": "'''
        + b"b" * 64
        + b'''",
}


async def orchestrate(ctx: "OrchestrationContext", input_ref: "ResourceRef") -> "ResourceRef":
    print("program stdout must not corrupt the protocol")
    document = await ctx.read_json(input_ref)
    handle = ctx.emit_node(PACKAGE_REF, [input_ref])
    result_ref = await ctx.result(handle)
    assert document["partitions"] == [input_ref]
    return result_ref
'''
    )
    executor = DockerOrchestrationExecutor(image="python:3.12-slim")

    async def read_json(ref):
        assert ref == input_ref
        return {"partitions": [input_ref.model_dump(mode="json")]}

    async def emit_node(package, inputs):
        assert package == package_ref
        assert inputs == [input_ref]
        return "node-1"

    async def result(handle):
        assert handle == "node-1"
        return final_ref

    returned = await executor.run(
        program,
        input_ref,
        read_json=read_json,
        emit_node=emit_node,
        result=result,
    )

    assert returned == final_ref


@pytest.mark.integration
async def test_docker_executor_reports_structured_program_failure():
    input_ref = _ref("content://sha256/" + "a" * 64, "a" * 64)
    program = b'''
async def orchestrate(ctx: "OrchestrationContext", input_ref: "ResourceRef") -> "ResourceRef":
    raise OrchestrationFailure({"code": "custom_failure", "details": {"value": 7}})
'''
    executor = DockerOrchestrationExecutor(image="python:3.12-slim")

    with pytest.raises(OrchestrationProgramError) as caught:
        await executor.run(
            program,
            input_ref,
            read_json=lambda ref: {},
            emit_node=lambda package, inputs: "unused",
            result=lambda handle: input_ref,
        )

    assert caught.value.reason == {"code": "custom_failure", "details": {"value": 7}}
