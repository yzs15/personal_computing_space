from __future__ import annotations

import ast
import asyncio
import inspect
import json
import os
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from loom_v2.contracts.types import ResourceRef
from loom_v2.observer.orchestration_preflight import validate_entry_signature


ReadJsonCallback = Callable[[ResourceRef], Any]
EmitNodeCallback = Callable[[ResourceRef, list[ResourceRef]], Any]
ResultCallback = Callable[[str], Any]


class OrchestrationExecutorError(RuntimeError):
    """The Docker orchestration boundary could not run the program."""


class OrchestrationProgramError(RuntimeError):
    """The orchestration program ended with a structured failure."""

    def __init__(self, reason: dict[str, Any]) -> None:
        self.reason = reason
        super().__init__(json.dumps(reason, sort_keys=True, ensure_ascii=False))


@dataclass(frozen=True)
class OrchestrationExecutorDescriptor:
    kind: str = "orchestrator_python_v1"
    version: str = "1"
    operation: str = "orchestrate"


# This bootstrap runs inside the pinned Python image.  It keeps the original
# stdout for protocol frames and redirects the program's ``sys.stdout`` to
# stderr, so ordinary ``print`` calls cannot masquerade as protocol messages.
_BOOTSTRAP = r'''
import asyncio
import builtins
import json
import sys

_PROTOCOL = sys.stdout
sys.stdout = sys.stderr


class OrchestrationFailure(Exception):
    def __init__(self, reason):
        if not isinstance(reason, dict):
            reason = {"code": "orchestration_failure", "details": reason}
        self.reason = reason
        super().__init__(json.dumps(reason, sort_keys=True, ensure_ascii=False))


def _send(message):
    _PROTOCOL.write(json.dumps(message, separators=(",", ":"), ensure_ascii=False))
    _PROTOCOL.write("\n")
    _PROTOCOL.flush()


def _receive():
    line = sys.stdin.readline()
    if not line:
        raise OrchestrationFailure({"code": "orchestration_protocol_closed"})
    try:
        message = json.loads(line)
    except Exception as exc:
        raise OrchestrationFailure({
            "code": "orchestration_protocol_invalid",
            "message": str(exc),
        }) from exc
    if message.get("type") == "error":
        raise OrchestrationFailure(message.get("error") or {"code": "orchestration_context_error"})
    return message


class OrchestrationContext:
    async def read_json(self, ref):
        _send({"type": "read_json", "ref": ref})
        message = _receive()
        if message.get("type") != "value" or "value" not in message:
            raise OrchestrationFailure({"code": "orchestration_protocol_invalid"})
        return message["value"]

    def emit_node(self, package_ref, input_refs):
        _send({
            "type": "emit_node",
            "package_ref": package_ref,
            "input_refs": input_refs,
        })
        message = _receive()
        if message.get("type") != "handle" or not isinstance(message.get("handle"), str):
            raise OrchestrationFailure({"code": "orchestration_protocol_invalid"})
        return message["handle"]

    async def result(self, handle):
        _send({"type": "result", "handle": handle})
        message = _receive()
        if message.get("type") != "ref" or not isinstance(message.get("ref"), dict):
            raise OrchestrationFailure({"code": "orchestration_protocol_invalid"})
        return message["ref"]


async def _main():
    initial_line = sys.stdin.readline()
    if not initial_line:
        raise OrchestrationFailure({"code": "orchestration_protocol_closed"})
    try:
        initial = json.loads(initial_line)
        program = initial["program"]
        input_ref = initial["input_ref"]
    except Exception as exc:
        raise OrchestrationFailure({
            "code": "orchestration_protocol_invalid",
            "message": str(exc),
        }) from exc

    allowed_modules = {"json", "math", "statistics", "itertools", "functools", "collections"}
    real_import = builtins.__import__
    def safe_import(name, globals=None, locals=None, fromlist=(), level=0):
        root = name.split(".", 1)[0]
        if level or root not in allowed_modules:
            raise ImportError("orchestration_import_forbidden")
        return real_import(name, globals, locals, fromlist, level)
    safe_builtins = {
        name: getattr(builtins, name)
        for name in (
            "abs", "all", "any", "bool", "dict", "enumerate", "filter", "float",
            "int", "isinstance", "len", "list", "map", "max", "min", "range",
            "set", "sorted", "str", "sum", "tuple", "zip", "print", "Exception",
        )
        if hasattr(builtins, name)
    }
    safe_builtins["__import__"] = safe_import
    namespace = {
        "__name__": "loom_orchestration",
        "__builtins__": safe_builtins,
        "OrchestrationFailure": OrchestrationFailure,
    }
    exec(compile(program, "<loom-orchestration>", "exec"), namespace)
    orchestrate = namespace.get("orchestrate")
    if not callable(orchestrate):
        raise OrchestrationFailure({"code": "orchestrate_function_required"})
    final_ref = await orchestrate(OrchestrationContext(), input_ref)
    if not isinstance(final_ref, dict):
        raise OrchestrationFailure({"code": "final_ref_required"})
    _send({"type": "final", "ref": final_ref})


try:
    asyncio.run(_main())
except OrchestrationFailure as exc:
    _send({"type": "failure", "error": exc.reason})
except BaseException as exc:
    _send({
        "type": "failure",
        "error": {
            "code": "orchestration_program_error",
            "message": str(exc),
        },
    })
'''


class DockerOrchestrationExecutor:
    """Run an orchestration program in a real Docker OS-level sandbox."""

    descriptor = OrchestrationExecutorDescriptor()

    def __init__(
        self,
        *,
        image: str = "python:3.12-slim",
        memory: str = "512m",
        cpus: float = 1.0,
        pids_limit: int = 128,
        max_program_bytes: int = 1_048_576,
        max_message_bytes: int = 4_194_304,
        timeout_seconds: float = 300.0,
    ) -> None:
        if not image:
            raise ValueError("orchestrator_image_required")
        if pids_limit <= 0:
            raise ValueError("orchestrator_pids_limit_invalid")
        if cpus <= 0:
            raise ValueError("orchestrator_cpus_invalid")
        if max_program_bytes <= 0 or max_message_bytes <= 0:
            raise ValueError("orchestrator_size_limit_invalid")
        if timeout_seconds <= 0:
            raise ValueError("orchestrator_timeout_invalid")
        self.image = image
        self.memory = memory
        self.cpus = cpus
        self.pids_limit = pids_limit
        self.max_program_bytes = max_program_bytes
        self.max_message_bytes = max_message_bytes
        self.timeout_seconds = timeout_seconds

    @classmethod
    def from_settings(cls, settings: Any) -> "DockerOrchestrationExecutor":
        return cls(
            image=settings.orchestrator_image,
            memory=settings.orchestrator_memory,
            cpus=settings.orchestrator_cpus,
            pids_limit=settings.orchestrator_pids_limit,
            max_program_bytes=settings.orchestrator_max_program_bytes,
            max_message_bytes=settings.orchestrator_max_message_bytes,
            timeout_seconds=getattr(settings, "orchestrator_timeout_seconds", 300.0),
        )

    def docker_args(self) -> list[str]:
        return [
            "docker",
            "run",
            "--rm",
            "--interactive",
            "--pull=never",
            "--network",
            "none",
            "--read-only",
            "--user",
            "65534:65534",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
            "--memory",
            self.memory,
            "--cpus",
            str(self.cpus),
            "--pids-limit",
            str(self.pids_limit),
            self.image,
            "python",
            "-I",
            "-c",
            _BOOTSTRAP,
        ]

    @staticmethod
    def sandbox_environment() -> dict[str, str]:
        """Return a non-sensitive environment for the isolated container."""
        environment = dict(os.environ)
        denied_names = {
            "LOOM_INTERNAL_API_SECRET",
            "LOOM_CODEX_API_KEY",
            "DEEPSEEK_API_KEY",
            "OPENAI_API_KEY",
            "CODEX_API_KEY",
            "CODEX_HOME",
            "LOOM_CODEX_API_KEY_FILE",
            "LOOM_INTERNAL_API_SECRET_FILE",
            "LOOM_DATABASE_URL",
            "LOOM_S3_ENDPOINT_URL",
            "LOOM_S3_BUCKET",
            "LOOM_S3_ACCESS_KEY",
            "LOOM_S3_SECRET_KEY",
            "LOOM_S3_REGION",
            "LOOM_S3_PREFIX",
            "LOOM_OBSERVER_URL",
            "LOOM_DRIVER_URL",
            "LOOM_SLAVE_A_URL",
            "LOOM_SLAVE_B_URL",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECURITY_TOKEN",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
        }
        for name in list(environment):
            if name in denied_names or "SECRET" in name or name.endswith("_API_KEY"):
                environment.pop(name, None)
        return environment

    def _validate_program(self, program: bytes) -> str:
        if not isinstance(program, bytes):
            raise OrchestrationExecutorError("program_required")
        if len(program) > self.max_program_bytes:
            raise OrchestrationExecutorError("orchestration_program_too_large")
        try:
            source = program.decode("utf-8")
            tree = ast.parse(source, filename="<loom-orchestration>")
        except (UnicodeDecodeError, SyntaxError) as exc:
            raise OrchestrationExecutorError("orchestration_program_invalid") from exc
        has_orchestrate = any(
            isinstance(node, ast.AsyncFunctionDef)
            and node.name == "orchestrate"
            for node in tree.body
        )
        if not has_orchestrate:
            raise OrchestrationExecutorError("orchestrate_function_required")
        forbidden_calls = {"open", "eval", "exec", "compile", "input", "__import__"}
        forbidden_attrs = {"environ", "system", "popen", "spawn", "fork", "socket", "create_connection"}
        forbidden_names = {"globals", "locals", "vars", "getattr", "setattr", "delattr", "object", "type", "builtins", "__builtins__"}
        allowed_modules = {"json", "math", "statistics", "itertools", "functools", "collections"}
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                if any(alias.name.split(".", 1)[0] not in allowed_modules for alias in node.names):
                    raise OrchestrationExecutorError("orchestration_program_import_forbidden")
            elif isinstance(node, ast.ImportFrom):
                if node.level or node.module is None or node.module.split(".", 1)[0] not in allowed_modules:
                    raise OrchestrationExecutorError("orchestration_program_import_forbidden")
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in forbidden_calls:
                raise OrchestrationExecutorError("orchestration_program_side_effect_forbidden")
            if isinstance(node, ast.Name) and node.id in forbidden_names:
                raise OrchestrationExecutorError("orchestration_program_side_effect_forbidden")
            if isinstance(node, ast.Attribute) and (node.attr.startswith("__") or node.attr in forbidden_attrs):
                raise OrchestrationExecutorError("orchestration_program_side_effect_forbidden")
        signature_blockers = validate_entry_signature(source)
        if signature_blockers:
            raise OrchestrationExecutorError("orchestration_program_type_error")
        return source

    async def _write_message(
        self,
        stdin: asyncio.StreamWriter,
        message: dict[str, Any],
    ) -> None:
        try:
            body = json.dumps(message, separators=(",", ":"), ensure_ascii=False).encode() + b"\n"
        except (TypeError, ValueError) as exc:
            raise OrchestrationExecutorError("orchestration_message_invalid") from exc
        if len(body) > self.max_message_bytes:
            raise OrchestrationExecutorError("orchestration_message_too_large")
        try:
            stdin.write(body)
            await stdin.drain()
        except (BrokenPipeError, ConnectionResetError) as exc:
            raise OrchestrationExecutorError("orchestration_process_closed") from exc

    async def _read_message(self, stdout: asyncio.StreamReader) -> dict[str, Any]:
        try:
            line = await stdout.readline()
        except (ValueError, asyncio.LimitOverrunError) as exc:
            raise OrchestrationExecutorError("orchestration_message_too_large") from exc
        if not line:
            raise OrchestrationExecutorError("orchestration_process_closed")
        if len(line) > self.max_message_bytes:
            raise OrchestrationExecutorError("orchestration_message_too_large")
        try:
            message = json.loads(line)
        except json.JSONDecodeError as exc:
            raise OrchestrationExecutorError("orchestration_message_invalid") from exc
        if not isinstance(message, dict):
            raise OrchestrationExecutorError("orchestration_message_invalid")
        return message

    @staticmethod
    async def _maybe_await(value: Any) -> Any:
        if inspect.isawaitable(value):
            return await value
        return value

    @staticmethod
    def _resource_ref(value: Any, *, error_code: str) -> ResourceRef:
        try:
            return ResourceRef.model_validate(value)
        except Exception as exc:
            raise OrchestrationProgramError({"code": error_code}) from exc

    @staticmethod
    def _callback_error(exc: Exception, fallback: str) -> OrchestrationProgramError:
        message = str(exc).strip()
        code = message.split(":", 1)[0] if message else fallback
        return OrchestrationProgramError({"code": code, "message": message[:512]})

    @staticmethod
    def _content_ref(value: Any, *, error_code: str) -> ResourceRef:
        ref = DockerOrchestrationExecutor._resource_ref(value, error_code=error_code)
        digest = ref.version_or_digest or ""
        resource_digest = ref.resource_id.removeprefix("content://sha256/")
        if (
            not ref.resource_id.startswith("content://sha256/")
            or len(digest) != 64
            or any(char not in "0123456789abcdefABCDEF" for char in digest)
            or resource_digest.lower() != digest.lower()
            or ref.identity_criterion not in {None, "content_digest"}
        ):
            raise OrchestrationProgramError({"code": error_code})
        return ref

    async def _stderr_tail(self, process: asyncio.subprocess.Process) -> tuple[str, bool]:
        if process.stderr is None:
            return "", False
        chunks: list[bytes] = []
        total = 0
        overflow = False
        while True:
            chunk = await process.stderr.read(4096)
            if not chunk:
                break
            if total < 65536:
                kept = chunk[: 65536 - total]
                chunks.append(kept)
                total += len(kept)
                if len(chunk) > len(kept):
                    overflow = True
            else:
                overflow = True
        return b"".join(chunks).decode(errors="replace"), overflow

    async def run(
        self,
        program: bytes,
        input_ref: ResourceRef,
        *,
        read_json: ReadJsonCallback,
        emit_node: EmitNodeCallback,
        result: ResultCallback,
    ) -> ResourceRef:
        try:
            return await asyncio.wait_for(
                self._run(program, input_ref, read_json=read_json, emit_node=emit_node, result=result),
                timeout=self.timeout_seconds,
            )
        except asyncio.TimeoutError as exc:
            raise OrchestrationExecutorError("orchestration_timeout") from exc

    async def _run(
        self,
        program: bytes,
        input_ref: ResourceRef,
        *,
        read_json: ReadJsonCallback,
        emit_node: EmitNodeCallback,
        result: ResultCallback,
    ) -> ResourceRef:
        source = self._validate_program(program)
        try:
            process = await asyncio.create_subprocess_exec(
                *self.docker_args(),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                limit=self.max_message_bytes,
                env=self.sandbox_environment(),
            )
        except OSError as exc:
            raise OrchestrationExecutorError("orchestrator_runtime_unavailable") from exc

        stderr_task = asyncio.create_task(self._stderr_tail(process))
        try:
            await self._write_message(
                process.stdin,
                {
                    "type": "start",
                    "program": source,
                    "input_ref": input_ref.model_dump(mode="json"),
                },
            )
            while True:
                message = await self._read_message(process.stdout)
                message_type = message.get("type")
                if message_type == "read_json":
                    ref = self._content_ref(message.get("ref"), error_code="read_json_ref_invalid")
                    try:
                        value = await self._maybe_await(read_json(ref))
                    except Exception as exc:
                        raise self._callback_error(exc, "orchestration_context_error") from exc
                    await self._write_message(process.stdin, {"type": "value", "value": value})
                elif message_type == "emit_node":
                    package_ref = self._resource_ref(message.get("package_ref"), error_code="node_package_ref_invalid")
                    input_refs = [
                        self._content_ref(item, error_code="node_input_ref_invalid")
                        for item in message.get("input_refs", [])
                    ]
                    try:
                        handle = await self._maybe_await(emit_node(package_ref, input_refs))
                    except Exception as exc:
                        raise self._callback_error(exc, "orchestration_context_error") from exc
                    if not isinstance(handle, str) or not handle:
                        raise OrchestrationExecutorError("node_handle_invalid")
                    await self._write_message(process.stdin, {"type": "handle", "handle": handle})
                elif message_type == "result":
                    handle = message.get("handle")
                    if not isinstance(handle, str) or not handle:
                        raise OrchestrationProgramError({"code": "node_handle_invalid"})
                    try:
                        returned = await self._maybe_await(result(handle))
                    except Exception as exc:
                        raise self._callback_error(exc, "orchestration_context_error") from exc
                    ref = self._content_ref(returned, error_code="node_result_ref_invalid")
                    await self._write_message(process.stdin, {"type": "ref", "ref": ref.model_dump(mode="json")})
                elif message_type == "final":
                    final_ref = self._content_ref(message.get("ref"), error_code="final_ref_invalid")
                    if process.stdin is not None:
                        process.stdin.close()
                    await process.wait()
                    _stderr, overflow = await stderr_task
                    if overflow:
                        raise OrchestrationExecutorError("orchestration_output_too_large")
                    if process.returncode != 0:
                        raise OrchestrationExecutorError("orchestration_process_failed")
                    return final_ref
                elif message_type == "failure":
                    reason = message.get("error")
                    if not isinstance(reason, dict):
                        reason = {"code": "orchestration_program_error"}
                    if process.stdin is not None:
                        process.stdin.close()
                    await process.wait()
                    _stderr, overflow = await stderr_task
                    if overflow:
                        raise OrchestrationExecutorError("orchestration_output_too_large")
                    raise OrchestrationProgramError(reason)
                else:
                    raise OrchestrationExecutorError("orchestration_message_invalid")
        finally:
            if process.returncode is None:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
                await process.wait()
            if not stderr_task.done():
                await stderr_task
