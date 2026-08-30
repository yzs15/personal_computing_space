from __future__ import annotations

import asyncio
import hashlib
import json
import os
import signal
import sys
from dataclasses import dataclass, field
from typing import Any, Protocol

from loom_v2.contracts.types import ResourceRef
from loom_v2.content_store import canonical_json_bytes


@dataclass
class ExecutionResult:
    resource_ref: ResourceRef
    value: Any
    replay_safety: str
    digest: str
    terminal_state: str = "completed"
    terminal_error: dict[str, Any] | None = None
    validation_evidence: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class ExecutorDescriptor:
    kind: str
    version: str = "1"
    operations: frozenset[str] = frozenset()
    replay_safety: str = "Idempotent"
    effect_class: str = "Pure"

    @property
    def descriptor_ref(self) -> str:
        return f"executor://{self.kind}/{self.version}"

    @property
    def digest(self) -> str:
        payload = json.dumps({"kind": self.kind, "version": self.version, "operations": sorted(self.operations), "replay_safety": self.replay_safety, "effect_class": self.effect_class}, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(payload).hexdigest()


class ExecutorAdapter(Protocol):
    descriptor: ExecutorDescriptor

    async def execute(self, operation: str, payload: dict[str, Any], *, program: bytes | None = None) -> ExecutionResult: ...


def _result(value: Any, replay_safety: str = "Idempotent") -> ExecutionResult:
    digest = hashlib.sha256(canonical_json_bytes(value)).hexdigest()
    return ExecutionResult(
        resource_ref=ResourceRef(resource_id=f"result-{digest[:16]}", version_or_digest=digest, identity_criterion="content_digest"),
        value=value,
        replay_safety=replay_safety,
        digest=digest,
    )


class BuiltinV1Adapter:
    descriptor = ExecutorDescriptor(kind="builtin_v1", operations=frozenset({"echo", "hash", "sort", "run_code"}))

    async def execute(self, operation: str, payload: dict[str, Any], *, program: bytes | None = None) -> ExecutionResult:
        if operation == "echo":
            return _result(payload)
        if operation == "hash":
            return _result({"sha256": hashlib.sha256(str(payload.get("text", "")).encode()).hexdigest()})
        if operation == "sort":
            return _result({"items": sorted(payload.get("items", []))})
        if operation == "run_code":
            raise ValueError("run_code_requires_subprocess_adapter")
        raise ValueError(f"unsupported_operation:{operation}")


class SubprocessJSONV1Adapter:
    descriptor = ExecutorDescriptor(kind="subprocess_json_v1", operations=frozenset({"run_code"}), replay_safety="DeclaredByPackage", effect_class="Sandboxed")

    def __init__(self, timeout_seconds: float | None = None) -> None:
        # Keep the timeout on the child-operation adapter, independent from
        # the coding-agent conversation deadline.  Reading the environment at
        # execution time keeps the process-level default easy to override in
        # Compose and in tests without rebuilding the registry.
        self.timeout_seconds = timeout_seconds

    def _timeout(self) -> float:
        configured = os.getenv("LOOM_CAPABILITY_OPERATION_TIMEOUT_SECONDS", "30")
        return self.timeout_seconds if self.timeout_seconds is not None else float(configured)

    async def execute(self, operation: str, payload: dict[str, Any], *, program: bytes | None = None) -> ExecutionResult:
        if operation != "run_code":
            raise ValueError(f"unsupported_operation:{operation}")
        if not program:
            raise ValueError("program_required")
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-I", "-c", program.decode("utf-8"),
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        try:
            stdout, _stderr = await asyncio.wait_for(
                proc.communicate(json.dumps(payload, ensure_ascii=False).encode() + b"\n"),
                timeout=self._timeout(),
            )
        except asyncio.TimeoutError as exc:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            await proc.wait()
            raise RuntimeError("capability_timeout") from exc
        if proc.returncode != 0:
            raise RuntimeError("capability_exec_error")
        try:
            value = json.loads(stdout.decode().strip() or "null")
        except json.JSONDecodeError as exc:
            raise RuntimeError("subprocess_invalid_json") from exc
        return _result(value, replay_safety=self.descriptor.replay_safety)


class ExecutorRegistry:
    def __init__(self, adapters: list[ExecutorAdapter] | None = None, *, capability_timeout_seconds: float | None = None) -> None:
        self._adapters: dict[str, ExecutorAdapter] = {}
        for adapter in adapters or [BuiltinV1Adapter(), SubprocessJSONV1Adapter(capability_timeout_seconds)]:
            self.register(adapter)

    def register(self, adapter: ExecutorAdapter | str, implementation: ExecutorAdapter | None = None) -> None:
        # Accept both ``register(adapter)`` and ``register(kind, adapter)``
        # for small integrations that keep an explicit registry key.
        value = implementation if implementation is not None else adapter
        if isinstance(value, str):
            raise TypeError("executor_adapter_required")
        self._adapters[value.descriptor.kind if not isinstance(adapter, str) else adapter] = value

    def get(self, kind: str) -> ExecutorAdapter:
        try:
            return self._adapters[kind]
        except KeyError as exc:
            if kind in FUTURE_EXECUTOR_KINDS:
                raise ValueError(f"unsupported_executor_extension:{kind}") from exc
            raise ValueError(f"unsupported_executor:{kind}") from exc

    def descriptors(self) -> list[ExecutorDescriptor]:
        return [adapter.descriptor for adapter in self._adapters.values()]

    async def execute(self, executor_kind: str, operation: str, payload: dict[str, Any], *, program: bytes | None = None) -> ExecutionResult:
        adapter = self.get(executor_kind)
        if operation not in adapter.descriptor.operations:
            raise ValueError(f"unsupported_operation:{operation}")
        return await adapter.execute(operation, payload, program=program)


default_registry = ExecutorRegistry()

FUTURE_EXECUTOR_KINDS = frozenset({"http_service_v1", "grpc_service_v1", "mcp_v1"})

# Public aliases mirror the wire-level adapter kinds used in the design.
builtin_v1 = BuiltinV1Adapter
subprocess_json_v1 = SubprocessJSONV1Adapter


async def execute_operation(operation: str, payload: dict[str, Any]) -> ExecutionResult:
    return await default_registry.execute("builtin_v1", operation, payload)
