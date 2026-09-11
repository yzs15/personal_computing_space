from __future__ import annotations

import asyncio
import hashlib
import json
import os
import signal
import sys
from dataclasses import dataclass, field
from typing import Any, Protocol

from loom_v2.contracts.types import ExecutionContract, ResourceRef
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
    provenance: dict[str, Any] | None = None


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

    async def invoke(self, payload: dict[str, Any], *, program: bytes | None = None) -> ExecutionResult: ...


def _result(value: Any, replay_safety: str = "Idempotent") -> ExecutionResult:
    digest = hashlib.sha256(canonical_json_bytes(value)).hexdigest()
    return ExecutionResult(
        resource_ref=ResourceRef(resource_id=f"result-{digest[:16]}", version_or_digest=digest, identity_criterion="content_digest"),
        value=value,
        replay_safety=replay_safety,
        digest=digest,
    )


class ProcessJSONStdioV1Adapter:
    descriptor = ExecutorDescriptor(kind="process:json_stdio", operations=frozenset({"run_code"}), replay_safety="DeclaredByPackage", effect_class="Sandboxed")

    def __init__(self, timeout_seconds: float | None = None) -> None:
        # Keep the timeout on the child-operation adapter, independent from
        # the coding-agent conversation deadline.  Reading the environment at
        # execution time keeps the process-level default easy to override in
        # Compose and in tests without rebuilding the registry.
        self.timeout_seconds = timeout_seconds

    def _timeout(self) -> float:
        configured = os.getenv("LOOM_CAPABILITY_OPERATION_TIMEOUT_SECONDS", "30")
        return self.timeout_seconds if self.timeout_seconds is not None else float(configured)

    async def invoke(self, payload: dict[str, Any], *, program: bytes | None = None) -> ExecutionResult:
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
        self._adapters: dict[tuple[str, str], ExecutorAdapter] = {}
        for adapter in adapters if adapters is not None else [ProcessJSONStdioV1Adapter(capability_timeout_seconds)]:
            self.register(adapter)

    def register(self, adapter: ExecutorAdapter) -> None:
        self._adapters[(adapter.descriptor.kind, adapter.descriptor.version)] = adapter

    def get(self, execution: ExecutionContract) -> ExecutorAdapter:
        try:
            return self._adapters[(execution.kind, execution.version)]
        except KeyError as exc:
            raise ValueError(f"unsupported_executor:{execution.kind}/{execution.version}") from exc

    def descriptors(self) -> list[ExecutorDescriptor]:
        return [adapter.descriptor for adapter in self._adapters.values()]

    async def invoke(self, execution: ExecutionContract, payload: dict[str, Any], *, program: bytes | None = None) -> ExecutionResult:
        adapter = self.get(execution)
        return await adapter.invoke(payload, program=program)


default_registry = ExecutorRegistry()
