from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from loom_v2.contracts.types import ResourceRef


@dataclass
class ExecutionResult:
    resource_ref: ResourceRef
    value: Any
    replay_safety: str
    digest: str


async def execute_operation(operation: str, payload: dict[str, Any]) -> ExecutionResult:
    if operation == "echo":
        value = payload
    elif operation == "hash":
        raw = payload.get("text", "").encode()
        value = {"sha256": hashlib.sha256(raw).hexdigest()}
    elif operation == "sort":
        value = {"items": sorted(payload.get("items", []))}
    else:
        raise ValueError(f"unsupported_operation:{operation}")
    digest = hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return ExecutionResult(
        resource_ref=ResourceRef(resource_id=f"result-{digest[:16]}", version_or_digest=digest, identity_criterion="content_digest"),
        value=value,
        replay_safety="Idempotent",
        digest=digest,
    )
