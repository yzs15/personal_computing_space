"""Pure helpers for capability/resource references.

These helpers intentionally do not access ContentStore or perform network I/O.
They only normalize identities and select already-loaded contract objects.
"""

from __future__ import annotations

import re
from typing import Any

from .types import (
    CapabilityExport,
    CapabilityPackageVersion,
    NodeInputBinding,
    ResourceRef,
    TaskClosure,
)


_SHA256 = re.compile(r"^[0-9a-fA-F]{64}$")
_CONTENT_PREFIX = "content://sha256/"


def operation_name(operation_ref: str) -> str:
    if not operation_ref:
        return ""
    return operation_ref.rstrip("/").rsplit("/", 1)[-1].rsplit(":", 1)[-1]


def input_binding_for(snapshot: TaskClosure, operation_ref: str) -> NodeInputBinding | None:
    candidates = {operation_ref, operation_name(operation_ref), "default"}
    return next(
        (binding for binding in snapshot.node_input_bindings if binding.node_id in candidates),
        None,
    )


def package_body_ref(package: CapabilityPackageVersion, field_name: str) -> ResourceRef:
    try:
        return ResourceRef.model_validate(package.body[field_name])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"{field_name}_required") from exc


def single_export(package: CapabilityPackageVersion) -> CapabilityExport:
    if len(package.capability_exports) != 1:
        raise ValueError("capability_export_selection_required")
    return package.capability_exports[0]


def select_slave_agents(agents: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    selected: dict[str, dict[str, Any]] = {}
    for agent in agents:
        slave_id = str(agent.get("agent_id") or "")
        if not slave_id:
            continue
        current = selected.get(slave_id)
        if current is None or (
            agent.get("lease_state") == "active"
            and current.get("lease_state") != "active"
        ):
            selected[slave_id] = dict(agent)
    return selected


def slave_supports_package(
    slave_id: str,
    package: CapabilityPackageVersion,
    *,
    agents: dict[str, dict[str, Any]],
    capabilities: dict[str, dict[str, Any]],
) -> bool:
    agent = agents.get(slave_id)
    if agent is None or agent.get("lease_state") != "active":
        return False
    details = capabilities.get(slave_id, {})
    declared_executors = (
        details.get("executor_kinds")
        or details.get("executors")
        or details.get("executor_descriptors")
    )
    if declared_executors:
        raw_executors = [declared_executors] if isinstance(declared_executors, str) else declared_executors
        if any(
            isinstance(item, dict)
            and (
                str(item.get("package_type") or ""),
                str(item.get("kind") or ""),
                str(item.get("version") or "1"),
            )
            == (package.package_type, package.execution.kind, package.execution.version)
            for item in raw_executors
        ):
            return True
    descriptors = details.get("runtime_plugin_descriptors") or details.get("runtime_plugins") or []
    for descriptor in descriptors if isinstance(descriptors, list) else []:
        supports = descriptor.get("supports", []) if isinstance(descriptor, dict) else []
        for support in supports if isinstance(supports, list) else []:
            if not isinstance(support, dict):
                continue
            execution_value = support.get("execution")
            execution: dict[str, Any] = execution_value if isinstance(execution_value, dict) else support
            if (
                str(support.get("package_type") or "") == package.package_type
                and str(support.get("execution_kind") or execution.get("kind") or "") == package.execution.kind
                and str(support.get("execution_version") or execution.get("version") or "1") == package.execution.version
            ):
                return True
    return False


def require_content_ref(ref: ResourceRef) -> None:
    if not ref.resource_id.startswith(_CONTENT_PREFIX):
        raise ValueError("content_ref_required")
    resource_digest = ref.resource_id.removeprefix(_CONTENT_PREFIX)
    digest = ref.digest or ""
    if not _SHA256.fullmatch(digest):
        raise ValueError("content_ref_required")
    if resource_digest.lower() != digest.lower():
        raise ValueError("content_ref_digest_mismatch")
    if ref.identity_criterion not in {None, "content_digest"}:
        raise ValueError("content_ref_required")


def is_content_ref(ref: ResourceRef) -> bool:
    try:
        require_content_ref(ref)
    except ValueError:
        return False
    return True


__all__ = [
    "input_binding_for",
    "is_content_ref",
    "operation_name",
    "package_body_ref",
    "require_content_ref",
    "select_slave_agents",
    "slave_supports_package",
    "single_export",
]
