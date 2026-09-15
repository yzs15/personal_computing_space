from __future__ import annotations

from typing import Any

from loom_v2.contracts.types import ResourceRef
from loom_v2.observer.repository import ObserverRepository


def register_test_descriptor(repo: ObserverRepository, resource_id: str) -> ResourceRef:
    payload = {
        "schema_version": "operation.v1",
        "resource_id": resource_id,
        "name": resource_id.rstrip("/").rsplit("/", 1)[-1],
    }
    digest = repo.register_operation_descriptor(resource_id, payload)
    return ResourceRef(
        resource_id=resource_id,
        version_or_digest=digest,
        identity_criterion="descriptor_digest",
    )


def capability_export(
    descriptor_ref: ResourceRef | dict[str, Any],
    io_contract_ref: ResourceRef | dict[str, Any],
    *,
    effect_class: str = "Sandboxed",
    permissions: list[str] | None = None,
    replay_safety: str = "DeclaredByPackage",
    runtime_binding: dict[str, Any] | None = None,
) -> dict[str, Any]:
    def dump(value: ResourceRef | dict[str, Any]) -> dict[str, Any]:
        return value.model_dump(mode="json") if isinstance(value, ResourceRef) else value

    return {
        "capability_descriptor_ref": dump(descriptor_ref),
        "io_contract_ref": dump(io_contract_ref),
        "effect_class": effect_class,
        "permissions": list(permissions or []),
        "replay_safety": replay_safety,
        "runtime_binding": dict(runtime_binding or {}),
    }


def process_package_value(
    repo: ObserverRepository,
    *,
    operation_ref: str,
    program_content_ref: ResourceRef | dict[str, Any],
    io_contract_ref: ResourceRef | dict[str, Any],
    package_id: str,
    package_version: str = "v1",
    permissions: list[str] | None = None,
    replay_safety: str = "DeclaredByPackage",
    body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    program_ref = (
        program_content_ref.model_dump(mode="json")
        if isinstance(program_content_ref, ResourceRef)
        else program_content_ref
    )
    return {
        "package_id": package_id,
        "package_version": package_version,
        "package_type": "function",
        "execution": {"kind": "process:json_stdio", "version": "1"},
        "capability_exports": [
            capability_export(
                register_test_descriptor(repo, operation_ref),
                io_contract_ref,
                permissions=permissions,
                replay_safety=replay_safety,
            )
        ],
        "body": {"program_content_ref": program_ref, **dict(body or {})},
    }


def orchestration_package_value(
    repo: ObserverRepository,
    *,
    operation_ref: str,
    program_content_ref: ResourceRef | dict[str, Any],
    io_contract_ref: ResourceRef | dict[str, Any],
    allowed_node_package_refs: list[ResourceRef | dict[str, Any]],
    package_id: str,
    package_version: str = "v1",
    max_nodes: int = 10,
    max_live_nodes: int = 2,
    body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    def dump(value: ResourceRef | dict[str, Any]) -> dict[str, Any]:
        return value.model_dump(mode="json") if isinstance(value, ResourceRef) else value

    return {
        "package_id": package_id,
        "package_version": package_version,
        "package_type": "function",
        "execution": {"kind": "container:python_orchestrator", "version": "1"},
        "capability_exports": [
            capability_export(
                register_test_descriptor(repo, operation_ref),
                io_contract_ref,
                replay_safety="DeterministicByEventLog",
            )
        ],
        "body": {
            "program_content_ref": dump(program_content_ref),
            "allowed_node_package_refs": [dump(item) for item in allowed_node_package_refs],
            "max_nodes": max_nodes,
            "max_live_nodes": max_live_nodes,
            **dict(body or {}),
        },
    }
