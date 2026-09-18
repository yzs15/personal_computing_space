"""Observer-side activation command execution boundary.

The repository remains the authority for desired/actual state and fencing.
This module only coordinates already-selected local workers/Slaves and turns
their reports into the API's health-report list.
"""

from __future__ import annotations

from typing import Any

from loom_v2.contracts.types import (
    CapabilityDeprovisionCommand,
    CapabilityPackageVersion,
    CapabilityProvisionCommand,
)


async def execute_activation_targets(
    repository: Any,
    package: CapabilityPackageVersion,
    targets: list[Any],
    *,
    desired_state: str,
    workspace_id: str,
    workers: dict[str, Any],
    slaves: dict[str, Any],
    compute_binding: dict[str, Any] | None = None,
    idempotency_key: str | None = None,
) -> list[dict[str, Any]]:
    """Execute one promote/deactivate command for each selected target.

    Target selection and forwarding remain outside this helper.  That keeps
    the embedded test adapter and the production Driver gateway distinct while
    removing the duplicate command/report plumbing.
    """

    reports: list[dict[str, Any]] = []
    for raw_target in targets:
        target = str(raw_target)
        action = "promote" if desired_state == "running" else "deactivate"
        requested_key = idempotency_key or f"{action}-{package.package_digest}-{target}"
        desired = await repository.set_capability_desired(
            package.version_ref,
            target,
            desired_state=desired_state,
            idempotency_key=requested_key,
            workspace_id=workspace_id,
            activation_closure_version_ref=(
                package.package_closure_version_ref if desired_state == "running" else ""
            ),
            compute_binding_ref=str((compute_binding or {}).get("binding_id") or ""),
        )
        command_values: dict[str, Any] = {
            "command_id": f"{'provision' if desired_state == 'running' else 'deprovision'}-{package.package_id}-{target}",
            "package_version_ref": package.version_ref,
            "package_digest": package.package_digest,
            "target_slave": target,
            "workspace_id": workspace_id,
            "idempotency_key": desired.last_idempotency_key,
            "activation_revision": desired.activation_revision,
        }
        if desired_state == "running":
            command_values.update(
                {
                    "activation_closure_version_ref": package.package_closure_version_ref,
                    "compute_binding": compute_binding,
                }
            )
            command = CapabilityProvisionCommand.model_validate(command_values)
        elif desired_state == "stopped":
            command = CapabilityDeprovisionCommand.model_validate(command_values)
        else:
            raise ValueError("unsupported_activation_state")
        try:
            worker = workers.get(target)
            slave = slaves.get(target)
            if worker is not None:
                report = (
                    await worker.provision(command=command, package=package)
                    if desired_state == "running"
                    else await worker.deprovision(command=command, package=package)
                )
            elif slave is not None:
                report = (
                    await slave.provision(command, package)
                    if desired_state == "running"
                    else await slave.deprovision(command, package)
                )
            else:
                raise RuntimeError("slave_not_found")
            await repository.record_capability_health(report)
            reports.append(report.model_dump(mode="json"))
        except (RuntimeError, ValueError) as exc:
            reports.append(
                {
                    "target_slave": target,
                    "activation_state": "failed",
                    "error": str(exc),
                }
            )
    return reports


__all__ = ["execute_activation_targets"]
