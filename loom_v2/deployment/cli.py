"""Command-line orchestration for remote Loom role deployment."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

from .config import DeploymentConfig, DeploymentConfigError
from .remote import DeploymentFailure, RemoteDeployer


def main(argv: list[str] | None = None, *, forced_target: str | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog=f"loom-deploy-{forced_target or 'cluster'}",
        description="Upload Loom source over SSH and start a remote Docker Compose project. Targets need Docker Engine, the Compose plugin, and passwordless Docker for the SSH user.",
    )
    parser.add_argument("--config", required=True, type=Path, help="TOML deployment inventory")
    parser.add_argument("--source-root", type=Path, default=_repository_root(), help="repository to upload (default: this checkout)")
    parser.add_argument("--dry-run", action="store_true", help="render plans without opening SSH or changing remote state")
    parser.add_argument("--id", help="Slave service ID (slave-a or slave-b)")
    parser.add_argument("--command-timeout", type=_positive_float, default=900.0, help="SSH command/build timeout in seconds (default: 900)")
    parser.add_argument("--health-timeout", type=_positive_float, default=300.0, help="role health timeout in seconds (default: 300)")
    parser.add_argument("--health-interval", type=_positive_float, default=2.0, help="seconds between health probes (default: 2)")
    args = parser.parse_args(argv)

    try:
        config = DeploymentConfig.from_file(args.config)
        targets = _select_targets(config, forced_target, args.id)
        deployer = RemoteDeployer(
            config,
            source_root=args.source_root,
            command_timeout=args.command_timeout,
            health_timeout=args.health_timeout,
            health_interval=args.health_interval,
        )
        for target in targets:
            plan = deployer.deploy(target, dry_run=args.dry_run)
            if args.dry_run:
                print(f"{plan.machine} ({plan.role}) project={plan.project_name}")
                print(plan.preview)
                print()
    except (DeploymentConfigError, DeploymentFailure) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    return 0


def _select_targets(config: DeploymentConfig, forced_target: str | None, slave_id: str | None) -> list[str]:
    if forced_target is None:
        if slave_id is not None:
            raise DeploymentConfigError("--id is only valid for deploy_slave.py")
        ordered = [config.machine("minio").name, config.machine("observer").name]
        ordered.extend(item.name for item in config.slaves)
        ordered.append(config.machine("driver").name)
        return ordered
    if forced_target == "slave":
        if slave_id is None:
            raise DeploymentConfigError("--id is required for deploy_slave.py")
        matches = [item for item in config.slaves if item.service_id == slave_id]
        if len(matches) != 1:
            raise DeploymentConfigError(f"unknown slave id: {slave_id}")
        return [matches[0].name]
    if slave_id is not None:
        raise DeploymentConfigError("--id is only valid for deploy_slave.py")
    return [config.machine(forced_target).name]


def _repository_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed
