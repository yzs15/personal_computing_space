from __future__ import annotations

from typing import Any, Literal

from pydantic import Field, model_validator

from .types import ContractModel


class AgentRegistration(ContractModel):
    role: Literal["driver", "slave"]
    agent_id: str = Field(min_length=1)
    instance_id: str = Field(min_length=1)
    workspace_id: str = Field(min_length=1)
    endpoint_url: str = Field(min_length=1)
    protocol_version: str = Field(min_length=1)
    capabilities: dict[str, Any] = Field(default_factory=dict)


class AgentLease(ContractModel):
    agent_id: str = Field(min_length=1)
    instance_id: str = Field(min_length=1)
    workspace_id: str = Field(min_length=1)
    lease_id: str = Field(min_length=1)
    epoch: int
    driver_epoch: int | None = None
    heartbeat_interval_seconds: float = Field(default=5.0, gt=0)
    thread_bindings: list[dict[str, Any]] = Field(default_factory=list)
    recovery_runs: list[dict[str, Any]] = Field(default_factory=list)

    @model_validator(mode="after")
    def normalize_driver_epoch(self) -> "AgentLease":
        if self.driver_epoch is None:
            self.driver_epoch = self.epoch
        elif self.driver_epoch != self.epoch:
            raise ValueError("driver_epoch_mismatch")
        return self


class DriverCommand(ContractModel):
    request_id: str = Field(min_length=1)
    driver_id: str = Field(min_length=1)
    instance_id: str = Field(min_length=1)
    lease_id: str = Field(min_length=1)
    driver_epoch: int
    command: str = Field(min_length=1)
    arguments: dict[str, Any] = Field(default_factory=dict)


class DriverThreadBinding(ContractModel):
    workspace_id: str = Field(min_length=1)
    conversation_ref: str = Field(min_length=1)
    thread_id: str = Field(min_length=1)
    model: str = Field(min_length=1)
    workspace_root: str = Field(min_length=1)
    last_turn_id: str | None = None
    turn_state: Literal[
        "idle",
        "starting",
        "in_progress",
        "completed",
        "interrupted",
        "recovery_pending",
    ] = "idle"
    active_request_id: str | None = None
    driver_epoch: int


class DriverRequestReceipt(ContractModel):
    request_id: str
    command: str
    response: dict[str, Any]
