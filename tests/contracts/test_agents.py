import pytest
from pydantic import ValidationError

from loom_v2.contracts.agents import AgentRegistration, DriverCommand, DriverThreadBinding


def test_driver_registration_requires_role_identity_and_endpoint():
    registration = AgentRegistration(
        role="driver",
        agent_id="driver-default",
        instance_id="instance-1",
        workspace_id="workspace-default",
        endpoint_url="http://driver:8090",
        protocol_version="driver.v1",
    )

    assert registration.role == "driver"
    assert registration.capabilities == {}


def test_driver_registration_rejects_unknown_role():
    with pytest.raises(ValidationError):
        AgentRegistration(
            role="observer",
            agent_id="observer-default",
            instance_id="instance-1",
            workspace_id="workspace-default",
            endpoint_url="http://observer:8080",
            protocol_version="agent.v1",
        )


def test_driver_command_requires_current_epoch_fields():
    command = DriverCommand(
        request_id="request-1",
        driver_id="driver-default",
        instance_id="instance-1",
        lease_id="lease-1",
        driver_epoch=4,
        command="run.get",
    )

    assert command.driver_epoch == 4
    assert command.arguments == {}


def test_driver_thread_binding_round_trips_thread_id():
    binding = DriverThreadBinding(
        workspace_id="workspace-default",
        conversation_ref="conversation-1",
        thread_id="thread-1",
        model="deepseek-v4-flash",
        workspace_root="/workspace",
        turn_state="idle",
        driver_epoch=2,
    )

    restored = DriverThreadBinding.model_validate(binding.model_dump())
    assert restored.thread_id == "thread-1"
    assert restored.turn_state == "idle"


def test_driver_thread_binding_requires_conversation_and_thread_identity():
    with pytest.raises(ValidationError):
        DriverThreadBinding(
            workspace_id="workspace-default",
            conversation_ref="",
            thread_id="thread-1",
            model="deepseek-v4-flash",
            workspace_root="/workspace",
            driver_epoch=1,
        )
