import json
from pathlib import Path

from .test_compose import make_config
from loom_v2.deployment.compose import render_project


def test_every_role_renderer_produces_parseable_compose(tmp_path: Path):
    config = make_config(tmp_path)
    for machine in ("storage", "observer", "driver", "worker-a"):
        document = json.loads(render_project(config, machine).compose_text)
        assert document["services"]
        assert all(name.replace("_", "").replace("-", "").isalnum() for name in document["services"])


def test_runtime_roles_mount_operator_package_contracts(tmp_path: Path):
    config = make_config(tmp_path)
    for machine, service in (
        ("observer", "observer"),
        ("driver", "driver"),
        ("worker-a", "slave"),
    ):
        document = json.loads(render_project(config, machine).compose_text)
        app = document["services"][service]
        assert app["environment"]["LOOM_PACKAGE_CONTRACT_DIR"] == "/opt/loom/package-contracts"
        assert "./source/package_contracts:/opt/loom/package-contracts:ro" in app["volumes"]
