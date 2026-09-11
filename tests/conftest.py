"""Hermetic defaults for tests that construct the application at import time."""

import os
import shutil
import subprocess

import pytest
from moto import mock_aws


def pytest_collection_modifyitems(config, items):
    """External Compose tests are opt-in when the Docker daemon is present."""
    docker = shutil.which("docker")
    available = False
    if docker:
        try:
            available = subprocess.run([docker, "info"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=2).returncode == 0
        except (OSError, subprocess.SubprocessError):
            available = False
    if available:
        return
    skip = pytest.mark.skip(reason="Docker daemon/Compose stack is unavailable")
    for item in items:
        if "integration" in item.keywords or "e2e" in item.keywords:
            item.add_marker(skip)


os.environ.setdefault("LOOM_S3_ENDPOINT_URL", "https://s3.amazonaws.com")
os.environ.setdefault("LOOM_S3_BUCKET", "loom-test")
os.environ.setdefault("LOOM_S3_ACCESS_KEY", "test-access")
os.environ.setdefault("LOOM_S3_SECRET_KEY", "test-secret")


@pytest.fixture(autouse=True)
def mock_s3_backend():
    """Keep application-level S3 calls hermetic without affecting subprocesses."""
    with mock_aws():
        yield
