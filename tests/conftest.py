"""Hermetic defaults for tests that construct the application at import time."""

import os

import pytest
from moto import mock_aws


os.environ.setdefault("LOOM_S3_ENDPOINT_URL", "https://s3.amazonaws.com")
os.environ.setdefault("LOOM_S3_BUCKET", "loom-test")
os.environ.setdefault("LOOM_S3_ACCESS_KEY", "test-access")
os.environ.setdefault("LOOM_S3_SECRET_KEY", "test-secret")


@pytest.fixture(autouse=True)
def mock_s3_backend():
    """Keep application-level S3 calls hermetic without affecting subprocesses."""
    with mock_aws():
        yield
