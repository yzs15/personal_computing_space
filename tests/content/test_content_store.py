from __future__ import annotations

import hashlib

import boto3
import pytest
from moto import mock_aws

from loom_v2.content_store import ContentStore, ContentStat


ENDPOINT = "https://s3.amazonaws.com"
BUCKET = "loom-test"
ACCESS_KEY = "test-access"
SECRET_KEY = "test-secret"


@pytest.fixture
def aws_store() -> ContentStore:
    with mock_aws():
        yield ContentStore(
            endpoint_url=ENDPOINT,
            bucket=BUCKET,
            access_key=ACCESS_KEY,
            secret_key=SECRET_KEY,
            region="us-east-1",
            prefix="objects",
        )


@pytest.mark.asyncio
async def test_put_is_content_addressed_and_idempotent(aws_store: ContentStore) -> None:
    body = b"hello loom"

    first = await aws_store.put(body, media_type="text/plain")
    second = await aws_store.put(body, media_type="text/plain")

    digest = hashlib.sha256(body).hexdigest()
    assert first.resource_id == f"content://sha256/{digest}"
    assert first.version_or_digest == digest
    assert second == first
    assert first.identity_criterion == "content_digest"
    assert "endpoint_url" not in first.access_binding
    assert "bucket" not in first.access_binding
    assert "key" not in first.access_binding
    assert first.access_binding.get("media_type") == "text/plain"


def test_put_has_no_resource_id_argument() -> None:
    import inspect

    assert "resource_id" not in inspect.signature(ContentStore.put).parameters


@pytest.mark.asyncio
async def test_existing_object_cannot_be_overwritten(aws_store: ContentStore) -> None:
    body = b"immutable"
    ref = await aws_store.put(body, media_type="text/plain")
    with pytest.raises(ValueError, match="content_integrity_conflict"):
        await aws_store.put(body, media_type="application/json")
    assert await aws_store.get(ref) == body


@pytest.mark.asyncio
async def test_get_exists_and_stat_verify_digest(aws_store: ContentStore) -> None:
    body = b"stat me"
    ref = await aws_store.put(body, media_type="application/octet-stream")

    assert await aws_store.exists(ref)
    assert await aws_store.exists("sha256:" + ref.version_or_digest)
    stat = await aws_store.stat(ref)
    assert isinstance(stat, ContentStat)
    assert stat.size == len(body)
    assert stat.declared_digest == ref.version_or_digest
    assert stat.media_type == "application/octet-stream"
    assert stat.integrity_verified is True
    assert await aws_store.get(ref, expected_digest=ref.version_or_digest) == body


@pytest.mark.asyncio
async def test_get_detects_expected_digest_and_body_mismatch(aws_store: ContentStore) -> None:
    ref = await aws_store.put(b"correct", media_type="text/plain")
    with pytest.raises(ValueError, match="content_digest_mismatch"):
        await aws_store.get(ref, expected_digest="0" * 64)

    client = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name="us-east-1",
    )
    client.delete_object(Bucket=BUCKET, Key=f"objects/{ref.version_or_digest}")
    client.put_object(
        Bucket=BUCKET,
        Key=f"objects/{ref.version_or_digest}",
        Body=b"tampered",
        ContentType="text/plain",
        Metadata={"sha256": ref.version_or_digest},
    )
    with pytest.raises(ValueError, match="content_digest_mismatch"):
        await aws_store.get(ref)


@pytest.mark.asyncio
async def test_missing_content_returns_false_or_none(aws_store: ContentStore) -> None:
    missing = "0" * 64
    assert await aws_store.exists(f"sha256:{missing}") is False
    assert await aws_store.stat(f"sha256:{missing}") is None


def test_missing_configuration_is_rejected() -> None:
    with pytest.raises(ValueError, match="s3_configuration_missing"):
        ContentStore(endpoint_url=None, bucket="loom", access_key=None, secret_key=None)
