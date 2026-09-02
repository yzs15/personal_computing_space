"""Asynchronous, immutable, content-addressed S3/MinIO storage."""

from __future__ import annotations

import asyncio
import hashlib
import re
from dataclasses import dataclass
from typing import Any

import boto3
import rfc8785
from botocore.exceptions import ClientError

from loom_v2.contracts.types import ResourceRef


_DIGEST_RE = re.compile(r"^[0-9a-fA-F]{64}$")


def canonical_json_bytes(value: Any) -> bytes:
    """Return RFC 8785/JCS bytes for a JSON-compatible Python value."""

    try:
        return rfc8785.dumps(value)
    except (TypeError, ValueError, rfc8785.CanonicalizationError) as exc:
        raise ValueError("json_canonicalization_error") from exc


@dataclass(frozen=True)
class ContentStat:
    size: int
    declared_digest: str | None
    media_type: str
    integrity_verified: bool


class _NotFound(Exception):
    """Internal marker used to keep S3 error mapping deliberately narrow."""


class ContentStore:
    """Single S3-backed content store used by Observer and Slaves."""

    def __init__(
        self,
        *,
        endpoint_url: str | None,
        bucket: str,
        access_key: str | None,
        secret_key: str | None,
        region: str = "us-east-1",
        prefix: str = "",
    ) -> None:
        if not endpoint_url or not bucket or not access_key or not secret_key:
            raise ValueError("s3_configuration_missing")
        self.endpoint_url = endpoint_url.rstrip("/")
        self.bucket = bucket
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region or "us-east-1"
        self.prefix = prefix.strip("/")
        self._client: Any | None = None
        self._bucket_ready = False
        self._bucket_lock = asyncio.Lock()

    @classmethod
    def from_settings(cls, settings: Any) -> "ContentStore":
        """Construct a store from a Loom ``Settings`` instance."""
        return cls(
            endpoint_url=settings.s3_endpoint_url,
            bucket=settings.s3_bucket,
            access_key=settings.s3_access_key,
            secret_key=settings.s3_secret_key,
            region=settings.s3_region,
            prefix=settings.s3_prefix,
        )

    @staticmethod
    def digest(content: bytes) -> str:
        return hashlib.sha256(content).hexdigest()

    @staticmethod
    def _normalize_digest(value: str | None) -> str:
        raw = (value or "").strip()
        if raw.startswith("content://sha256/"):
            raw = raw.removeprefix("content://sha256/")
        elif raw.startswith("sha256:"):
            raw = raw.removeprefix("sha256:")
        if not _DIGEST_RE.fullmatch(raw):
            raise ValueError("invalid_content_digest")
        return raw.lower()

    @classmethod
    def _digest_from_ref(cls, ref: ResourceRef | str) -> str:
        if isinstance(ref, ResourceRef):
            if ref.identity_criterion not in {None, "content_digest"}:
                raise ValueError("invalid_content_digest")
            if ref.version_or_digest and ref.resource_id.startswith("content://sha256/"):
                resource_digest = cls._normalize_digest(ref.resource_id)
                version_digest = cls._normalize_digest(ref.version_or_digest)
                if resource_digest != version_digest:
                    raise ValueError("content_digest_mismatch")
                return version_digest
            return cls._normalize_digest(ref.version_or_digest or ref.resource_id)
        return cls._normalize_digest(ref)

    def _key(self, digest: str) -> str:
        return f"{self.prefix}/{digest}" if self.prefix else digest

    def _sync_client(self) -> Any:
        if self._client is None:
            self._client = boto3.client(
                "s3",
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name=self.region,
            )
        return self._client

    @staticmethod
    def _error_code(exc: ClientError) -> str:
        return str(exc.response.get("Error", {}).get("Code", ""))

    @classmethod
    def _is_not_found(cls, exc: ClientError) -> bool:
        return cls._error_code(exc) in {"404", "NoSuchKey", "NoSuchBucket", "NotFound"}

    @classmethod
    def _is_precondition_failed(cls, exc: ClientError) -> bool:
        return cls._error_code(exc) in {"412", "PreconditionFailed", "ConditionalRequestConflict"}

    def _ensure_bucket_sync(self) -> None:
        if self._bucket_ready:
            return
        client = self._sync_client()
        try:
            client.head_bucket(Bucket=self.bucket)
        except ClientError as exc:
            if not self._is_not_found(exc):
                if self._error_code(exc) not in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
                    raise
                self._bucket_ready = True
                return
            params: dict[str, Any] = {"Bucket": self.bucket}
            if self.region != "us-east-1":
                params["CreateBucketConfiguration"] = {"LocationConstraint": self.region}
            try:
                client.create_bucket(**params)
            except ClientError as create_exc:
                if self._error_code(create_exc) not in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
                    raise
        self._bucket_ready = True

    async def _ensure_bucket(self) -> None:
        async with self._bucket_lock:
            if not self._bucket_ready:
                await asyncio.to_thread(self._ensure_bucket_sync)

    def _head_sync(self, digest: str) -> dict[str, Any]:
        self._ensure_bucket_sync()
        try:
            return self._sync_client().head_object(Bucket=self.bucket, Key=self._key(digest))
        except ClientError as exc:
            if self._is_not_found(exc):
                raise _NotFound from exc
            raise

    @staticmethod
    def _metadata_digest(metadata: dict[str, Any]) -> str | None:
        for key, value in metadata.items():
            if key.lower() == "sha256":
                return str(value).lower()
        return None

    def _validate_head(self, head: dict[str, Any], *, digest: str, size: int, media_type: str) -> None:
        declared = self._metadata_digest(head.get("Metadata") or {})
        actual_type = str(head.get("ContentType") or "application/octet-stream")
        actual_size = int(head.get("ContentLength", -1))
        if declared != digest or actual_size != size or actual_type != media_type:
            raise ValueError("content_integrity_conflict")

    async def put(self, content: bytes, *, media_type: str = "application/octet-stream") -> ResourceRef:
        if not isinstance(content, bytes):
            raise TypeError("content_must_be_bytes")
        digest = self.digest(content)
        size = len(content)
        key = self._key(digest)
        await self._ensure_bucket()

        try:
            head = await asyncio.to_thread(self._head_sync, digest)
        except _NotFound:
            head = None
        if head is not None:
            self._validate_head(head, digest=digest, size=size, media_type=media_type)
        else:
            def create() -> None:
                self._ensure_bucket_sync()
                self._sync_client().put_object(
                    Bucket=self.bucket,
                    Key=key,
                    Body=content,
                    ContentType=media_type,
                    ContentLength=size,
                    Metadata={"sha256": digest},
                    IfNoneMatch="*",
                )

            try:
                await asyncio.to_thread(create)
            except ClientError as exc:
                if not self._is_precondition_failed(exc):
                    raise
                head = await asyncio.to_thread(self._head_sync, digest)
                self._validate_head(head, digest=digest, size=size, media_type=media_type)

        return ResourceRef(
            resource_id=f"content://sha256/{digest}",
            version_or_digest=digest,
            identity_criterion="content_digest",
            access_binding={"media_type": media_type},
        )

    def _get_sync(self, digest: str) -> bytes:
        self._ensure_bucket_sync()
        try:
            response = self._sync_client().get_object(Bucket=self.bucket, Key=self._key(digest))
        except ClientError as exc:
            if self._is_not_found(exc):
                raise _NotFound from exc
            raise
        body = response["Body"]
        try:
            return body.read()
        finally:
            body.close()

    async def get(self, ref: ResourceRef | str, *, expected_digest: str | None = None) -> bytes:
        digest = self._digest_from_ref(ref)
        if expected_digest is None:
            expected = None
        else:
            try:
                expected = self._normalize_digest(expected_digest)
            except ValueError as exc:
                raise ValueError("content_digest_mismatch") from exc
        try:
            content = await asyncio.to_thread(self._get_sync, digest)
        except _NotFound as exc:
            raise FileNotFoundError("content_not_found") from exc
        actual = self.digest(content)
        if actual != digest or (expected is not None and actual != expected):
            raise ValueError("content_digest_mismatch")
        return content

    def _stat_sync(self, digest: str) -> ContentStat:
        head = self._head_sync(digest)
        declared = self._metadata_digest(head.get("Metadata") or {})
        media_type = str(head.get("ContentType") or "application/octet-stream")
        return ContentStat(
            size=int(head.get("ContentLength", 0)),
            declared_digest=declared,
            media_type=media_type,
            integrity_verified=declared == digest,
        )

    async def exists(self, ref: ResourceRef | str) -> bool:
        digest = self._digest_from_ref(ref)
        try:
            await asyncio.to_thread(self._head_sync, digest)
            return True
        except _NotFound:
            return False

    async def stat(self, ref: ResourceRef | str) -> ContentStat | None:
        digest = self._digest_from_ref(ref)
        try:
            return await asyncio.to_thread(self._stat_sync, digest)
        except _NotFound:
            return None


__all__ = ["ContentStore", "ContentStat"]
