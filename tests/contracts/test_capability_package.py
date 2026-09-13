import os

import pytest

from loom_v2.contracts.types import CapabilityPackageVersion, ResourceRef, TaskClosure
from loom_v2.content_store import ContentStore


def _store() -> ContentStore:
    return ContentStore(
        endpoint_url=os.environ["LOOM_S3_ENDPOINT_URL"],
        bucket=os.environ["LOOM_S3_BUCKET"],
        access_key=os.environ["LOOM_S3_ACCESS_KEY"],
        secret_key=os.environ["LOOM_S3_SECRET_KEY"],
    )


@pytest.mark.asyncio
async def test_content_store_is_content_addressed():
    store = _store()
    ref = await store.put(b"hello", media_type="text/plain")
    assert ref.digest
    assert await store.get(ref) == b"hello"
    assert await store.exists(ref)
    with pytest.raises(ValueError, match="content_digest_mismatch"):
        await store.get(ref, expected_digest="bad")


@pytest.mark.asyncio
async def test_package_digest_and_scope_are_explicit():
    store = _store()
    program = await store.put(b"print(1)", media_type="text/x-python")
    contract = await store.put(
        b'{"schema_version":"io.v1","input_schema_ref":null,"output_schema_ref":null,"success_semantics":null,"success_validator_ref":null}',
        media_type="application/vnd.loom.io-contract+json",
    )
    package = CapabilityPackageVersion(
        package_id="pkg",
        package_version="v1",
        package_closure_version_ref="closure-pkg-v1",
        source_run_ref="run-1",
        source_closure_version_ref="committed-1",
        body={"operation_descriptor_ref": ResourceRef(resource_id="loom://matmul"), "program_content_ref": program, "io_contract_ref": contract},
    )
    assert package.scope == "run_bound"
    assert package.publication_state == "candidate"
    assert package.package_digest


def test_closure_digest_ignores_resource_access_binding():
    left = TaskClosure.model_validate({"data": {"logical_inputs": [{"resource_id": "content://sha256/" + "a" * 64, "access_binding": {"media_type": "text/plain", "endpoint": "one"}}]}})
    right = left.model_copy(deep=True)
    right.data.logical_inputs[0].access_binding = {"media_type": "text/plain", "endpoint": "two", "bucket": "other"}
    assert left.canonical_digest() == right.canonical_digest()


def test_package_digest_ignores_ref_access_and_lifecycle_metadata():
    descriptor = ResourceRef(
        resource_id="loom://matmul",
        version_or_digest="a" * 64,
        identity_criterion="descriptor_digest",
        access_binding={"endpoint": "slave-a"},
        provenance=[{"source": "run-1"}],
    )
    body = {
        "operation_descriptor_ref": descriptor,
        "program_content_ref": ResourceRef(resource_id="content://sha256/" + "b" * 64, identity_criterion="content_digest"),
        "io_contract_ref": ResourceRef(resource_id="content://sha256/" + "c" * 64, identity_criterion="content_digest"),
    }
    left = CapabilityPackageVersion(
        package_id="pkg",
        package_version="v1",
        package_closure_version_ref="closure-1",
        source_run_ref="run-1",
        source_closure_version_ref="version-1",
        body=body,
        provenance=[{"source": "run-1"}],
    )
    right_payload = left.model_dump(mode="json")
    right_payload.update(
        {
            "package_id": "pkg-renamed",
            "package_version": "v2",
            "package_closure_version_ref": "closure-2",
            "source_run_ref": "run-2",
            "source_closure_version_ref": "version-2",
            "scope": "workspace_reusable",
            "publication_state": "published",
            "provenance": [{"source": "promotion"}],
            "package_digest": "",
        }
    )
    right = CapabilityPackageVersion.model_validate(right_payload)
    right.body.operation_descriptor_ref.access_binding = {"endpoint": "slave-b"}
    right.body.operation_descriptor_ref.provenance = [{"source": "run-2"}]
    assert right.package_digest == left.package_digest
