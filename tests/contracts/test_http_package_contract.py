from copy import deepcopy

import pytest

from loom_v2.contracts.types import CapabilityPackageVersion, ResourceRef


def _resource_ref(resource_id: str, *, descriptor_digest: str | None = None) -> dict[str, object]:
    return ResourceRef(
        resource_id=resource_id,
        version_or_digest=descriptor_digest,
        identity_criterion="descriptor_digest" if descriptor_digest else "content_digest",
    ).model_dump(mode="json")


def _export(
    descriptor_id: str,
    descriptor_digest: str,
    io_digest: str,
    path: str,
    *,
    permissions: list[str] | None = None,
) -> dict[str, object]:
    return {
        "capability_descriptor_ref": _resource_ref(
            descriptor_id, descriptor_digest=descriptor_digest
        ),
        "io_contract_ref": _resource_ref("content://sha256/" + io_digest),
        "effect_class": "NetworkService",
        "permissions": permissions or [],
        "replay_safety": "DeclaredByPackage",
        "runtime_binding": {"path": path},
    }


def http_package_payload() -> dict[str, object]:
    return {
        "package_type": "service",
        "package_id": "pkg-http",
        "package_version": "v1",
        "package_closure_version_ref": "closure-1",
        "source_run_ref": "run-1",
        "source_closure_version_ref": "version-1",
        "execution": {"kind": "container:http", "version": "1"},
        "capability_exports": [
            _export("loom://http/a", "a" * 64, "c" * 64, "/v1/a")
        ],
        "body": {
            "image_ref": "registry.example/loom/service@sha256:" + "d" * 64,
            "container_port": 8080,
            "health_path": "/healthz",
        },
    }


@pytest.mark.parametrize(
    ("mutate",),
    [
        (lambda value: value["body"].update({"unknown": True}),),
        (lambda value: value["body"].update({"container_port": 0}),),
        (lambda value: value["body"].update({"health_path": "//host/path"}),),
        (
            lambda value: value["body"].update(
                {
                    "image_ref": "registry.example/loom/service:latest@sha256:"
                    + "d" * 64
                }
            ),
        ),
        (
            lambda value: value["capability_exports"][0]["runtime_binding"].update(
                {"unknown": True}
            ),
        ),
        (
            lambda value: value["capability_exports"][0].update(
                {"runtime_binding": {"path": "relative"}}
            ),
        ),
    ],
)
def test_http_contract_rejects_invalid_or_unknown_fields(mutate) -> None:
    payload = http_package_payload()
    mutate(payload)

    with pytest.raises(ValueError, match="package_contract_invalid"):
        CapabilityPackageVersion.model_validate(payload)


def test_http_contract_round_trips_full_manifest() -> None:
    package = CapabilityPackageVersion.model_validate(http_package_payload())

    restored = CapabilityPackageVersion.model_validate(package.model_dump(mode="json"))

    assert restored == package


def test_duplicate_descriptor_export_is_rejected() -> None:
    payload = http_package_payload()
    payload["capability_exports"].append(
        _export("loom://http/a", "a" * 64, "e" * 64, "/v1/b")
    )

    with pytest.raises(ValueError, match="capability_export_duplicate"):
        CapabilityPackageVersion.model_validate(payload)


def test_export_and_permission_reordering_does_not_change_digest() -> None:
    left_payload = http_package_payload()
    left_payload["capability_exports"] = [
        _export(
            "loom://http/b",
            "b" * 64,
            "e" * 64,
            "/v1/b",
            permissions=["network", "read"],
        ),
        _export(
            "loom://http/a",
            "a" * 64,
            "c" * 64,
            "/v1/a",
            permissions=["read", "network"],
        ),
    ]
    right_payload = deepcopy(left_payload)
    right_payload["capability_exports"].reverse()
    for capability_export in right_payload["capability_exports"]:
        capability_export["permissions"].reverse()
        capability_export["capability_descriptor_ref"]["access_binding"] = {
            "endpoint": "elsewhere"
        }
        capability_export["capability_descriptor_ref"]["provenance"] = [
            {"source": "promotion"}
        ]

    left = CapabilityPackageVersion.model_validate(left_payload)
    right = CapabilityPackageVersion.model_validate(right_payload)

    assert right.package_digest == left.package_digest


def test_content_backed_descriptor_digest_participates_in_package_identity() -> None:
    left_payload = http_package_payload()
    descriptor_ref = left_payload["capability_exports"][0][
        "capability_descriptor_ref"
    ]
    descriptor_ref["resource_id"] = "content://sha256/" + "f" * 64
    right_payload = deepcopy(left_payload)
    right_payload["capability_exports"][0]["capability_descriptor_ref"][
        "version_or_digest"
    ] = "e" * 64

    left = CapabilityPackageVersion.model_validate(left_payload)
    right = CapabilityPackageVersion.model_validate(right_payload)

    assert right.package_digest != left.package_digest
