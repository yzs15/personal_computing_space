import pytest
from fastapi.testclient import TestClient

from loom_v2.driver.mcp import DriverMCP
from loom_v2.observer.app import create_app
from loom_v2.observer.repository import ObserverRepository


def test_content_endpoint_canonicalizes_json_and_returns_semantic_ref() -> None:
    client = TestClient(create_app())

    first = client.post(
        "/api/v1/content",
        json={
            "media_type": "application/schema+json",
            "content": {"required": ["scores"], "type": "object"},
        },
    )
    second = client.post(
        "/api/v1/content",
        json={
            "media_type": "application/schema+json",
            "content": {"type": "object", "required": ["scores"]},
        },
    )

    assert first.status_code == 200
    assert second.status_code == 200
    first_ref = first.json()
    second_ref = second.json()
    assert first_ref == second_ref
    assert first_ref["resource_id"].startswith("content://sha256/")
    assert first_ref["identity_criterion"] == "content_digest"
    assert "endpoint_url" not in first_ref.get("access_binding", {})
    assert "bucket" not in first_ref.get("access_binding", {})
    assert "key" not in first_ref.get("access_binding", {})

    digest = first_ref["version_or_digest"]
    stored = client.get(f"/api/v1/content/{digest}")
    assert stored.status_code == 200
    assert stored.json() == {"required": ["scores"], "type": "object"}


def test_content_endpoint_rejects_invalid_json_schema() -> None:
    client = TestClient(create_app())

    response = client.post(
        "/api/v1/content",
        json={
            "media_type": "application/schema+json",
            "content": {"type": "not-a-json-schema-type"},
        },
    )

    assert response.status_code == 422
    assert response.json()["detail"] == "schema_invalid"


def test_mcp_exposes_only_the_generic_content_upload_tool() -> None:
    names = {spec["name"] for spec in DriverMCP.tool_specs()}

    assert "loom_put_content" in names
    assert "loom_put_schema" not in names
    assert "loom_put_program" not in names


async def _put_schema_through_mcp() -> dict:
    mcp = DriverMCP(ObserverRepository(), "conversation-content")
    return await mcp.call(
        "loom_put_content",
        {
            "media_type": "application/schema+json",
            "content": {"type": "object"},
        },
    )


@pytest.mark.asyncio
async def test_mcp_content_upload_returns_a_resource_ref() -> None:
    result = await _put_schema_through_mcp()

    assert result["resource_ref"]["resource_id"].startswith("content://sha256/")
