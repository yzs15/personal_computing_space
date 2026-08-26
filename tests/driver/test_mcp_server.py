import json

from fastapi.testclient import TestClient

from loom_v2.observer.app import create_app
from loom_v2.observer.repository import ObserverRepository


def test_mcp_json_rpc_registers_tools_and_scopes_calls_to_conversation():
    client = TestClient(create_app(ObserverRepository()))
    headers = {"x-loom-conversation-ref": "conversation-mcp-http"}

    initialized = client.post(
        "/mcp",
        headers=headers,
        json={"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}},
    )
    assert initialized.status_code == 200
    assert initialized.json()["result"]["capabilities"]["tools"] == {"listChanged": False}

    listed = client.post(
        "/mcp",
        headers=headers,
        json={"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
    )
    names = {tool["name"] for tool in listed.json()["result"]["tools"]}
    assert "loom_open_run" in names

    called = client.post(
        "/mcp",
        headers=headers,
        json={
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "loom_open_run",
                "arguments": {
                    "closure_contract": {
                        "closure_id": "closure-mcp-http",
                        "goal": "echo",
                        "body": {"closure_id": "closure-mcp-http"},
                    }
                },
            },
        },
    )
    assert called.status_code == 200
    payload = json.loads(called.json()["result"]["content"][0]["text"])
    assert payload["run_id"]

    other = client.post(
        "/mcp",
        headers={"x-loom-conversation-ref": "conversation-mcp-other"},
        json={"jsonrpc": "2.0", "id": 4, "method": "tools/call", "params": {"name": "loom_get_run_status", "arguments": {}}},
    )
    assert other.status_code == 200
    assert other.json()["result"]["isError"] is True
