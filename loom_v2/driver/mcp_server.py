from __future__ import annotations

import json
from typing import Any

from .mcp import DriverMCP
from loom_v2.observer.repository import ObserverRepository


class DriverMCPServer:
    """Minimal MCP JSON-RPC transport for conversation-scoped Driver tools.

    Codex app-server uses its native ``dynamicTools`` channel, while other MCP
    clients can use this HTTP transport.  The conversation identity is taken
    from the transport header and never from model arguments.
    """

    def __init__(self, repository: ObserverRepository) -> None:
        self.repository = repository
        self.sessions: dict[str, DriverMCP] = {}

    def session(self, conversation_ref: str) -> DriverMCP:
        if not conversation_ref:
            raise ValueError("conversation_ref_required")
        if conversation_ref not in self.sessions:
            self.sessions[conversation_ref] = DriverMCP(self.repository, conversation_ref)
        return self.sessions[conversation_ref]

    async def handle(self, request: dict[str, Any], conversation_ref: str) -> dict[str, Any]:
        request_id = request.get("id")
        method = request.get("method")
        try:
            if method == "initialize":
                result = {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {"tools": {"listChanged": False}},
                    "serverInfo": {"name": "loom-driver-mcp", "version": "0.1.0"},
                }
            elif method == "notifications/initialized":
                result = {}
            elif method == "tools/list":
                result = {"tools": self._tool_list()}
            elif method == "tools/call":
                params = request.get("params") or {}
                name = str(params.get("name") or "")
                arguments = params.get("arguments") or {}
                value = await self.session(conversation_ref).call(name, arguments)
                result = {"content": [{"type": "text", "text": json.dumps(value, ensure_ascii=False)}], "isError": False}
            else:
                return {"jsonrpc": "2.0", "id": request_id, "error": {"code": -32601, "message": f"method_not_found:{method}"}}
        except Exception as exc:
            if method == "tools/call":
                result = {"content": [{"type": "text", "text": json.dumps({"code": str(exc)}, ensure_ascii=False)}], "isError": True}
            else:
                return {"jsonrpc": "2.0", "id": request_id, "error": {"code": -32602, "message": str(exc)}}
        return {"jsonrpc": "2.0", "id": request_id, "result": result}

    @staticmethod
    def _tool_list() -> list[dict[str, Any]]:
        tools: list[dict[str, Any]] = []
        for spec in DriverMCP.tool_specs():
            tools.append(
                {
                    "name": spec["name"],
                    "description": spec.get("description", ""),
                    "inputSchema": spec.get("inputSchema", {"type": "object"}),
                }
            )
        return tools
