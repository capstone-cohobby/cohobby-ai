# mcp_http.py — HTTP JSON-RPC client (POST /), async/await
import os, asyncio, httpx
from typing import Any, Dict

BASE = os.getenv("MCP_URL", "http://127.0.0.1:8765")  # 예: http://127.0.0.1:8765
API_KEY = os.getenv("SMITHERY_API_KEY")
PROTOCOL_VERSION = "2025-06-18"

def _url() -> str:
    if API_KEY:
        sep = "&" if "?" in BASE else "?"
        return f"{BASE}{sep}api_key={API_KEY}"
    return BASE

async def _rpc(method: str, params: Dict[str, Any] | None = None, _id: int = 1) -> Dict[str, Any]:
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(
            _url(),
            headers={
                "Content-Type": "application/json",
                "MCP-Protocol-Version": PROTOCOL_VERSION,
            },
            json={"jsonrpc": "2.0", "id": _id, "method": method, "params": params or {}},
        )
        r.raise_for_status()
        return r.json()

async def initialize() -> Dict[str, Any]:
    return await _rpc(
        "initialize",
        {"protocolVersion": PROTOCOL_VERSION, "capabilities": {}, "clientInfo": {"name": "httpx", "version": "0.0.1"}},
        _id=1,
    )

async def list_tools() -> Dict[str, Any]:
    return await _rpc("tools/list", _id=2)

async def call_tool(name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    return await _rpc("tools/call", {"name": name, "arguments": arguments}, _id=3)

# 편의 동기 래퍼
def run(coro):
    return asyncio.run(coro)

def tool_fetch_core_from_s3(bucket: str, key: str, limit: int = 50):
    run(initialize())  # 최초 1회만 호출해도 됨
    run(list_tools())
    return run(call_tool("fetch_core_from_s3", {"bucket": bucket, "key": key, "limit": limit}))

def tool_fetch_and_normalize_from_s3(bucket: str, key: str, limit: int = 50):
    run(initialize())
    run(list_tools())
    return run(call_tool("fetch_and_normalize_from_s3", {"bucket": bucket, "key": key, "limit": limit}))
