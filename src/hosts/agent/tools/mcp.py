import os, requests
from typing import Any, Dict, List
from pydantic import BaseModel, Field
from langchain_core.tools import StructuredTool
from typing import Optional


# ─────────────────────────────────────────────────────────────
# HTTP 모드(FastMCP) 전용 세션/호출 유틸 (SSE 헤더 제거 + 엔드포인트 분리)
# ─────────────────────────────────────────────────────────────
import requests

MCP_BASE_URL = os.getenv("MCP_DATA_LOOKUP_URL", "http://localhost:8765/").rstrip("/")  # 예: http://localhost:8765
MCP_HTTP_TIMEOUT = float(os.getenv("MCP_HTTP_TIMEOUT", "30"))

_session = requests.Session()
_mcp_session_id: Optional[str] = None

def _ensure_mcp_session():
    """tools/list로 핸드셰이크하고 mcp-session-id를 확보한다 (HTTP 모드)."""
    global _mcp_session_id
    if _mcp_session_id:
        return

    payload = {"jsonrpc": "2.0", "id": "hello", "method": "tools/list", "params": {}}
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",               
        "mcp-protocol-version": "2025-06-18",
    }
    url = f"{MCP_BASE_URL}/"                
    r = _session.post(url, json=payload, headers=headers, timeout=MCP_HTTP_TIMEOUT)
    
    print("--- [MCP 세션 요청 결과] ---")
    print(f"URL: {url}")
    print(f"Status Code: {r.status_code}")
    print("Response Headers:")
    for key, value in r.headers.items():
        print(f"  {key}: {value}")
    print("Response Body:")
    print(r.text)
    print("--- [결과 끝] ---")
    
    # 에러 디버깅 보조
    if r.status_code >= 400:
        raise RuntimeError(f"[MCP] tools/list failed {r.status_code}: {r.text[:500]}")
    sid = r.headers.get("mcp-session-id")
    if not sid:
        # 일부 구현체가 body에 줄 수 있으니 여유 파싱
        try:
            body = r.json() or {}
            sid = (
                body.get("result", {}).get("sessionId")
                or body.get("result", {}).get("session_id")
                or body.get("sessionId")
            )
        except Exception:
            sid = None
    if not sid:
        raise RuntimeError("MCP: server did not return a session id (header or body).")

    _mcp_session_id = sid
    # print(f"[MCP] session established: {sid}")

def _call_mcp(tool_name: str, params: Dict[str, Any]) -> Any:
    """HTTP 모드로 tools/call 호출."""
    _ensure_mcp_session()

    payload = {
        "jsonrpc": "2.0",
        "id": f"call_{tool_name}",
        "method": "tools/call",
        "params": {"name": tool_name, "arguments": params},
    }
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "mcp-protocol-version": "2025-06-18",
        "mcp-session-id": _mcp_session_id,
    }
    url=f"{MCP_BASE_URL}/"
    r= _session.post(url, json=payload, headers=headers, timeout=MCP_HTTP_TIMEOUT)
    print("[DBG] base =", MCP_BASE_URL)
    print("[DBG] url  =", f"{MCP_BASE_URL}/")
    print("[DBG] headers =", headers)
    
# ---- S3용 툴들 ----
class FetchCoreFromS3Args(BaseModel):
    bucket: str = Field(..., description="S3 버킷")
    key: str = Field(..., description="S3 키 (예: out.jsonl 또는 .jsonl.gz)")
    limit: int = Field(500, ge=1, le=5000)

def _extract_records(rpc_json):
    """
    JSON-RPC 응답에서 실제 레코드 리스트를 안전하게 뽑아낸다.
    서버 구현 차이를 흡수하기 위해 여러 키를 시도한다.
    """
    if isinstance(rpc_json, list):
        return rpc_json

    res = (rpc_json or {}).get("result")
    if isinstance(res, list):
        return res
    if isinstance(res, dict):
        for k in ("records", "items", "content", "data"):
            v = res.get(k)
            if isinstance(v, list):
                return v
        # 결과가 단일 객체면 리스트로 감싸서 반환
        if isinstance(res, dict) and res:
            return [res]

    return []

def tool_fetch_core_from_s3(bucket: str, key: str, limit: int = 50):
    payload = {"bucket": bucket, "key": key, "limit": limit}
    rpc = _call_mcp("fetch_core_from_s3", payload)   # ← JSON-RPC 전체
    return _extract_records(rpc)                      # ← 리스트만 리턴

class SummarizeArgs(BaseModel):
    records: List[Dict[str, Any]]

def tool_summarize_rental_prices(records: List[Dict[str, Any]]):
    return _call_mcp("summarize_rental_prices", {"records": records})

TOOLS = [
    StructuredTool.from_function(
        name="fetch_core_from_s3",
        description="S3 JSONL(.gz)에서 코어 필드만 가져오기",
        func=tool_fetch_core_from_s3,
        args_schema=FetchCoreFromS3Args,
    ),
    StructuredTool.from_function(
        name="summarize_rental_prices",
        description="records의 rental_price를 IQR로 요약 통계",
        func=tool_summarize_rental_prices,
        args_schema=SummarizeArgs,
    ),
]