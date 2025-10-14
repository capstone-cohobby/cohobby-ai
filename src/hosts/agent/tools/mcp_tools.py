# mcp_tools.py
from typing import Any, Dict, Optional, List
from pydantic import BaseModel, Field
from langchain.tools import StructuredTool
from cohobby_mcp.client.mcp_client_http import MCPHttpClient

# --- MCP client 인스턴스 (환경변수/설정에서 주입) ---
# 예) MCP_URL=https://server.smithery.ai/...  SMITHERY_API_KEY=...
import os
MCP_URL = os.getenv("MCP_URL", "http://127.0.0.1:8765")
mcp = MCPHttpClient(MCP_URL)

# (선택) 부팅 시 1회 초기화
try:
    mcp.initialize()
except Exception:
    # 초기화가 필수는 아니지만, 여기서 실패해도 실제 call은 동작함
    pass

# ---- 각 MCP 툴의 입력 스키마 ----
class FetchCoreInput(BaseModel):
    bucket: str = Field(..., description="S3 bucket name")
    key: str = Field(..., description="S3 key (file path)")
    limit: Optional[int] = Field(500, description="Max records to return")

class FetchNormalizeInput(BaseModel):
    bucket: str = Field(..., description="S3 bucket name")
    key: str = Field(..., description="S3 key (file path)")
    limit: Optional[int] = Field(500, description="Max records to return")

class SummarizePricesInput(BaseModel):
    records: List[Dict[str, Any]] = Field(..., description="Array of objects that may include rental_price")

# ---- LangChain StructuredTool 생성 ----
def _unwrap_result(result: Dict[str, Any]) -> Any:
    """MCP tools/call 표준 응답에서 content[0].value 꺼내기."""
    # {"result": {"content": [{"type": "json", "value": ...}]}}
    r = result.get("result", {})
    content = r.get("content", [])
    if content and isinstance(content, list):
        item = content[0]
        return item.get("value", item)
    return r or result

fetch_core_from_s3_tool = StructuredTool.from_function(
    name="fetch_core_from_s3",
    description="Read normalized core fields from S3 (bucket+key).",
    args_schema=FetchCoreInput,
    func=lambda bucket, key, limit=500: _unwrap_result(
        mcp.tools_call("fetch_core_from_s3", {"bucket": bucket, "key": key, "limit": limit})
    ),
)

fetch_and_normalize_from_s3_tool = StructuredTool.from_function(
    name="fetch_and_normalize_from_s3",
    description="Read and normalize rental listings from S3 (bucket+key).",
    args_schema=FetchNormalizeInput,
    func=lambda bucket, key, limit=500: _unwrap_result(
        mcp.tools_call("fetch_and_normalize_from_s3", {"bucket": bucket, "key": key, "limit": limit})
    ),
)

summarize_rental_prices_tool = StructuredTool.from_function(
    name="summarize_rental_prices",
    description="IQR-filtered summary statistics of rental_price.",
    args_schema=SummarizePricesInput,
    func=lambda records: _unwrap_result(
        mcp.tools_call("summarize_rental_prices", {"records": records})
    ),
)

TOOLS = [
    fetch_core_from_s3_tool,
    fetch_and_normalize_from_s3_tool,
    summarize_rental_prices_tool,
]