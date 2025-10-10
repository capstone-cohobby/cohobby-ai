from __future__ import annotations
import os
import requests
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field
from langchain_core.tools import StructuredTool

# ─────────────────────────────────────────────────────────────
# MCP HTTP Client (간단 JSON RPC 스타일)
# ─────────────────────────────────────────────────────────────
MCP_DATA_LOOKUP_URL = os.getenv("MCP_DATA_LOOKUP_URL", "http://localhost:8765")

def _call_mcp(tool_name: str, params: Dict[str, Any]) -> Any:
    """
    FastMCP 서버의 특정 tool을 호출한다.
    서버에서 tool을 @mcp.tool()로 등록했다면
    POST {base}/tools/<name> 로 JSON 요청을 받도록 설정했을 것.
    필요 시 엔드포인트는 서버 구현에 맞춰 변경.
    """
    url = f"{MCP_DATA_LOOKUP_URL}/tools/{tool_name}"
    r = requests.post(url, json=params, timeout=int(os.getenv("MCP_HTTP_TIMEOUT", "60")))
    r.raise_for_status()
    return r.json()

# ─────────────────────────────────────────────────────────────
# Pydantic Arg Schemas
# ─────────────────────────────────────────────────────────────
class FetchAndNormalizeArgs(BaseModel):
    path: str = Field(..., description="로컬 JSONL 경로 (예: ./data/daangn.jsonl)")
    limit: int = Field(500, ge=1, le=5000, description="최대 읽기 개수")

class FilterByCategoryHintArgs(BaseModel):
    records: List[Dict[str, Any]] = Field(..., description="fetch_and_normalize로 얻은 표준화 레코드 리스트")
    category: str = Field(..., description="필터할 카테고리 키워드 (예: '캠핑', '전자기기')")
    limit: int = Field(100, ge=1, le=5000, description="최대 반환 개수")

class SummarizeArgs(BaseModel):
    records: List[Dict[str, Any]] = Field(..., description="표준화 레코드 리스트")

# ─────────────────────────────────────────────────────────────
# 실제 호출 함수 (LangChain Tool에 바인딩될 함수)
# ─────────────────────────────────────────────────────────────
def tool_fetch_and_normalize(path: str, limit: int = 500) -> Any:
    return _call_mcp("fetch_and_normalize", {"path": path, "limit": limit})

def tool_filter_by_category_hint(records: List[Dict[str, Any]], category: str, limit: int = 100) -> Any:
    return _call_mcp("filter_by_category_hint", {"records": records, "category": category, "limit": limit})

def tool_summarize_rental_prices(records: List[Dict[str, Any]]) -> Any:
    return _call_mcp("summarize_rental_prices", {"records": records})

# ─────────────────────────────────────────────────────────────
# LangChain Tool 등록
# ─────────────────────────────────────────────────────────────
TOOLS = [
    StructuredTool.from_function(
        name="fetch_and_normalize",
        description="로컬 JSONL을 읽어 표준화 레코드를 만든다. (대여/판매 구분, category_hint, rental_price 등 힌트 포함)",
        func=tool_fetch_and_normalize,
        args_schema=FetchAndNormalizeArgs,
        return_direct=False,
    ),
    StructuredTool.from_function(
        name="filter_by_category_hint",
        description="표준화 레코드에서 category_hint/raw.category로 카테고리 필터링",
        func=tool_filter_by_category_hint,
        args_schema=FilterByCategoryHintArgs,
        return_direct=False,
    ),
    StructuredTool.from_function(
        name="summarize_rental_prices",
        description="records에서 rental_price들을 IQR 필터링 후 요약 통계를 반환",
        func=tool_summarize_rental_prices,
        args_schema=SummarizeArgs,
        return_direct=False,
    ),
]
