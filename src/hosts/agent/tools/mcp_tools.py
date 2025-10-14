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

# --- 헬퍼 함수 (기존과 동일) ---
def _unwrap_result(result: Dict[str, Any]) -> Any:
    """MCP tools/call 표준 응답에서 content[0].value 꺼내기."""
    r = result.get("result", {})
    content = r.get("content", [])
    if content and isinstance(content, list):
        item = content[0]
        return item.get("value", item)
    return r or result

# -----------------------------------------------------------
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
# 1. 새로운 '통합 함수'를 만듭니다.
#    이 함수가 내부적으로 fetch와 summarize를 순서대로 호출합니다.
# -----------------------------------------------------------
def get_price_summary_from_s3(bucket: str, key: str, limit: int = 500) -> Dict[str, Any]:
    """
    S3에서 데이터를 가져와 정규화한 뒤, 가격 통계를 요약하여 반환하는 통합 함수.
    """
    print(f"Executing combo-tool: get_price_summary_from_s3 (bucket={bucket}, key={key})")
    
    # 1단계: S3에서 데이터 가져오기 및 정규화
    print(" -> Step 1: Fetching and normalizing records from S3...")
    normalized_records_result = mcp.tools_call("fetch_and_normalize_from_s3", {
        "bucket": bucket, "key": key, "limit": limit
    })
    records = _unwrap_result(normalized_records_result)
    
    if not records or isinstance(records, dict) and "error" in records:
        print(f" -> Step 1 Failed. Result: {records}")
        return {"error": "Failed to fetch or no records found.", "details": records}
    
    print(f" -> Step 1 Success. Fetched {len(records)} records.")
    
    # 2단계: 가져온 데이터로 가격 통계 요약
    print(" -> Step 2: Summarizing rental prices...")
    summary_result = mcp.tools_call("summarize_rental_prices", {"records": records})
    summary = _unwrap_result(summary_result)
    print(f" -> Step 2 Success. Summary generated.")
    
    return summary

# -----------------------------------------------------------
# 2. 새로운 통합 함수의 입력 스키마를 정의합니다.
#    (기존 FetchNormalizeInput 재사용 가능)
# -----------------------------------------------------------
class GetPriceSummaryInput(BaseModel):
    bucket: str = Field(..., description="S3 bucket name")
    key: str = Field(..., description="S3 key (file path)")
    limit: Optional[int] = Field(500, description="Max records to analyze")

# -----------------------------------------------------------
# 3. 이 새로운 통합 함수를 LangChain 툴로 만듭니다.
# -----------------------------------------------------------
get_price_summary_from_s3_tool = StructuredTool.from_function(
    name="get_price_summary_from_s3",
    description="S3 파일(bucket, key)을 지정하면, 그 안의 데이터를 분석하여 가격 통계 요약을 반환합니다. 가격 분석이 필요할 때 사용하는 유일한 도구입니다.",
    args_schema=GetPriceSummaryInput,
    func=get_price_summary_from_s3,
)

# -----------------------------------------------------------
# 4. AI 에이전트에게는 이 강력한 통합 툴 하나만 노출합니다.
# -----------------------------------------------------------
TOOLS = [
    get_price_summary_from_s3_tool,
]