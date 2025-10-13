# batch_executor.py (MCP 서버의 툴을 호출하는 역할만 수행)

import os
import json
from cohobby_mcp.client.mcp_client_http import MCPHttpClient
from cache.redis_client import set_cached

# --- MCP 클라이언트 설정 (기존과 동일) ---
MCP_URL = os.getenv("MCP_URL")
mcp = MCPHttpClient(MCP_URL, timeout=60)
BUCKET = os.getenv("AWS_S3_BUCKET")
KEY = os.getenv("S3_INPUT_KEY", "out.jsonl")

def run_batch_judgment(category: str) -> dict:
    """
    Python Orchestrator(judge.py)의 요청을 받아,
    FastMCP 서버의 전문가 툴을 호출하고 그 결과를 반환합니다.
    """
    print(f"🚀 [Batch Executor] '{category}' 카테고리 데이터 요약을 MCP 서버에 요청합니다...")

    # 🔴 S3 조회, Claude Batch API 호출 등 복잡한 로직 모두 제거

    try:
        # 🟢 FastMCP 서버에 있는 '전문가 툴'을 호출합니다.
        #    서버의 실제 툴 이름과 파라미터에 맞게 수정하세요.
        #    예시: summarize_rental_prices, process_s3_data_and_summarize 등
        tool_name = "fetch_core_from_s3" # ◀◀◀ 서버에 구현된 툴 이름
        arguments = {
            "bucket": BUCKET,
            "key": KEY,
            "limit": 500
        }
        
        print(f"📞 [Batch Executor] Calling MCP Tool: '{tool_name}' with args: {arguments}")
        
        # MCP 클라이언트를 통해 툴 호출
        resp = mcp.tools_call(tool_name, arguments)

        # 🟢 MCP 서버가 반환한 '정제된 값'을 추출합니다.
        #    (응답 구조에 따라 이 부분은 달라질 수 있습니다)
        summary_result = (resp.get("result", {}).get("content") or [{}])[0].get("value")
        
        if not summary_result:
            print(f"⚠️ [Batch Executor] MCP 서버가 '{category}'에 대한 유효한 결과를 반환하지 않았습니다.")
            return {"error": f"No valid summary for {category}"}

        print(f"✅ [Batch Executor] MCP 서버로부터 정제된 값을 성공적으로 수신했습니다.")
        
        # Redis에 캐싱 (기존 로직 유지)
        set_cached(category, summary_result)
        
        return summary_result

    except Exception as e:
        print(f"❌ [Batch Executor] MCP 툴 호출 중 에러 발생: {e}")
        # 에러 상황을 상위 호출자(judge.py)에게 전파
        raise e