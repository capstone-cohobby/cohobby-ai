# tools/batch_tool.py
from __future__ import annotations
import asyncio
from typing import Optional, Dict, Any

# MCP 툴을 통해 배치 요약을 부르는 기존 방식을 최대한 재사용하되,
# 없으면 폴백(간단 평균)으로 처리
try:
    from ..mcp_tools import batch_summary as mcp_batch_summary   # 가정: 제공되는 경우
except Exception:
    mcp_batch_summary = None

async def run_batch_and_cache(name: str, category: str, signature: str) -> Optional[Dict[str, Any]]:
    """
    배치 요약(통계)만 담당. (검색/RAG는 Chroma)
    1) 가능하면 MCP의 batch_summary 호출
    2) 폴백: 간단 평균/중앙값 계산(데모용)
    """
    try:
        if mcp_batch_summary is not None:
            # 가정: mcp_batch_summary(bucket?, key?, category?, signature?) 등
            # 기존 프로젝트 함수 시그니처에 맞춰 조정하세요.
            res = await _maybe_await(mcp_batch_summary(category=category, signature=signature, name=name))
            if res:
                return res
    except Exception as e:
        print(f"[Batch] MCP error -> fallback: {e}")

    # ---- 폴백(데모): 최소 형태 ----
    # 실제에선 S3에서 유사 품목 N개를 읽어 간단 집계 (평균/중앙값) 내리면 됨.
    await asyncio.sleep(0.05)
    return {
        "decision": "reasonable",
        "reasoning": "Fallback batch: not enough MCP signal; using safe default window.",
        "price": {"point": 20000.0, "low": 18000.0, "high": 26000.0, "basis": "fallback"},
        "signals": {"n_total": None, "n_eligible": None},
    }

async def _maybe_await(x):
    if asyncio.iscoroutine(x):
        return await x
    return x
