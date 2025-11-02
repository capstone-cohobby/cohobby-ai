from __future__ import annotations
from typing import Optional, Literal, Any, Dict, List
from pydantic import BaseModel, Field


# ─────────────────────────────────────────────────────────────
# 0) 공통 입력
# ─────────────────────────────────────────────────────────────
class AgentInput(BaseModel):
    name: str
    description: Optional[str] = None
    category: Optional[str] = None
    # 아래 두 줄을 추가하세요
    condition: Optional[str] = None
    bought_at: Optional[str] = None
    # ---
    evidence_summary: Optional[Dict[str, Any]] = None
    evidence_source: Optional[str] = None

# ─────────────────────────────────────────────────────────────
# 1) Probe(1차) 출력
# ─────────────────────────────────────────────────────────────
class ProbeOutput(BaseModel):
    category: Optional[str] = None
    info_need: Literal["none","low","medium","high"] = "none"
    reasoning: Optional[str] = None


# ─────────────────────────────────────────────────────────────
# 2) Batch Summarizer(배치 판단 요약) 출력
# ─────────────────────────────────────────────────────────────
class PriceEstimate(BaseModel):
    point: Optional[float] = Field(None, description="최종 일일 대여가 대표값(KRW)")
    low:   Optional[float] = Field(None, description="합리 구간 하한(일일, KRW)")
    high:  Optional[float] = Field(None, description="합리 구간 상한(일일, KRW)")
    basis: Optional[str]   = Field(default=None, description="산출 근거(median/IQR/유사군/휴리스틱 등)")

class BatchSummaryOutput(BaseModel):
    """배치 판단기의 간결 JSON(증거로 그대로 evidence_summary에 넣음)"""
    target_category: Optional[str] = None
    decision: Literal["reasonable","risky","uncertain"]
    reasoning: str
    price: PriceEstimate
    signals: Optional[Dict[str, Any]] = None   # n_total, n_eligible, flags 등
    created_at: Optional[str] = None


# ─────────────────────────────────────────────────────────────
# 3) 최종 판단(2차) 출력
# ─────────────────────────────────────────────────────────────
class DecisionOutput(BaseModel):
    decision: Literal["reasonable", "unreasonable", "uncertain"]
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: Optional[str] = None
    info_need: Literal["none","low"] = "none"

    category: Optional[str] = None
    evidence_summary: Optional[Dict[str, Any]] = None  # 배치 판단(JSON) 또는 프리플랜
    evidence_source: Optional[str] = None               # "batch-llm" 등
    stats_summary: Optional[Dict[str, Any]] = None      # 추가 통계(없으면 생략/None)

    price: PriceEstimate
    timestamp: Optional[str] = None
