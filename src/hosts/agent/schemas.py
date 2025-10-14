# src/hosts/agent/schemas.py
from __future__ import annotations
from typing import Optional, Literal, Any
from pydantic import BaseModel, Field

class PriceEstimate(BaseModel):
    point: Optional[float] = Field(None, description="최종 일일 대여가(대표값, KRW)")
    low:   Optional[float] = Field(None, description="합리 구간 하한(일일, KRW)")
    high:  Optional[float] = Field(None, description="합리 구간 상한(일일, KRW)")
    currency: Literal["KRW","USD"] = "KRW"
    unit: Literal["per_day","per_hour"] = "per_day"
    basis: Optional[str] = Field(default=None, description="산출 근거(예: median, IQR, 최근n=128 등)")

class DecisionOutput(BaseModel):
    # 1) 결정 본문
    decision: Literal["reasonable", "unreasonable", "uncertain"]
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: Optional[str] = None
    info_need: Literal["none", "low", "medium", "high"] = "none"

    # 2) 메타
    category: Optional[str] = None
    evidence_summary: Optional[dict[str, Any]] = None
    evidence_source: Optional[str] = None

    # 3) 통계/보조
    stats_summary: Optional[dict[str, Any]] = Field(
        default=None,
        description="내부 통계 계산 결과 (예: IQR, mean, std, q1, q3 등)"
    )

    # 4) 최종 가격 추정
    price: Optional[PriceEstimate] = Field(
        default=None,
        description="최종 일일 대여가 추정(대표값/구간/통화/단위/근거)"
    )

    # 5) 기타
    timestamp: Optional[str] = None

class AgentInput(BaseModel):
    name: str
    description: Optional[str] = None
    category: Optional[str] = None
    evidence_summary: Optional[dict[str, Any]] = None
    evidence_source: Optional[str] = None
