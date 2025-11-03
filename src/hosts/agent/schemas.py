from __future__ import annotations
from typing import Optional, Literal, Any, Dict, List, TypedDict
from pydantic import BaseModel, Field

InfoNeed = Literal["none", "low", "medium", "high"]
# ─────────────────────────────────────────────────────────────
# 0) 공통 입력
# ─────────────────────────────────────────────────────────────
class AgentInput(BaseModel):
    name: str
    description: Optional[str] = None
    category: Optional[str] = None
    condition: Optional[str] = None
    bought_at: Optional[str] = None
    
    # RAG 및 Batch 요약 결과 주입
    rag_summary: Optional[str] = Field(None, description="RAG 검색 결과 요약")
    batch_summary: Optional[Dict[str, Any]] = Field(None, description="S3 배치 요약 결과")

# ─────────────────────────────────────────────────────────────
# 1) Probe(1차) 출력
# ─────────────────────────────────────────────────────────────
class ProbeOutput(BaseModel):
    category: Optional[str] = None
    info_need: Literal["none","low","medium","high"] = "none"
    reasoning: Optional[str] = None
class ProbeOutput(BaseModel):
    """Probe 체인의 출력"""
    category: Optional[str] = "unknown"
    info_need: Literal["none", "low", "medium", "high"] = "low"
    reasoning: Optional[str] = None

class PriceEstimate(BaseModel):
    point: Optional[float] = None
    low: Optional[float] = None
    high: Optional[float] = None
    basis: Optional[str] = None

class PriceDecision(BaseModel):
    """Finalize(Price) 체인의 출력"""
    decision: Literal["reasonable", "unreasonable", "uncertain"]
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: Optional[str] = None
    price: PriceEstimate

class DepositDecision(BaseModel):
    """Finalize(Deposit) 체인의 출력"""
    deposit_required: bool = False
    deposit_amount: Optional[float] = None
    reasoning: Optional[str] = None

class RulesDecision(BaseModel):
    """Finalize(Rules) 체인의 출력"""
    rules: List[str] = []
    reasoning: Optional[str] = None


# --- 2. LangGraph 상태 스키마 (TypedDict) ---

class GraphState(TypedDict, total=False):
    """LangGraph의 전체 상태를 정의하는 TypedDict"""
    
    # 입력
    inp: Dict[str, Any]
    signature: str
    cache_hit: bool

    # Probe/RAG/Batch 결과
    category: str
    info_need: InfoNeed
    internal_docs: List[Dict[str, Any]]
    web_docs: List[Dict[str, Any]]
    evidence: List[Dict[str, Any]] # RAG 병합 결과
    
    # LLM 요약 결과 (Finalize 입력)
    rag_summary: Optional[str]
    batch_summary: Optional[Dict[str, Any]]

    # 최종 병렬 출력
    price_decision: Optional[PriceDecision]
    deposit_decision: Optional[DepositDecision]
    rules_decision: Optional[RulesDecision]
    
    # 에러
    error: Optional[str]

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
