from __future__ import annotations
from typing import Optional, Literal, Any, Dict, List, TypedDict
from pydantic import BaseModel, Field, field_validator, model_validator

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
    rag_anlysis_report: Optional[Dict[str, Any]] = Field(None, description="RAG 분석 리포트")
    evidence: Optional[List[Dict[str, Any]]] = Field(None, description="[신규] 요약 전 원본 RAG 증거 목록")

# ─────────────────────────────────────────────────────────────
# 1) Probe(1차) 출력
# ─────────────────────────────────────────────────────────────
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
    @model_validator(mode="after")
    def _ensure_bounds(self):
        if self.low is not None and self.high is not None and self.point is not None:
            if self.low > self.high:
                raise ValueError("price.low must be <= price.high")
            if not (self.low <= self.point <= self.high):
                raise ValueError("price.point must be within [low, high]")
        for k in ("point","low","high"):
            v = getattr(self, k, None)
            if v is not None and v < 0:
                raise ValueError(f"price.{k} must be >= 0")
        return self

class PriceDecision(BaseModel):
    """Finalize(Price) 체인의 출력"""
    decision: Literal["reasonable", "unreasonable", "uncertain"]
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: Optional[str] = None
    price: PriceEstimate
    @model_validator(mode="after")
    def _coherence(self):
        if self.decision == "reasonable" and self.confidence < 0.6:
            raise ValueError("If decision is 'reasonable', confidence should be >= 0.6")
        if self.decision == "uncertain" and self.confidence > 0.8:
            raise ValueError("If decision is 'uncertain', confidence seems too high (>0.8)")
        if self.price is None:
            raise ValueError("price field is required")
        return self

class DepositDecision(BaseModel):
    """Finalize(Deposit) 체인의 출력"""
    deposit_required: bool = False
    deposit_amount: Optional[float] = None
    reasoning: Optional[str] = None
    @model_validator(mode="after")
    def _deposit_consistency(self):
        if self.deposit_required:
            if self.deposit_amount is None or self.deposit_amount <= 0:
                raise ValueError("deposit_amount must be > 0 when deposit_required is true")
        else:
            if self.deposit_amount not in (None, 0):
                raise ValueError("deposit_amount should be None or 0 when deposit is not required")
        return self

class RulesDecision(BaseModel):
    """Finalize(Rules) 체인의 출력"""
    rules: List[str] = []
    reasoning: Optional[str] = None
    @field_validator("rules")
    @classmethod
    def _rules_basic_checks(cls, v):
        if not isinstance(v, list):
            raise ValueError("rules must be a list")
        cleaned, seen = [], set()
        for s in v:
            if not isinstance(s, str):
                raise ValueError("each rule must be a string")
            s2 = s.strip()
            if not s2:
                continue
            if len(s2) > 140:
                raise ValueError("each rule must be <= 140 chars")
            if s2 in seen:
                continue
            seen.add(s2)
            cleaned.append(s2)
        if len(cleaned) == 0:
            raise ValueError("at least one non-empty rule required")
        if len(cleaned) > 5:
            raise ValueError("too many rules (max 5)")
        return cleaned

class RagAnalysisReport(BaseModel):
    summary_text:str = Field(description="RAG 요약 텍스트")
    analysis_reasoning: str = Field(description="내부/외부 데이터 비교, 충돌, 아웃라이어 판단 등 상세 분석 근거")
    basis_of_summary: Literal["internal_priority", "web_priority", "blended", "no_data"] = Field(description="어떤 데이터를 우선했는지")
    conflict_detected: bool = Field(description="내부 외부 데이터 간 충돌 여부")
    outlier_info: Optional[str] = Field(description="아웃라이어 식별 시 그 이유 및 정보")


class EstimationResponse(BaseModel):
    price: PriceDecision
    deposit: DepositDecision
    rules: RulesDecision
    cache_hit: bool = False
    error: Optional[str] = None
    evidence: List[Dict[str, Any]] = []
    
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
    rag_analysis_report: Optional[Dict[str, Any]]
    sale_evidence: Optional[List[Dict[str, Any]]]

    # 최종 병렬 출력
    price_decision: Optional[PriceDecision]
    deposit_decision: Optional[DepositDecision]
    rules_decision: Optional[RulesDecision]
    
    # 에러
    error: Optional[str]

# ─────────────────────────────────────────────────────────────
# 2) Batch Summarizer(배치 판단 요약) 출력
# ─────────────────────────────────────────────────────────────

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
