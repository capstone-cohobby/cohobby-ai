from __future__ import annotations
from typing import Optional, Literal, Any, Dict, List, TypedDict
from pydantic import BaseModel, Field, field_validator, model_validator

InfoNeed = Literal["none", "low", "medium", "high"]
# ─────────────────────────────────────────────────────────────
# 0) 공통 입력
# ─────────────────────────────────────────────────────────────
class AgentInput(BaseModel):
    name: str
    category: Optional[str] = None
    condition: Optional[str] = None
    bought_at: Optional[str] = None
    
    # RAG 및 Batch 요약 결과 주입
    rag_analysis_report: Optional[Dict[str, Any]] = Field(None, description="RAG 분석 리포트")
    evidence: Optional[List[Dict[str, Any]]] = Field(None, description=" 요약 전 시장 데이터 원본 RAG 증거 목록")
    used_evidence: Optional[List[Dict[str, Any]]] = Field(None, description="중고가 검색 결과 (보증금 산정용)")
    dispute_evidence: Optional[List[Dict[str, Any]]] = Field(None, description="분쟁 사례 리스트 (RAG 증거)")

class PriceEstimate(BaseModel):
    point: Optional[float] = None
    low: Optional[float] = None
    high: Optional[float] = None
    basis: Optional[str] = None
    # Fallback 추론의 근거가 된 가격과 출처 (Deriver 체인에서 사용)
    reference_price: Optional[float] = Field(None, description="참조한 판매가 또는 중고가")
    reference_type: Optional[Literal["new", "used"]] = Field(None, description="참조 가격의 유형 (신품/중고)")
    reference_url: Optional[str] = Field(None, description="참조한 가격 정보의 출처 URL")
    @model_validator(mode="after")
    def _ensure_bounds(self):
        if self.low is not None and self.high is not None and self.point is not None:
            if self.low > self.high:
                self.low, self.high = self.high, self.low  # 자동 스왑
        for k in ("point","low","high"):
            v = getattr(self, k, None)
            if v is not None and v < 0:
                raise ValueError(f"price.{k} must be >= 0")
        return self

class PriceDecision(BaseModel):
    """Finalize(Price) 체인의 출력
    
    - Price 체인: 일반 가격 산정 (reference_* 필드 없음)
    - Deriver 체인: Fallback 추론 (reference_* 필드는 price 객체 안에 있음)
    """
    decision: Literal["reasonable", "unreasonable", "uncertain"]
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: Optional[str] = None
    price: PriceEstimate
    
    @model_validator(mode="after")
    def _coherence(self):
        # 외부 정보 없이 판단하는 경우 신뢰도는 낮을 수 있으므로, 0.5 이상으로 완화
        if self.decision == "reasonable" and self.confidence < 0.5:
            raise ValueError("If decision is 'reasonable', confidence should be >= 0.5")
        if self.decision == "uncertain" and self.confidence > 0.8:
            raise ValueError("If decision is 'uncertain', confidence seems too high (>0.8)")
        if self.price is None:
            raise ValueError("price field is required")
        return self

class DepositDecision(BaseModel):
    """Finalize(Deposit) 체인의 출력
    
    보증금은 파손/연체 대비용 책임 한도 금액으로 산정됩니다.
    분실/도난에 대한 전액 배상은 별도 약관으로 처리하므로 보증금에 포함하지 않습니다.
    """
    deposit_amount: int = Field(ge=0, description="파손/연체 대비 책임 한도 금액 (원 단위, 0원 가능)")
    reasoning: Optional[str] = None

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
    used_evidence: Optional[List[Dict[str, Any]]] # 중고가 RAG 결과 (보증금 산정용)
    dispute_evidence: Optional[List[Dict[str, Any]]] # 분쟁 사례 RAG 결과
    
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
