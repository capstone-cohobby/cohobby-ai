from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Optional, Literal, Any

class DecisionOutput(BaseModel):
    """
    에이전트의 판단 결과를 표준화한 모델.
    LLM이 직접 반환하거나, judge() 내부에서 post-process됨.
    """

    # ─────────────────────────────
    # 🧠 1️⃣ 핵심 판단 필드
    # ─────────────────────────────
    decision: Literal["reasonable", "unreasonable", "uncertain"] = Field(
        ..., description="LLM이 판단한 결과: 합리적 / 비합리적 / 불확실"
    )
    confidence: float = Field(
        default=0.0,
        description="LLM의 자기 신뢰도 (0~1). 후속 Batch evidence 반영으로 조정됨."
    )
    reasoning: str = Field(
        ...,
        description="LLM이 판단을 내린 근거 및 reasoning 텍스트"
    )

    # ─────────────────────────────
    # 🔍 2️⃣ 정보 필요도 (자기 판단)
    # ─────────────────────────────
    info_need: Literal["none", "low", "medium", "high"] = Field(
        ...,
        description="LLM 스스로 느낀 정보 부족 정도."
    )

    # ─────────────────────────────
    # 📦 3️⃣ 카테고리 / 품목 정보
    # ─────────────────────────────
    category: Optional[str] = Field(
        None,
        description="판단 대상 물품의 카테고리 (예: 캠핑용품, 전자기기 등)"
    )
    item_name: Optional[str] = Field(
        None,
        description="사용자 입력의 실제 물품명"
    )

    # ─────────────────────────────
    # 📊 4️⃣ 증거 요약 (Batch + Redis 캐시)
    # ─────────────────────────────
    evidence_summary: Optional[Any] = Field(
        None,
        description="Redis 또는 Batch에서 가져온 통계 요약 데이터."
    )
    evidence_source: Optional[Literal["redis", "batch", "mcp", "none"]] = Field(
        "none",
        description="evidence_summary의 출처."
    )

    # ─────────────────────────────
    # 📈 5️⃣ 수학적 요약 (IQR, 평균 등)
    # ─────────────────────────────
    stats_summary: Optional[dict] = Field(
        default=None,
        description="내부 통계 계산 결과 (예: IQR, mean, std 등)."
    )

    # ─────────────────────────────
    # 🕒 6️⃣ 메타 정보
    # ─────────────────────────────
    timestamp: Optional[str] = Field(
        None,
        description="판단이 생성된 시간 (ISO 8601)"
    )

    # ─────────────────────────────
    # ⚙️ 유틸
    # ─────────────────────────────
    def boost_confidence(self, factor: float = 0.1):
        """Batch evidence 등으로 confidence를 조정"""
        self.confidence = min(1.0, self.confidence + factor)

    def set_evidence(self, evidence: Any, source: str):
        """Redis/Batch 결과를 evidence_summary로 설정"""
        self.evidence_summary = evidence
        self.evidence_source = source
        if self.confidence < 0.8:
            self.boost_confidence(0.15)
class AgentInput(BaseModel):
    """
    Agent의 판단을 위해 입력되는 데이터의 스키마입니다.
    """
    name: str = Field(
        ...,
        description="상품 또는 게시글의 이름/제목입니다. 판단의 가장 핵심적인 정보입니다."
    )
    
    description: Optional[str] = Field(
        None, 
        description="상품 또는 게시글의 상세 설명입니다. 이름만으로 부족한 정보를 보충합니다."
    )
    
    category: Optional[str] = Field(
        None, 
        description="상품의 카테고리 정보입니다. (예: 캠핑용품, 전자기기)"
    )
    
    # --- 아래 필드들은 2차 보강 판단 시 시스템에 의해 주입됩니다 ---
    
    evidence_summary: Optional[str] = Field(
        None,
        description="1차 판단에서 정보가 불충분할 경우, 2차 판단을 돕기 위해 외부에서 가져온 근거 정보 요약입니다."
    )
    
    evidence_source: Optional[str] = Field(
        None,
        description="근거 정보의 출처를 나타냅니다. (예: 'redis', 'batch')"
    )
