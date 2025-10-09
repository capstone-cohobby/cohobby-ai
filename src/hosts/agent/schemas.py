from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

InfoNeed = Literal["low", "medium", "high"]
Judgment = Literal["reasonable", "borderline", "unreasonable"]
PosLabel = Literal["low", "mid", "high"]
ToolName = Literal["none", "cache", "s3", "external_price", "events"]

class Position(BaseModel):
    quantile: float = Field(ge=0.0, le=1.0)
    label: PosLabel

class Decision(BaseModel):
    ratio: float
    position: Position
    judgment: Judgment

class DecisionOutput(BaseModel):
    decision: Decision
    llm_reasoning_summary: str
    confidence: float = Field(ge=0.0, le=1.0)
    info_need: InfoNeed
    tools_requested: list[ToolName]
    evidence_used: list[ToolName]
    flags: list[str] = []

class CacheContext(BaseModel):
    median_ratio: float
    n: int
    iqr: float
    updated_at: str  # ISO8601
    p10: float | None = None
    p50: float | None = None
    p90: float | None = None

class S3Stats(BaseModel):
    n: int
    median_ratio: float
    iqr: float
    p10: float
    p50: float
    p90: float
    recent_days: int

class ExternalPrice(BaseModel):
    sources: list[str]
    median_sale_price: float | None = None

class EventsInfo(BaseModel):
    has_event: bool
    notes: str | None = None

class AgentInput(BaseModel):
    category: str
    product: str
    region: str
    cache: CacheContext
    s3: S3Stats | None = None
    sale: ExternalPrice | None = None
    events: EventsInfo | None = None
