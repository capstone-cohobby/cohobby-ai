# config.py
from __future__ import annotations
import os
from dataclasses import dataclass
from dotenv import load_dotenv

# .env는 여기서 한 번만 로드
load_dotenv()

def _as_float(envval: str, default: float) -> float:
    try:
        v = float(envval)
        # 과거에 ms 단위를 쓴 값(예: 3000)을 넣었을 가능성 대비
        return v / 1000.0 if v > 300 else v
    except Exception:
        return default

@dataclass(frozen=True)
class Settings:
    """앱 전역 설정"""

    # --- Anthropic ---
    anthropic_api_key: str = os.getenv("ANTHROPIC_API_KEY") or ""
    # 단일 모델을 쓰고 싶으면 ANTHROPIC_MODEL만 설정해도 됨
    anthropic_model: str = os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-20240620")

    # 결정/요약 LLM을 분리해서 세밀 제어
    anthropic_model_decision: str = os.getenv("ANTHROPIC_MODEL_DECISION", os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-20240620"))
    anthropic_model_summarizer: str = os.getenv("ANTHROPIC_MODEL_SUMMARIZER", os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-20240620"))

    # 샘플링/토큰/타임아웃(초)
    temperature_decision: float = float(os.getenv("DECISION_TEMPERATURE", "0.1"))
    temperature_summarizer: float = float(os.getenv("SUMMARIZER_TEMPERATURE", "0.3"))

    max_output_tokens_decision: int = int(os.getenv("MAX_OUTPUT_TOKENS_DECISION", os.getenv("MAX_OUTPUT_TOKENS", "1024")))
    max_output_tokens_summarizer: int = int(os.getenv("MAX_OUTPUT_TOKENS_SUMMARIZER", os.getenv("MAX_OUTPUT_TOKENS", "1536")))

    # 새 이름: 초 단위. 구버전 LLM_REQUEST_TIMEOUT(ms/초 혼용)도 자동 보정
    llm_request_timeout_secs: float = _as_float(
        os.getenv("LLM_REQUEST_TIMEOUT_SECS", os.getenv("LLM_REQUEST_TIMEOUT", "60")),
        default=60.0,
    )

    # --- HTTP/캐시 등 내부 서비스 ---
    http_timeout_secs: float = float(os.getenv("HTTP_TIMEOUT_SECS", "12.0"))
    cache_base_url: str | None = os.getenv("REDIS_URL")

    # --- RAG/Web ---
    tavily_api_key: str | None = os.getenv("TAVILY_API_KEY")
    rag_internal_top_k: int = int(os.getenv("RAG_INTERNAL_TOP_K", "5"))
    rag_web_top_k: int = int(os.getenv("RAG_WEB_TOP_K", "5"))

    # MCP 관련은 제거(의존성 삭제). 필요하면 주석 해제해서 쓰세요.
    # mcp_datalookup_url: str | None = os.getenv("MCP_DATALOOKUP_URL")
    # mcp_pricing_url: str | None = os.getenv("MCP_PRICING_URL")
    # mcp_events_url: str | None = os.getenv("MCP_EVENTS_URL")

# 싱글톤
settings = Settings()

if not settings.anthropic_api_key:
    raise RuntimeError("ANTHROPIC_API_KEY is required. Please set it in your .env file.")
