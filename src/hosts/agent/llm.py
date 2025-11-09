# llm.py  — MCP 의존성 제거 & 결정/요약 LLM 분리 버전

from __future__ import annotations
from langchain_anthropic import ChatAnthropic
from anthropic import Anthropic

# 프로젝트 설정
from .config import settings  # Settings: anthropic_* / *_temperature / *_tokens / *_timeout

# ------------------------------------------------------------
# 1) 판정/결정 체인용 LLM (Probe / Price / Deposit / Rules)
#    - JSON 파싱 안정성과 일관성 우선: 낮은 temperature, 응답 길이 짧게
# ------------------------------------------------------------
chat_claude_decision = ChatAnthropic(
    anthropic_api_key=settings.anthropic_api_key,
    model=getattr(settings, "anthropic_model_decision", settings.anthropic_model),
    temperature=getattr(settings, "temperature_decision", 0.1),
    max_tokens=getattr(settings, "max_output_tokens_decision", 1024),
    timeout=getattr(settings, "llm_request_timeout_secs", 60),
)

# ------------------------------------------------------------
# 2) 요약 체인용 LLM (RAG Summarizer)
#    - 맥락 보존/가독성: 약간 높은 temperature, 토큰 여유
# ------------------------------------------------------------
chat_claude_summarizer = ChatAnthropic(
    anthropic_api_key=settings.anthropic_api_key,
    model=getattr(settings, "anthropic_model_summarizer", settings.anthropic_model),
    temperature=getattr(settings, "temperature_summarizer", 0.3),
    max_tokens=getattr(settings, "max_output_tokens_summarizer", 1536),
    timeout=getattr(settings, "llm_request_timeout_secs", 60),
)

# ✅ 기존 호환: judge.py 등에서 import하는 chat_claude는 "결정용"을 기본으로 둠
chat_claude = chat_claude_decision

# (선택) 원시 SDK 클라이언트 — 도구/유틸 레벨에서 직접 호출이 필요할 때만 사용
anthropic_client = Anthropic(
    api_key=settings.anthropic_api_key,
    timeout=getattr(settings, "llm_request_timeout_secs", 60),
)

__all__ = [
    "chat_claude",              # 호환 별칭 (= 결정용)
    "chat_claude_decision",     # 결정/판정 체인
    "chat_claude_summarizer",   # RAG 요약 체인
    "anthropic_client",
]
