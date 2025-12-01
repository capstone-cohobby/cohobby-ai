# llm.py — LLM 팩토리 버전

from __future__ import annotations
from langchain_core.language_models import BaseChatModel
from langchain_openai import ChatOpenAI
# from langchain_google_vertexai import ChatVertexAI  # (참고) 나중에 Gemini 추가 시

# 프로젝트 설정
from .config import settings

def _create_chat_model(
    provider: str,
    api_key: str,
    model_name: str,
    temperature: float,
    max_tokens: int,
    timeout: float
) -> BaseChatModel:
    """설정에 맞는 LLM 클라이언트를 생성하는 팩토리 함수"""
    
    provider = provider.lower()
    
    if provider == "openai":
        # JSON Mode 활성화 (GPT-4o가 JSON을 content에 반환하도록 강제)
        # 단, 프롬프트에서도 JSON 출력을 명시해야 함
        return ChatOpenAI(
            openai_api_key=api_key,
            model=model_name,
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=timeout,
            model_kwargs={
                "response_format": {"type": "json_object"}  # JSON Mode 강제
            }
        )
    
    # (참고) Google Gemini 추가 시
    # elif provider == "google":
    #     return ChatVertexAI(
    #         model_name=model_name,
    #         temperature=temperature,
    #         max_output_tokens=max_tokens,
    #         # ... google용 파라미터 ...
    #     )
        
    else:
        raise ValueError(f"지원하지 않는 LLM 프로바이더: {provider}. 현재는 'openai'만 지원합니다.")

# ------------------------------------------------------------
# 1) 판정/결정 체인용 LLM
# ------------------------------------------------------------
chat_decision = _create_chat_model(
    provider=settings.llm_provider,
    api_key=settings.llm_api_key,
    model_name=settings.llm_model_decision,
    temperature=settings.temperature_decision,
    max_tokens=settings.max_output_tokens_decision,
    timeout=settings.llm_request_timeout_secs,
)

# ------------------------------------------------------------
# 2) 요약 체인용 LLM
# ------------------------------------------------------------
chat_summarizer = _create_chat_model(
    provider=settings.llm_provider,
    api_key=settings.llm_api_key,
    model_name=settings.llm_model_summarizer,
    temperature=settings.temperature_summarizer,
    max_tokens=settings.max_output_tokens_summarizer,
    timeout=settings.llm_request_timeout_secs,
)

# 기존 호환: chat_claude를 import하는 다른 파일들을 위해 제네릭 별칭 제공
chat_model = chat_decision




__all__ = [
    "chat_model",           # 호환 별칭 (= 결정용)
    "chat_decision",        # 결정/판정 체인 (제네릭)
    "chat_summarizer"      # RAG 요약 체인 (제네릭)
]
