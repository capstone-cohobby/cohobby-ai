# config.py
from __future__ import annotations
import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

# .env는 여기서 한 번만 로드
load_dotenv()

def _as_float(envval: str, default: float) -> float:
    try:
        v = float(envval)
        return v / 1000.0 if v > 300 else v
    except Exception:
        return default

@dataclass(frozen=True)
class Settings:
    """앱 전역 설정 (프로바이더 분리)"""

    # --- 1. 마스터 스위치 ---
    # .env에서 "openai" 또는 "anthropic"을 지정
    llm_provider: str = os.getenv("LLM_PROVIDER", "anthropic")

    # --- 2. 프로바이더별 원본 키 ---
    anthropic_api_key: str = os.getenv("ANTHROPIC_API_KEY") or ""
    openai_api_key: str = os.getenv("OPENAI_API_KEY") or ""

    # --- 3. 프로바이더별 원본 모델 이름 ---
    # (구) 호환성을 위해 ANTHROPIC_MODEL도 읽음
    _anthropic_model: str = os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-20240620")
    _openai_model: str = os.getenv("OPENAI_MODEL", "gpt-4o")

    # --- 4. 제네릭 모델/설정 (프로바이더 공통) ---
    # .env에서 LLM_MODEL_DECISION 등으로 제어하는 것을 권장
    llm_model_decision_override: str = os.getenv("LLM_MODEL_DECISION")
    llm_model_summarizer_override: str = os.getenv("LLM_MODEL_SUMMARIZER")

    temperature_decision: float = float(os.getenv("DECISION_TEMPERATURE", "0.1"))
    temperature_summarizer: float = float(os.getenv("SUMMARIZER_TEMPERATURE", "0.3"))

    max_output_tokens_decision: int = int(os.getenv("MAX_OUTPUT_TOKENS_DECISION", "2048"))  # Deriver 체인에서 긴 JSON 응답을 위해 증가
    max_output_tokens_summarizer: int = int(os.getenv("MAX_OUTPUT_TOKENS_SUMMARIZER", "1536"))

    llm_request_timeout_secs: float = _as_float(
        os.getenv("LLM_REQUEST_TIMEOUT_SECS", "60"),
        default=60.0,
    )
    
    # --- RAG/Web ---
    tavily_api_key: str | None = os.getenv("TAVILY_API_KEY")
    rag_internal_top_k: int = int(os.getenv("RAG_INTERNAL_TOP_K", "5"))
    rag_web_top_k: int = int(os.getenv("RAG_WEB_TOP_K", "5"))

    # --- 5. 동적으로 할당될 최종 제네릭 변수 ---
    # (참고) frozen=True이므로, __post_init__에서만 값을 할당합니다.
    llm_api_key: str = field(init=False, default="")
    llm_model_decision: str = field(init=False, default="")
    llm_model_summarizer: str = field(init=False, default="")

    def __post_init__(self):
        # 1. 사용할 API 키 결정
        api_key = ""
        default_model = ""
        provider = self.llm_provider.lower()

        if provider == "openai":
            api_key = self.openai_api_key
            default_model = self._openai_model
            if not api_key:
                raise RuntimeError("LLM_PROVIDER='openai'지만 OPENAI_API_KEY가 .env에 없습니다.")
        
        elif provider == "anthropic":
            api_key = self.anthropic_api_key
            default_model = self._anthropic_model
            if not api_key:
                raise RuntimeError("LLM_PROVIDER='anthropic'이지만 ANTHROPIC_API_KEY가 .env에 없습니다.")
        
        else:
            raise RuntimeError(f"알 수 없는 LLM_PROVIDER: {self.llm_provider}. 'openai' 또는 'anthropic'을 사용하세요.")

        # frozen=True인 dataclass의 값을 수정하기 위해 object.__setattr__ 사용
        object.__setattr__(self, "llm_api_key", api_key)
        
        # 2. 사용할 모델 이름 결정 (오버라이드 우선)
        object.__setattr__(
            self, 
            "llm_model_decision", 
            self.llm_model_decision_override or default_model
        )
        object.__setattr__(
            self, 
            "llm_model_summarizer", 
            self.llm_model_summarizer_override or default_model
        )

# 싱글톤
settings = Settings()