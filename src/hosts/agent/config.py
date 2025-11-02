from __future__ import annotations
import os
from dataclasses import dataclass
from dotenv import load_dotenv

# .env 파일 로드는 여기서 단 한 번만 수행합니다.
load_dotenv()

@dataclass(frozen=True)
class Settings:
    """애플리케이션의 모든 설정을 보관하는 중앙 저장소입니다."""
    
    # --- Anthropic Core Settings ---
    anthropic_api_key: str = os.getenv("ANTHROPIC_API_KEY")
    anthropic_model: str = os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-20240620")

    # --- Anthropic Performance Settings (기본값 추가로 안정성 향상) ---
    temperature: float = float(os.getenv("TEMPERATURE", "0.7"))
    max_output_tokens: int = int(os.getenv("MAX_OUTPUT_TOKENS", "2048"))
    llm_request_timeout: int = int(os.getenv("LLM_REQUEST_TIMEOUT", "3000"))

    # --- Internal Services Settings (MAZ -> HTTP_TIMEOUT_SECS 이름 수정) ---
    http_timeout_secs: float = float(os.getenv("HTTP_TIMEOUT_SECS", "12.0"))
    cache_base_url: str | None = os.getenv("REDIS_URL")
    mcp_datalookup_url: str | None = os.getenv("MCP_DATALOOKUP_URL")
    mcp_pricing_url: str | None = os.getenv("MCP_PRICING_URL")
    mcp_events_url: str | None = os.getenv("MCP_EVENTS_URL")

# 다른 모든 파일에서 import하여 사용할 settings 인스턴스
settings = Settings()

# 중요한 설정값 존재 여부 확인
if not settings.anthropic_api_key:
    raise RuntimeError("ANTHROPIC_API_KEY is required. Please set it in your .env file.")