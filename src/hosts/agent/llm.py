import os
from typing import Optional

from langchain_anthropic import ChatAnthropic
from anthropic import Anthropic
from dotenv import load_dotenv
load_dotenv()


def _get(key: str, default: Optional[str] = None) -> str:
    v = os.getenv(key, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing env: {key}")
    return v


# ─────────────────────────────────────────────────────────────
# LangChain용 Claude (에이전트의 '두뇌')
# ─────────────────────────────────────────────────────────────
# 사용처: judge.py, classifier.py 등에서 import 하여 사용
ANTHROPIC_API_KEY = _get("ANTHROPIC_API_KEY")
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-20240620")
CLAUDE_TEMPERATURE = float(os.getenv("CLAUDE_TEMPERATURE", "0.2"))
CLAUDE_MAX_OUTPUT_TOKENS = int(os.getenv("CLAUDE_MAX_OUTPUT_TOKENS", "1024"))
REQUEST_TIMEOUT = int(os.getenv("LLM_REQUEST_TIMEOUT", "60"))  # sec

# LangChain Chat Model 인스턴스 (tool 바인딩용)
chat_claude = ChatAnthropic(
    anthropic_api_key=ANTHROPIC_API_KEY,
    model=ANTHROPIC_MODEL,
    temperature=CLAUDE_TEMPERATURE,
    max_tokens=CLAUDE_MAX_OUTPUT_TOKENS,
)

# ─────────────────────────────────────────────────────────────
# Native Anthropic client (배치/세부 제어용)
# ─────────────────────────────────────────────────────────────
# 사용처: judgement_batch.py 등 배치/로우 API 접근이 필요한 곳
anthropic_client = Anthropic(api_key=ANTHROPIC_API_KEY)


__all__ = [
    "chat_claude",
    "anthropic_client",
]

