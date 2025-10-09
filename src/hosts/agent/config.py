from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class Settings:
    anthropic_api_key: str = os.getenv("ANTHROPIC_API_KEY", "")
    model: str = os.getenv("CLAUDE_MODEL", "claude-3-7-sonnet-latest")
    http_timeout_secs: float = float(os.getenv("HTTP_TIMEOUT_SECS", "12"))

    cache_base_url: str | None = os.getenv("CACHE_BASE_URL")
    mcp_datalookup_url: str | None = os.getenv("MCP_DATALOOKUP_URL")
    mcp_pricing_url: str | None = os.getenv("MCP_PRICING_URL")
    mcp_events_url: str | None = os.getenv("MCP_EVENTS_URL")

    max_output_tokens: int = int(os.getenv("MAX_OUTPUT_TOKENS", "1024"))

settings = Settings()

if not settings.anthropic_api_key:
    raise RuntimeError("ANTHROPIC_API_KEY is required. Put it in your .env")
