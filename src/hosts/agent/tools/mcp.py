from __future__ import annotations

from typing import Any

import httpx
from agent.config import settings
from langchain_core.tools import tool
from pydantic import BaseModel


# ---- MCP HTTP client helpers ----
async def _safe_get(url: str | None, path: str, params: dict[str, Any]) -> dict | None:
    if not url:
        return None
    full = url.rstrip("/") + path
    async with httpx.AsyncClient(timeout=settings.http_timeout_secs) as client:
        r = await client.get(full, params=params)
        r.raise_for_status()
        return r.json()


# ---- Tool I/O Schemas ----
class LookupInput(BaseModel):
    category: str
    product: str
    region: str


# 이 세 함수는 MCP 서버(별도 FastAPI)와의 HTTP 계약을 가정합니다.
async def _mcp_datalookup(category: str, product: str, region: str) -> dict | None:
    return await _safe_get(
        settings.mcp_datalookup_url,
        "/lookup",
        {"category": category, "product": product, "region": region},
    )


async def _mcp_pricing(category: str, product: str, region: str) -> dict | None:
    return await _safe_get(
        settings.mcp_pricing_url,
        "/sale_price",
        {"category": category, "product": product, "region": region},
    )


async def _mcp_events(category: str, product: str, region: str) -> dict | None:
    return await _safe_get(
        settings.mcp_events_url,
        "/events",
        {"category": category, "product": product, "region": region},
    )


# ---- LangChain Tools ----
@tool("lookup_s3_stats", args_schema=LookupInput, return_direct=False)
async def lookup_s3_stats(category: str, product: str, region: str) -> dict:
    """S3 표본 읽기+정규화+요약을 MCP 서버에 위임하여 가져온다."""
    data = await _mcp_datalookup(category, product, region)
    return data or {}


@tool("lookup_external_price", args_schema=LookupInput, return_direct=False)
async def lookup_external_price(category: str, product: str, region: str) -> dict:
    """외부 판매가(예: naver/danawa 등)의 중앙값을 MCP 서버에서 조회한다."""
    data = await _mcp_pricing(category, product, region)
    return data or {}


@tool("lookup_events", args_schema=LookupInput, return_direct=False)
async def lookup_events(category: str, product: str, region: str) -> dict:
    """출시/가격변동 등의 이벤트 정보를 MCP 서버에서 조회한다."""
    data = await _mcp_events(category, product, region)
    return data or {}


TOOLS = [lookup_s3_stats, lookup_external_price, lookup_events]
