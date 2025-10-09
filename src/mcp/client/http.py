# src/mcp/client/http.py
from __future__ import annotations
from typing import Any, Dict, Optional
import httpx

async def safe_get(base: Optional[str], path: str, params: Dict[str, Any]) -> Optional[dict]:
    if not base:
        return None
    url = base.rstrip("/") + path
    async with httpx.AsyncClient(timeout=12) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        return r.json()
