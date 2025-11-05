# tools/rag_tools.py
from __future__ import annotations
from typing import Any, Dict, List, Optional
from hosts.agent.stores.emb_store import ChromaHybridIndex
from tavily import TavilyClient
import os
import asyncio

# 컬렉션 이름 고정(필요시 ENV로 빼도 됨)
_INTERNAL_IDX = ChromaHybridIndex("cohobby_internal")  # 내부 데이터

async def retrieve_internal(query: str) -> List[Dict[str, Any]]:
    if not query:
        return []
    # source 필터가 메타데이터에 있다면 where로도 거를 수 있음. (컬렉션 분리로 충분)
    return _INTERNAL_IDX.search(query, top_k=5)


tavily_client = TavilyClient(api_key=os.getenv("TAVILY_API_KEY"))

async def retrieve_external_web(query: str):
    res = await asyncio.to_thread(tavily_client.search, query, max_results=5)
    return [
        {"title": r["title"], "content": r["content"], "url": r["url"]}
        for r in res["results"]
    ]
    
def merge_evidence(internal: List[Dict], web: List[Dict], top_k: int = 8) -> List[Dict[str, Any]]:
    merged = (internal or []) + (web or [])
    seen, uniq = set(), []
    for d in merged:
        key = d.get("id") or d.get("listing_id") or d.get("title")
        if key and key in seen:
            continue
        if key:
            seen.add(key)
        uniq.append(d)
    uniq.sort(key=lambda x: x.get("score") or 0.0, reverse=True)
    return uniq[:top_k]

