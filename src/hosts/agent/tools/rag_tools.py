# tools/rag_tools.py
from __future__ import annotations
from typing import Any, Dict, List, Optional
from hosts.agent.stores.emb_store import ChromaHybridIndex
from tavily import TavilyClient
import os
import asyncio

# 컬렉션 이름 고정(필요시 ENV로 빼도 됨)
_INTERNAL_IDX = ChromaHybridIndex("cohobby_internal")  # 내부 데이터

async def retrieve_internal(query: str, top_k=10) -> List[Dict[str, Any]]:

    # 1차 후보 넓게 뽑기
    cands = _INTERNAL_IDX.search(query, top_k=top_k)

    # 카테고리/타이틀 필터 & ‘대여|렌탈’ 가중
    def score(doc):
        s = 0.0
        title = (doc.get("title") or "").lower()
        cat   = (doc.get("category") or "").lower()
        snip  = (doc.get("snippet") or doc.get("content") or "").lower()
        
        if ("대여" in title) or ("렌탈" in title) or ("대여" in snip) or ("렌탈" in snip):
            s += 0.5
        # 원래 similarity score도 더해주기
        s += float(doc.get("score", 0))
        return s

    # listing_id 기준 dedupe
    uniq, seen = [], set()
    for d in sorted(cands, key=score, reverse=True):
        lid = d.get("listing_id") or d.get("url")
        if lid in seen:
            continue
        seen.add(lid)
        uniq.append(d)
        if len(uniq) >= 5:
            break

    # source='internal' 보존
    for d in uniq:
        d["source"] = "internal"
    return uniq


tavily_client = TavilyClient(api_key=os.getenv("TAVILY_API_KEY"))

async def retrieve_external_web(query: str):
    res = await asyncio.to_thread(tavily_client.search, query, max_results=5)
    return [
        {"title": r["title"], "content": r["content"], "url": r["url"]}
        for r in res["results"]
    ]

# [신규] 판매가/중고가 검색 툴
async def retrieve_sale_price_web(query: str):
    """'판매가/중고가' 중심으로 웹 검색 (쿼리 수정 로직 제거)"""
    
    # [수정] 쿼리 생성은 graph_pipeline의 _build_sale_query가 담당
    # sale_query = query.replace("대여", "")... (이 로직 삭제)
    
    print(f"[Graph] Fallback Sale RAG: Querying '{query}'") # 받은 쿼리 그대로 사용
    
    res = await asyncio.to_thread(
        tavily_client.search, 
        query, # 받은 쿼리 그대로 사용
        max_results=5,
    )
    return [
        {"title": r["title"], "content": r["content"], "url": r["url"]}
        for r in res.get("results", [])
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

