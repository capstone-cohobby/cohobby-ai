# tools/rag_tools.py
from __future__ import annotations
from typing import Any, Dict, List, Optional
from hosts.agent.stores.emb_store import ChromaHybridIndex, DisputeIndex
from tavily import TavilyClient
import os
import asyncio

# 컬렉션 이름 
_INTERNAL_IDX = ChromaHybridIndex("cohobby_internal")  # 내부 데이터
_DISPUTE_IDX = DisputeIndex("cohobby_dispute")    # 분쟁 사례 데이터

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
        {"title": r["title"], "content": r["content"], "url": r["url"], "source": "web", "score":r.get("score", 0.5)}
        for r in res["results"]
    ]

# 중고가 검색 툴
async def retrieve_used_price_web(query: str):
    """'중고가' 중심으로 웹 검색 """    
    print(f"[Graph] Fallback Sale RAG: Querying '{query}'") 
    res = await asyncio.to_thread(
        tavily_client.search, 
        query,
        max_results=3,
        include_domains = [
            "bunjae.com",
            "joongnara.co.kr",
            "daangn.com"
        ]
    )
    return [
        {"title": r["title"], "content": r["content"], "url": r["url"]}
        for r in res.get("results", [])
    ]
    
# 중고가 검색 툴    
async def retrieve_sale_price_web(query: str):
    """'판매가' 중심으로 웹 검색"""
    print(f"[Graph] Fallback Sale RAG: Querying '{query}'")     
    res = await asyncio.to_thread(
        tavily_client.search, 
        query,
        max_results=3,
        include_domains = [
            "coupang.com",
            "smartstore.naver.com",
            "11st.co.kr",
            "ssg.com",
            "gmarket.co.kr"
        ]
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

async def retrieve_dispute_cases(query: str, top_k=5) -> List[Dict[str, Any]]:
    cands = await asyncio.to_thread(_DISPUTE_IDX.search_rules, query=query, top_k=top_k)
    return cands