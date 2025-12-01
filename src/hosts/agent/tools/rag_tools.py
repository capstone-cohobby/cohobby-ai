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

    # 카테고리/타이틀 필터 & '대여|렌탈' 가중, 유사도 기반 곱하기 가중치 방식 적용
    def score(doc):
        # 1. 기본 벡터 유사도 (0.0 ~ 1.0 사이, 높을수록 관련성 높음)
        base_sim = float(doc.get("score", 0.0))
        
        # [Safety] 유사도가 너무 낮으면(예: 0.3 미만) 아예 가산점을 주지 않음 (샤이니 응원봉 방지)
        if base_sim < 0.35:
            return base_sim
            
        s = base_sim
        
        title = (doc.get("title") or "").lower()
        snip  = (doc.get("snippet") or doc.get("content") or "").lower()

        # 2. 관련성이 확보된 문서에 한해 가중치 '곱하기' 적용
        
        # (1) 대여/렌탈 의도 부합 시 1.5배
        if ("대여" in title) or ("렌탈" in title) or ("대여" in snip) or ("렌탈" in snip):
            s *= 1.5 
        
        # (2) 가격 정보 존재 시 2.0배 (가장 중요)
        # 유사도가 높은(0.7) 문서는 0.7 * 1.5 * 2.0 = 2.1이 되어 웹 검색(최대 1.0)을 압도
        # 유사도가 낮은(0.2) 문서는 0.2 (Boost 없음) 그대로 유지 -> 웹 검색에 밀림
        if doc.get("price"):
            s *= 2.0
        
        return s

    # listing_id 기준 dedupe
    uniq, seen = [], set()
    for d in sorted(cands, key=score, reverse=True):
        lid = d.get("listing_id") or d.get("url")
        if lid in seen:
            continue
        seen.add(lid)
        d["score"] = score(d)
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
    """내부와 웹 증거를 병합하되, 내부 데이터(가격 정보 포함)에 우선순위 부여"""
    merged = (internal or []) + (web or [])
    seen, uniq = set(), []
    for d in merged:
        key = d.get("id") or d.get("listing_id") or d.get("title")
        if key and key in seen:
            continue
        if key:
            seen.add(key)
        uniq.append(d)
    
    # [중요] 스코어 정렬 시 내부 데이터에 가격이 있으면 보정
    def adjusted_score(doc):
        base_score = float(doc.get("score") or 0.5)
        source = doc.get("source", "")
        price = doc.get("price")
        
        # 내부 데이터이고 가격 정보가 있으면 스코어 보정 (우선순위 상향)
        if source == "internal" and price:
            base_score += 0.5  # 내부 가격 정보가 있으면 +0.5 보정 (기존 0.3 → 0.5)
        # 내부 데이터는 기본적으로 +0.3 보정 (웹보다 우선) (기존 0.1 → 0.3)
        elif source == "internal":
            base_score += 0.3
        
        return base_score
    
    uniq.sort(key=adjusted_score, reverse=True)
    return uniq[:top_k]

async def retrieve_dispute_cases(query: str, top_k=5) -> List[Dict[str, Any]]:
    cands = await asyncio.to_thread(_DISPUTE_IDX.search_rules, query=query, top_k=top_k)
    return cands