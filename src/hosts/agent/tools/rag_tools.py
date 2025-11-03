# tools/rag_tools.py
from typing import Any, Dict, List

# (가정) ES, Tavily 클라이언트 설정
# from elasticsearch import Elasticsearch
# from tavily import TavilyClient
# ES = Elasticsearch(...)
# TV = TavilyClient(...)
print("[RAG] Mock ES/Tavily Clients Initialized.")

def _normalize_es_hit(hit: Dict[str, Any]) -> Dict[str, Any]:
    src = hit.get("_source", {})
    return { "source": "internal", "id": hit.get("_id"), "score": hit.get("_score"),
             "title": src.get("title"), "snippet": src.get("content") or "", "price": src.get("price") }

def _normalize_tv_item(item: Dict[str, Any]) -> Dict[str, Any]:
    return { "source": "web", "url": item.get("url"), "title": item.get("title"),
             "snippet": item.get("content") or "", "score": item.get("score") }

async def retrieve_internal(query: str) -> List[Dict[str, Any]]:
    """ES에서 내부 Top-K 검색"""
    if not query: return []
    try:
        # resp = ES.search(...)
        # (Mock) 가짜 응답
        resp = { "hits": { "hits": [
            {"_id": "es1", "_score": 0.9, "_source": {"title": "KCS 400W 2024", "price": 15000, "content": "Similar model"}}
        ]}}
        return [_normalize_es_hit(h) for h in resp["hits"]["hits"]]
    except Exception:
        return []

async def retrieve_web(query: str) -> List[Dict[str, Any]]:
    """Tavily 웹 검색"""
    if not query: return []
    try:
        # resp = TV.search(query=query, max_results=5, time_range="year")
        # (Mock) 가짜 응답
        resp = { "results": [
            {"url": "http://web.com/1", "title": "Review: KCS 450W", "content": "New model, rents for 25k/day", "score": 0.8}
        ]}
        return [_normalize_tv_item(r) for r in resp.get("results", [])]
    except Exception:
        return []

def merge_evidence(internal: List[Dict], web: List[Dict], top_k: int = 8) -> List[Dict[str, Any]]:
    """내부/웹 증거 병합 + 중복 제거 + 간단 가중치."""
    merged = (internal or []) + (web or [])
    seen, uniq = set(), []
    for d in merged:
        key = d.get("url") or d.get("title") or d.get("id")
        if key and key in seen: continue
        if key: seen.add(key)
        uniq.append(d)

    for d in uniq:
        base = d.get("score") or 0.0
        d["score"] = base + (1.0 if d.get("source") == "internal" else 0.5)

    uniq.sort(key=lambda x: x.get("score") or 0.0, reverse=True)
    return uniq[:top_k]