# ls_retrieval_logger.py
from __future__ import annotations
from typing import List, Dict, Any, Optional
from datetime import datetime
import re
from langsmith import Client
from langsmith import run_helpers as rh

RENTAL_KWS = ["대여","렌탈","보증금","반납","연체","일일","하루","2박3일","요금"]

def _tokenize_ko(s: str) -> set[str]:
    return set(t for t in re.split(r"[^0-9A-Za-z가-힣]+", (s or "").lower()) if t)

def _rental_hit(d: Dict[str, Any]) -> bool:
    title = (d.get("title") or "").lower()
    return (d.get("is_rental") is True) or any(kw in title for kw in RENTAL_KWS)

def _fresh_hit(d: Dict[str, Any], days: int = 90) -> Optional[bool]:
    ts = d.get("published_at")
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts[:19]) if "T" in ts else datetime.fromisoformat(ts)
    except Exception:
        return None
    return (datetime.utcnow() - dt).days <= days

def _rel_hit(query: str, d: Dict[str, Any]) -> bool:
    qtok = _tokenize_ko(query)
    dtok = _tokenize_ko(d.get("title") or "")
    return bool(qtok & dtok) or _rental_hit(d)

def _avg_bool(xs: List[bool]) -> float:
    if not xs:
        return 0.0
    return sum(1 for x in xs if x) / len(xs)

@rh.traceable  # 이 함수 호출이 LangSmith에 Run으로 기록됨
def log_retrieval_to_langsmith(
    query: str,
    docs: List[Dict[str, Any]],
    *,
    k: int = 5,
    freshness_days: int = 90,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    docs: [{"doc_id":..., "title":..., "published_at":"YYYY-MM-DD", "is_rental":bool, "url":...}, ...]
    반환: LangSmith Run outputs로 저장될 dict (UI에서 바로 보임)
    """
    topk = docs[:k]
    rel = _avg_bool([_rel_hit(query, d) for d in topk])
    rental = _avg_bool([_rental_hit(d) for d in topk])
    fresh_hits, fresh_denom = 0, 0
    for d in topk:
        fh = _fresh_hit(d, days=freshness_days)
        if fh is not None:
            fresh_denom += 1
            fresh_hits += int(fh)
    fresh = (fresh_hits / fresh_denom) if fresh_denom > 0 else 0.0

    out = {
        "query": query,
        "retrieved_topk": topk,             # LangSmith 표에서 리스트로 확인 가능
        "Rel@{}" .format(k): round(rel, 3),
        "Rental@{}".format(k): round(rental, 3),
        "Fresh@{}({}d)".format(k, freshness_days): round(fresh, 3),
    }
    # 추가 메타를 outputs에 합쳐 UI에서 같이 보이도록
    if metadata:
        out["meta"] = metadata
    return out

def attach_feedback_scores(run_id: str, scores: Dict[str, float]):
    """
    필요 시, Run에 별도 feedback 점수로 부착하고 싶을 때 사용.
    UI에서 Feedback 컬럼으로 필터/정렬 가능.
    """
    client = Client()
    for key, val in scores.items():
        client.create_feedback(run_id, key=key, score=float(val))
