# adapters.py (새 파일로 두거나 graph_pipeline.py 상단에 둬도 됨)
from typing import Dict, Any

RENTAL_KWS = ["대여","렌탈","보증금","반납","연체","일일","하루","2박3일","요금"]

def _is_rental_like(title: str = "", content: str = "") -> bool:
    t = (title or "").lower()
    c = (content or "").lower()
    return any(kw in t for kw in RENTAL_KWS) or any(kw in c for kw in RENTAL_KWS)

def normalize_doc(d: Dict[str, Any], source_hint: str | None = None) -> Dict[str, Any]:
    """
    내부/외부 RAG 결과를 LangSmith 로깅용 공통 메타로 변환
    출력 스키마: {doc_id, title, url, published_at, is_rental, source, score}
    """
    # 내부일 수도( listing_id/score/snippet/category ), 외부일 수도( content만 )
    title = d.get("title") or ""
    url = d.get("url")
    content = d.get("snippet") or d.get("content") or ""
    score = d.get("score", 0.0)
    published_at = d.get("published_at")  # 있으면 유지, 없으면 None
    doc_id = d.get("id") or d.get("listing_id") or url or title[:50]
    source = d.get("source") or source_hint or ("internal" if d.get("listing_id") else "web")

    # 내부: is_rental 메타가 있을 수도 있고, 없으면 키워드 휴리스틱
    is_rental = d.get("is_rental")
    if is_rental is None:
        is_rental = _is_rental_like(title, content)

    return {
        "doc_id": doc_id,
        "title": title,
        "url": url,
        "published_at": published_at,
        "is_rental": bool(is_rental),
        "source": source,
        "score": float(score) if score is not None else 0.0,
    }
