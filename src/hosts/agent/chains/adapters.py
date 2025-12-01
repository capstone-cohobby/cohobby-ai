# adapters.py (새 파일로 두거나 graph_pipeline.py 상단에 둬도 됨)
from typing import Dict, Any
import re

RENTAL_KWS = ["대여","렌탈","보증금","반납","연체","일일","하루","2박3일","요금"]

def _is_rental_like(title: str = "", content: str = "") -> bool:
    t = (title or "").lower()
    c = (content or "").lower()
    return any(kw in t for kw in RENTAL_KWS) or any(kw in c for kw in RENTAL_KWS)

def normalize_doc(d: Dict[str, Any], source_hint: str | None = None) -> Dict[str, Any]:
    """
    내부/외부 RAG 결과를 LangSmith 로깅용 공통 메타로 변환
    출력 스키마: {doc_id, title, url, published_at, is_rental, source, score, price, snippet, content}
    """
    # 내부일 수도( listing_id/score/snippet/category ), 외부일 수도( content만 )
    title = d.get("title") or ""
    url = d.get("url")
    content = d.get("snippet") or d.get("content") or ""
    score = d.get("score", 0.0)
    published_at = d.get("published_at")  # 있으면 유지, 없으면 None
    doc_id = d.get("id") or d.get("listing_id") or url or title[:50]
    source = d.get("source") or source_hint or ("internal" if d.get("listing_id") else "web")
    
    # [중요] price 필드 보존 또는 추출
    price = d.get("price")
    
    # [신규] 웹 검색 결과에서 가격이 없으면 content에서 추출 시도
    if price is None and source == "web":
        # content에서 가격 정보를 찾으려고 시도 (예: "15,000원", "15000원" 등)
        price_match = re.search(r'(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*원', content)
        if price_match:
            price = price_match.group(1) + "원"
        # 여러 가격이 있으면 첫 번째 것 사용 (더 정확한 추출은 _format_evidence_list_to_string에서 수행)
    
    # [중요] snippet/content 필드 보존 (가격 추출을 위해)
    snippet = d.get("snippet") or content
    body = d.get("content") or content

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
        "price": price,  # [중요] 가격 정보 보존
        "snippet": snippet,  # [중요] snippet 보존
        "content": body,  # [중요] content 보존
    }
