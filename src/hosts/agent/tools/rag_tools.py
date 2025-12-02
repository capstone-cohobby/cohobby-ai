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
    """Chroma 내부 DB에서 대여 관련 게시물 검색"""
    # 1차 후보 넓게 뽑기
    cands = _INTERNAL_IDX.search(query, top_k=top_k)
    
    print(f"[RAG] retrieve_internal: Found {len(cands)} candidates from Chroma for query='{query}'")
    if cands:
        print(f"[RAG] retrieve_internal: First candidate keys={list(cands[0].keys())}")
        print(f"[RAG] retrieve_internal: First candidate has snippet={bool(cands[0].get('snippet'))}, content={bool(cands[0].get('content'))}, price={bool(cands[0].get('price'))}")

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
    
    print(f"[RAG] retrieve_internal: Returning {len(uniq)} internal docs after filtering")
    if uniq:
        print(f"[RAG] retrieve_internal: Sample doc - title='{uniq[0].get('title', 'N/A')[:50]}', price={uniq[0].get('price', 'N/A')}, has_snippet={bool(uniq[0].get('snippet'))}, has_content={bool(uniq[0].get('content'))}")
    
    return uniq

tavily_client = TavilyClient(api_key=os.getenv("TAVILY_API_KEY"))

async def retrieve_external_web(queries: List[str] = None, query: str = None):
    """웹 검색 결과를 가져오되, 대여 관련 게시물만 포함 (하드 필터)
    
    3단계 구조:
    1. 커뮤니티 대여글 (당근, 중고나라, 네이버 카페)
    2. 대여샵(업체) 가격
    3. 기타 대여 관련 글
    
    [중요] rental-price 단계에서는 '대여/렌탈' 언급 없는 문서는 아예 제외합니다.
    구매/판매 글은 Fallback 단계(retrieve_sale_price_web, retrieve_used_price_web)에서만 사용됩니다.
    """
    # 커뮤니티 및 대여샵 도메인 우선 지정
    PREFERRED_DOMAINS = [
        "daangn.com",           # 당근마켓
        "joongnara.co.kr",      # 중고나라
        "cafe.naver.com",       # 네이버 카페
        "tentmarket.co.kr",     # 텐트마켓
        "campingbox.co.kr",     # 캠핑박스
        "campingrental.co.kr",  # 캠핑렌탈
        "rental",               # 렌탈 관련 도메인
    ]
    
    # 쿼리 리스트가 있으면 병렬 검색, 단일 쿼리면 단일 검색
    if queries:
        # 여러 쿼리를 병렬로 실행 (include_domains로 우선 도메인 지정)
        tasks = [
            asyncio.to_thread(
                tavily_client.search, 
                q, 
                max_results=5,
                include_domains=PREFERRED_DOMAINS
            ) 
            for q in queries
        ]
        search_results = await asyncio.gather(*tasks)
        # 모든 결과를 하나로 합침
        all_results = []
        for res in search_results:
            all_results.extend(res.get("results", []))
    elif query:
        res = await asyncio.to_thread(
            tavily_client.search, 
            query, 
            max_results=8,
            include_domains=PREFERRED_DOMAINS
        )
        all_results = res.get("results", [])
    else:
        return []
    
    # 대여 관련 키워드 (강화)
    RENTAL_KEYWORDS = ["대여", "렌탈", "대여료", "보증금", "하루", "일일", "대여합니다", "반납", "연체", "요금"]
    # 개인 대여 키워드 (C2C 맥락 - 우대 대상)
    PERSONAL_RENTAL_KEYWORDS = ["빌려드려요", "개인 대여", "1:1 거래", "개인간", "직거래", "개인이", "개인용", "개인용품"]
    # 구매/판매 관련 키워드
    PURCHASE_KEYWORDS = ["판매", "구매"]
    # 대여샵 도메인 패턴
    RENTAL_SHOP_DOMAINS = [
        "tentmarket.co.kr",
        "campingbox.co.kr",
        "rental",
        "렌탈",
        "대여샵",
        "대여업체"
    ]
    
    results = []
    seen_urls = set()  # 중복 URL 제거
    
    for r in all_results:
        url = r.get("url", "").lower()
        if url in seen_urls:
            continue
        seen_urls.add(url)
        
        title = (r.get("title") or "").lower()
        content = (r.get("content") or "").lower()
        combined_text = f"{title} {content}"
        
        # 대여 관련 키워드 확인
        has_rental = any(kw in combined_text for kw in RENTAL_KEYWORDS)
        has_purchase = any(kw in combined_text for kw in PURCHASE_KEYWORDS)
        
        # [하드 필터] rental-price 단계에서는 '대여/렌탈' 언급 없는 문서는 아예 제외
        if not has_rental:
            continue  # 대여 관련이 없으면 완전히 제외
        
        # 기본 점수
        base_score = r.get("score", 0.5) * 1.5  # has_rental == True 이므로 1.5배 적용
        
        # 개인 대여 키워드 감지 및 점수 부스트 (C2C 맥락 우대)
        has_personal_rental = any(kw in combined_text for kw in PERSONAL_RENTAL_KEYWORDS)
        if has_personal_rental:
            base_score *= 1.2  # 개인 대여 키워드가 있으면 1.2배 추가 부스트
        
        # 커뮤니티 대여글 감지 (당근, 중고나라, 네이버 카페) - C2C 맥락에서 우선
        is_community = any(domain in url for domain in ["daangn.com", "joongnara.co.kr", "cafe.naver.com"])
        if is_community:
            base_score *= 1.5  # 커뮤니티 대여글은 실거래가로 신뢰도 높음 (1.3 → 1.5로 증가)
        
        # 대여샵(업체) 가격 감지 및 점수 부스트 (커뮤니티보다 낮게)
        is_rental_shop = any(domain in url or domain in combined_text for domain in RENTAL_SHOP_DOMAINS)
        if is_rental_shop:
            base_score *= 1.8  # 대여샵 가격은 참고용 (2.0 → 1.8로 조정, 커뮤니티보다 낮게)
        elif "업체" in combined_text or "대여샵" in combined_text:
            base_score *= 1.6  # 업체 언급이 있으면 1.6배 (1.8 → 1.6으로 조정)
        
        results.append({
            "title": r["title"],
            "content": r["content"],
            "url": r["url"],
            "source": "web",
            "score": base_score,
            "is_rental": True,  # 여기까지 온 건 전부 대여 문맥 있음
            "is_rental_shop": is_rental_shop,  # 대여샵 여부
            "is_community": is_community,  # 커뮤니티 여부
            "is_personal_rental": has_personal_rental,  # 개인 대여 키워드 여부
        })
    
    # 점수 순으로 정렬
    results.sort(key=lambda x: x["score"], reverse=True)
    rental_shop_count = sum(1 for r in results if r.get("is_rental_shop"))
    community_count = sum(1 for r in results if r.get("is_community"))
    personal_rental_count = sum(1 for r in results if r.get("is_personal_rental"))
    
    # 상세 로그 출력 (디버깅용)
    print(f"[RAG] retrieve_external_web: Filtered to {len(results)} rental-related results "
          f"(rental_shops={rental_shop_count}, community={community_count}, personal_rental_keywords={personal_rental_count}) from {len(all_results)} total")
    
    # 샘플 결과 출력 (디버깅용)
    if results:
        rental_shop_samples = [r for r in results if r.get("is_rental_shop")][:2]
        community_samples = [r for r in results if r.get("is_community")][:2]
        other_samples = [r for r in results if not r.get("is_rental_shop") and not r.get("is_community")][:2]
        
        if rental_shop_samples:
            print(f"  ↳ Rental Shop samples:")
            for i, r in enumerate(rental_shop_samples, 1):
                print(f"    [{i}] {r.get('url', '')[:80]}... (score={r.get('score', 0):.2f})")
        if community_samples:
            print(f"  ↳ Community samples:")
            for i, r in enumerate(community_samples, 1):
                print(f"    [{i}] {r.get('url', '')[:80]}... (score={r.get('score', 0):.2f})")
        if other_samples:
            print(f"  ↳ Other rental samples:")
            for i, r in enumerate(other_samples, 1):
                print(f"    [{i}] {r.get('url', '')[:80]}... (score={r.get('score', 0):.2f})")
    
    return results

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
    """내부와 웹 증거를 병합하되, 내부 데이터(가격 정보 포함)에 우선순위 부여
    
    3단계 우선순위 (C2C 개인간 대여 맥락):
    1. 내부 DB (가장 우선)
    2. 커뮤니티 대여글(실거래가) - C2C 맥락에서 우선
    3. 대여샵(업체) 가격 - 참고용
    
    [중요] 웹 검색 결과 중 is_rental=True인 것만 price evidence로 사용합니다.
    구매/판매 web 결과는 아예 price evidence에 포함되지 않습니다.
    """
    # 1) 내부 데이터 정렬 (가격 정보 우선)
    def _internal_score(doc):
        base_score = float(doc.get("score") or 0.5)
        price = doc.get("price")
        if price:
            base_score += 0.5  # 내부 가격 정보가 있으면 +0.5 보정
        else:
            base_score += 0.3  # 내부 데이터는 기본적으로 +0.3 보정
        return base_score
    
    internal_sorted = sorted(internal or [], key=_internal_score, reverse=True)
    
    # 2) 웹은 is_rental == True 만 가격 evidence로 사용 (하드 필터)
    rental_web = [d for d in (web or []) if d.get("is_rental") is True]
    
    # 웹 결과를 커뮤니티와 대여샵으로 분류 (C2C 맥락에서 커뮤니티 우선)
    community_rentals = [d for d in rental_web if d.get("is_community") is True]
    rental_shops = [d for d in rental_web if d.get("is_rental_shop") is True and not d.get("is_community")]
    other_rentals = [d for d in rental_web if not d.get("is_rental_shop") and not d.get("is_community")]
    
    def _web_score(doc):
        base_score = float(doc.get("score") or 0.5)
        # 커뮤니티 대여글은 C2C 맥락에서 가장 높은 우선순위 (실거래가)
        if doc.get("is_community"):
            base_score += 0.4  # 커뮤니티 대여글 +0.4 (0.2 → 0.4로 증가)
        # 대여샵 가격은 참고용 (커뮤니티보다 낮게)
        elif doc.get("is_rental_shop"):
            base_score += 0.2  # 대여샵 가격 +0.2 (0.4 → 0.2로 조정)
        else:
            base_score += 0.1  # 기타 대여 관련 웹 결과 +0.1
        return base_score
    
    # 커뮤니티 > 대여샵 > 기타 순으로 정렬 (C2C 맥락 반영)
    community_sorted = sorted(community_rentals, key=_web_score, reverse=True)
    rental_shops_sorted = sorted(rental_shops, key=_web_score, reverse=True)
    other_sorted = sorted(other_rentals, key=_web_score, reverse=True)
    rental_web_sorted = community_sorted + rental_shops_sorted + other_sorted
    
    # 3) 중복 제거 및 병합
    merged = internal_sorted + rental_web_sorted
    seen, uniq = set(), []
    for d in merged:
        key = d.get("id") or d.get("listing_id") or d.get("title")
        if key and key in seen:
            continue
        if key:
            seen.add(key)
        uniq.append(d)
    
    # 4) 내부 데이터를 더 우선시 (내부 N개는 무조건 먼저 채우고, 남는 슬롯만 web으로)
    N_INTERNAL_PRIORITY = 5  # 내부 데이터 최대 5개 우선 보장
    picked = internal_sorted[:N_INTERNAL_PRIORITY]
    picked_keys = {d.get("id") or d.get("listing_id") or d.get("title") for d in picked if d.get("id") or d.get("listing_id") or d.get("title")}
    
    # 남는 슬롯을 web으로 채우기 (대여샵 > 커뮤니티 > 기타 순)
    remain = top_k - len(picked)
    if remain > 0:
        for d in rental_web_sorted:
            if len(picked) >= top_k:
                break
            key = d.get("id") or d.get("listing_id") or d.get("title")
            if key and key not in picked_keys:
                picked.append(d)
                if key:
                    picked_keys.add(key)
    
    # 내부 데이터가 부족하면 나머지 내부 데이터도 추가
    if len(picked) < top_k:
        for d in internal_sorted[N_INTERNAL_PRIORITY:]:
            if len(picked) >= top_k:
                break
            key = d.get("id") or d.get("listing_id") or d.get("title")
            if key and key not in picked_keys:
                picked.append(d)
                if key:
                    picked_keys.add(key)
    
    rental_shop_count = len([d for d in picked if d.get("is_rental_shop")])
    community_count = len([d for d in picked if d.get("is_community")])
    internal_count = len([d for d in picked if d.get("source") == "internal"])
    other_web_count = len([d for d in picked if d.get("source") == "web"]) - rental_shop_count - community_count
    
    print(f"[RAG] merge_evidence: Selected {len(picked)} items "
          f"({internal_count} internal, {rental_shop_count} rental_shops, {community_count} community, {other_web_count} other web)")
    
    # 샘플 결과 출력 (디버깅용)
    if picked:
        internal_samples = [d for d in picked if d.get("source") == "internal"][:2]
        rental_shop_samples = [d for d in picked if d.get("is_rental_shop")][:2]
        community_samples = [d for d in picked if d.get("is_community")][:2]
        
        if internal_samples:
            print(f"  ↳ Internal samples:")
            for i, d in enumerate(internal_samples, 1):
                title = d.get("title", "No Title")[:50]
                price = d.get("price", "N/A")
                print(f"    [{i}] {title}... (price={price}, score={d.get('score', 0):.2f})")
        if rental_shop_samples:
            print(f"  ↳ Rental Shop samples:")
            for i, d in enumerate(rental_shop_samples, 1):
                url = d.get("url", "")[:80]
                print(f"    [{i}] {url}... (score={d.get('score', 0):.2f})")
        if community_samples:
            print(f"  ↳ Community samples:")
            for i, d in enumerate(community_samples, 1):
                url = d.get("url", "")[:80]
                print(f"    [{i}] {url}... (score={d.get('score', 0):.2f})")
    
    return picked[:top_k]

async def retrieve_dispute_cases(query: str, top_k=8) -> List[Dict[str, Any]]:
    """분쟁 사례 검색 (강화: 카테고리 필터 없이 검색하여 비슷한 카테고리 분쟁도 포함)
    
    카테고리 필터를 사용하지 않고 쿼리 기반으로만 검색하여,
    정확히 일치하지 않는 카테고리라도 관련 분쟁 사례를 찾을 수 있도록 함.
    """
    # category=None으로 전달하여 카테고리 필터를 사용하지 않음
    # top_k를 8로 늘려 더 많은 결과를 가져옴
    cands = await asyncio.to_thread(_DISPUTE_IDX.search_rules, query=query, category=None, top_k=top_k)
    return cands