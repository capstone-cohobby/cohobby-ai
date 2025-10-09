# apps/mcp_datalookup/server.py
from __future__ import annotations
from typing import Iterable, Iterator
from fastmcp import FastMCP

from src.mcp.servers.datalookup.utils.s3_reader import read_jsonl
from src.mcp.servers.datalookup.utils.classifier import classify_listing
from src.mcp.servers.datalookup.utils.extractor import extract_product_and_category
from src.mcp.servers.datalookup.utils.normalizer import extract_rental_price
from src.mcp.servers.datalookup.utils.pricing_utils import iqr_filter, summarize

mcp = FastMCP("data-lookup")

def _normalize_record(raw: dict) -> dict:
    """
    서버 내부 표준화 레코드 (판단 없음: 증거/힌트만).
    - 최종 카테고리/합리성 평가는 Agent(LLM)가 한다.
    """
    title: str = (raw.get("title") or "").strip()
    body: str | None = (raw.get("body") or "").strip() or None

    # 1) 룰 기반 listing 타입 + 신호
    listing_type, signals = classify_listing(title, body)

    # 2) 모델명/카테고리 힌트(맵 없어도 안전)
    model_hint, category_hint = extract_product_and_category(title, body)

    # 3) 대여가(있을 때만 추출)
    rental_price = extract_rental_price(title, body) if listing_type == "rental" else None

    # 4) 표준화 레코드(증거 중심)
    return {
        "id": str(raw.get("id") or raw.get("url") or ""),
        "source": raw.get("source", "unknown"),
        "url": raw.get("url", ""),
        "title_raw": title,
        "body_raw": body,
        "scraped_at": raw.get("scraped_at"),
        "location": raw.get("location"),

        "listing_type": listing_type,      # "rental" | "sale" | "unknown"
        "model_hint": model_hint,          # 최종 결정 아님 (Agent에서 판단)
        "category_hint": category_hint,    # 최종 결정 아님 (Agent에서 판단)
        "rental_price": rental_price,      # int | None (KRW)

        "signals": signals,                # 전처리 레벨 힌트 플래그
    }

def _iter_normalized(items: Iterable[dict]) -> Iterator[dict]:
    for raw in items:
        try:
            yield _normalize_record(raw)
        except Exception as e:
            # 서버는 튼튼해야 하므로 개별 실패는 건너뜀
            yield {"error": str(e), "raw_id": raw.get("id") or raw.get("url")}

@mcp.tool()
def fetch_and_normalize(bucket: str, key: str, limit: int = 500) -> list[dict]:
    """
    S3의 jsonl(.gz) 표본을 읽어 표준화 레코드로 변환.
    - 반환: 표준화 레코드 리스트 [{...}]
    """
    # S3 jsonl(.gz) 리더  :contentReference[oaicite:9]{index=9}
    it = read_jsonl(bucket, key)
    out = []
    for i, rec in enumerate(_iter_normalized(it)):
        out.append(rec)
        if i + 1 >= max(1, limit):
            break
    return out

@mcp.tool()
def summarize_rental_prices(records: list[dict]) -> dict:
    """
    표준화 레코드에서 rental_price를 모아 IQR 필터 → 요약 통계 반환.
    - 반환: {"count":..., "median":..., "mean":..., "min":..., "max":..., "iqr_bounds": {...}}
    """
    prices = [float(r["rental_price"]) for r in records if r and r.get("rental_price") is not None]
    kept, meta = iqr_filter(prices)      # 이상치 제거  :contentReference[oaicite:10]{index=10}
    summary = summarize(kept)            # 기본 요약    :contentReference[oaicite:11]{index=11}
    summary["iqr_bounds"] = meta
    return summary

def main():
    # FastMCP 서버 실행
    mcp.run()

if __name__ == "__main__":
    main()
