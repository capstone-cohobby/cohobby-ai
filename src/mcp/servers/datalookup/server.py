# src/mcp/servers/datalookup/server.py
from __future__ import annotations
from typing import Iterable, Iterator, List, Dict, Any, Optional
from fastmcp import FastMCP

# 유틸 모듈 (네 폴더 구조 기준)
from src.mcp.servers.datalookup.utils.s3_reader import read_jsonl  # 로컬 JSONL 리더(경로 1개 받음)
from src.mcp.servers.datalookup.utils.classifier import classify_listing  # 대여/판매 분류
from src.mcp.servers.datalookup.utils.extractor import extract_product_and_category  # 모델/카테고리 힌트
from src.mcp.servers.datalookup.utils.normalizer import extract_rental_price  # 대여가 추출
from src.mcp.servers.datalookup.utils.pricing_utils import iqr_filter, summarize  # IQR/요약

mcp = FastMCP("data-lookup")

def _normalize_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    서버 내부 표준화 레코드 (판단 없음: 증거/힌트만).
    - 최종 카테고리/합리성 평가는 Agent(LLM)가 수행.
    """
    title: str = (raw.get("title") or "").strip()
    body: Optional[str] = (raw.get("body") or "").strip() or None

    # 1) 룰 기반 listing 타입 + 신호
    listing_type, signals = classify_listing(title, body)  # 대여/판매 힌트  :contentReference[oaicite:6]{index=6}

    # 2) 모델/카테고리 힌트
    model_hint, category_hint = extract_product_and_category(title, body)  # 약한 카테고리 힌트  :contentReference[oaicite:7]{index=7}

    # 3) 대여가(있을 때만 추출)
    rental_price = extract_rental_price(title, body) if listing_type == "rental" else None  # :contentReference[oaicite:8]{index=8}

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

        "signals": signals,                # 전처리 힌트 플래그
    }

def _iter_normalized(items: Iterable[Dict[str, Any]]) -> Iterator[Dict[str, Any]]:
    for raw in items:
        try:
            yield _normalize_record(raw)
        except Exception as e:
            # 서버는 견고해야 하므로 개별 실패는 건너뛴다.
            yield {"error": str(e), "raw_id": raw.get("id") or raw.get("url")}

@mcp.tool()
def fetch_and_normalize(path: str, limit: int = 500) -> List[Dict[str, Any]]:
    """
    로컬 JSONL(.jsonl) 파일을 읽어 표준화 레코드로 변환.
    - path: 로컬 경로 (예: ./data/daangn_sample.jsonl)
    - 반환: 표준화 레코드 리스트 [{...}]
    """
    items = read_jsonl(path)  # 로컬 파일 로더 (경로 하나 받음)  :contentReference[oaicite:9]{index=9}
    out: List[Dict[str, Any]] = []
    for i, rec in enumerate(_iter_normalized(items)):
        out.append(rec)
        if i + 1 >= max(1, limit):
            break
    return out

s3 = boto3.client("s3", config=Config(signature_version="s3v4"))

@mcp.tool()
def fetch_and_normalize_from_s3(bucket: str, key: str, limit: int = 500) -> List[Dict[str, Any]]:
    """
    S3의 JSONL(.jsonl 또는 .jsonl.gz)을 스트리밍 읽기 → 표준화.
    """
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()
    # gz 여부 판별
    try:
        content = gzip.decompress(body).decode("utf-8")
    except OSError:
        content = body.decode("utf-8")

    out: List[Dict[str, Any]] = []
    for i, line in enumerate(content.splitlines()):
        line = line.strip()
        if not line:
            continue
        try:
            raw = json.loads(line)
            norm = _normalize_record(raw)
        except Exception as e:
            norm = {"error": str(e), "raw_line": line[:160]}
        out.append(norm)
        if i + 1 >= max(1, limit):
            break
    return out

@mcp.tool()
def summarize_rental_prices(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    표준화 레코드에서 rental_price를 모아 IQR 필터 → 요약 통계 반환.
    - 반환: {"count":..., "median":..., "mean":..., "min":..., "max":..., "iqr_bounds": {...}}
    """
    prices = [float(r["rental_price"]) for r in records if r and r.get("rental_price") is not None]
    kept, meta = iqr_filter(prices)    # 이상치 제거  :contentReference[oaicite:10]{index=10}
    summary = summarize(kept)          # 기본 요약    :contentReference[oaicite:11]{index=11}
    summary["iqr_bounds"] = meta
    return summary

@mcp.tool()
def filter_by_category_hint(records: List[Dict[str, Any]], category: str, limit: int = 100) -> List[Dict[str, Any]]:
    """
    표준화 레코드에서 category_hint(또는 raw.category가 있을 경우 둘 다)로 필터링.
    - category: 예) "캠핑", "전자기기" 등
    """
    out: List[Dict[str, Any]] = []
    for r in records:
        if not r or "error" in r:
            continue
        cat_hint = (r.get("category_hint") or "").lower()
        raw_cat = (r.get("category") or "").lower()  # 원본에 category 필드가 있을 수도 있음
        if category.lower() in (cat_hint, raw_cat):
            out.append(r)
            if len(out) >= max(1, limit):
                break
    return out

def main():
    mcp.run()

if __name__ == "__main__":
    main()
