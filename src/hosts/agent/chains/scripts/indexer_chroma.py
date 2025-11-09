# scripts/indexer_chroma.py
"""
S3의 크롤링 원본(JSON/JSONL)을 읽어와 Chroma 컬렉션(내부/웹)에 증분 색인.
- ENV:
  AWS_REGION            (예: ap-northeast-2)
  AWS_S3_BUCKET         (예: cohobby-crawler)
  S3_INPUT_PREFIX       (예: listings/2025-11/)  # prefix 또는
  S3_INPUT_KEY          (예: out.jsonl)          # 단일 키 중 하나
  CHROMA_DB_DIR         (예: ./chroma_db)
  EMB_MODEL             (예: jhgan/ko-sroberta-multitask)
- 실행:
  poetry run python -m src.hosts.agent.chains.scripts.indexer_chroma
"""
from __future__ import annotations
import os
from typing import Dict, Any, Iterable, List
from dotenv import load_dotenv; 
load_dotenv()

from hosts.agent.stores.emb_store import ChromaHybridIndex
from hosts.agent.tools.loaders.s3_loader import iter_s3_records
from hosts.agent.utils.text_chunker import chunk_record

REGION = os.getenv("AWS_REGION", "ap-northeast-2")
BUCKET = os.getenv("AWS_S3_BUCKET")
PREFIX = os.getenv("S3_INPUT_PREFIX")
KEY    = os.getenv("S3_INPUT_KEY")
CHUNK  = int(os.getenv("INDEX_CHUNK_SIZE", "600"))
OVERLP = int(os.getenv("INDEX_CHUNK_OVERLAP", "80"))

import hashlib, uuid

def _stable_id_from(rec: dict) -> str:
    # URL이 있으면 URL 해시로 안정적 ID 생성
    if rec.get("url"):
        return "url-" + hashlib.sha1(rec["url"].encode("utf-8")).hexdigest()[:16]
    # title+ts 로도 만들 수 있음
    t = (rec.get("title") or "").strip()
    ts = (rec.get("ts") or rec.get("timestamp") or "")
    if t and ts:
        key = f"{t}::{ts}"
        return "tt-" + hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]
    # 최후: 랜덤
    return "auto-" + uuid.uuid4().hex[:16]

def normalize_record(r: Dict[str, Any]) -> Dict[str, Any]:
    title = r.get("product_name") or r.get("titile") or ""
    body  = r.get("body") or r.get("description") or ""
    snippet = body.strip()
    if not snippet and title:
        snippet = title.strip()
    rec = {
        "id": r.get("post_id"),
        "listing_id": r.get("post_id") or r.get("id"),
        "title": title.strip(),
        "snippet": snippet,
        "price": r.get("rental_price") or r.get("rental_price_raw"),
        "category": r.get("category") or r.get("category_hint"),
        "source": (r.get("source") or "internal").strip().lower(),
        "ts": r.get("ts") or r.get("timestamp"),
        "url": r.get("post_link"),
        "location": r.get("location"),
        "model": r.get("model") or r.get("model_name"),
    }
    # ID 보강
    if not rec["id"]:
        rec["id"] = _stable_id_from(rec)
    if not rec["listing_id"]:
        rec["listing_id"] = rec["id"]
    return rec

def main():
    if not BUCKET:
        raise SystemExit("ENV AWS_S3_BUCKET 이(가) 필요합니다.")

    n_tot = 0
    n_int = 0
    n_web = 0
    batch_int = []
    #batch_web = []
    idx_internal = ChromaHybridIndex("cohobby_internal")
    #idx_web      = ChromaHybridIndex("cohobby_web")
    skipped_no_text = 0
    skipped_dupe    = 0
    processed_ids   = set()

    for raw in iter_s3_records(bucket=BUCKET, prefix=PREFIX, key=KEY, region=REGION):
        n_tot += 1
        rec = normalize_record(raw)

        # title/snippet 모두 비면 검색 품질이 너무 떨어짐 → 스킵(카운트)
        if not (rec.get("title") or rec.get("snippet")):
            skipped_no_text += 1
            continue
  
        record_id = rec["id"]
        if record_id in processed_ids:
            skipped_dupe += 1
            continue
        processed_ids.add(record_id)

        chunks = chunk_record(rec, chunk_size=CHUNK, overlap=OVERLP)

        batch_int.extend(chunks); n_int += 1
        if len(batch_int) >= 500:
            idx_internal.upsert_docs(batch_int); batch_int = []
            print("[Index] upsert internal (500)")

    if batch_int:
        idx_internal.upsert_docs(batch_int)
        print(f"[Index] upsert internal (final {len(batch_int)})")


    print(f"[Index] done. total raw={n_tot}, internal={n_int}, web={n_web}, "
          f"skipped_no_text={skipped_no_text}, skipped_dupe={skipped_dupe})")

    # 최종 색인 개수 확인
    try:
        from hosts.agent.tools.rag_tools import index_counts
        print("[Index] Chroma counts:", index_counts())
        print(f"[Index] done. total raw={n_tot}, internal={n_int}, web={n_web}")
    except Exception:
        pass

if __name__ == "__main__":
    main()
