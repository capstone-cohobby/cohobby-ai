from __future__ import annotations
from typing import Dict, Any, List
from langchain.text_splitter import RecursiveCharacterTextSplitter
import hashlib

def make_base_id(rec: Dict[str, Any]) -> str:
    for k in ("listing_id", "id", "url"):
        v = rec.get(k)
        if v:
            return str(v)
    key = f"{rec.get('title','')}|{rec.get('ts','')}|{(rec.get('snippet') or '')[:64]}"
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]

def chunk_record(rec: Dict[str, Any], chunk_size: int=600, overlap: int=80) -> List[Dict[str, Any]]:
    title = rec.get("title") or ""
    text  = rec.get("snippet") or ""
    splitter = RecursiveCharacterTextSplitter(chunk_size=chunk_size, chunk_overlap=overlap)
    chunks = splitter.split_text(f"{title}\n{text}") or [title]

    base_meta = {k: v for k, v in rec.items() if k not in ("snippet",)}
    base_id = make_base_id(rec)

    outs = []
    for i, ch in enumerate(chunks):
        m = dict(base_meta)
        m["title"] = title
        m["snippet"] = ch
        m["id"] = f"{base_id}::{i}"
        outs.append(m)
    return outs
