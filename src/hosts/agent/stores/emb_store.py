# stores/emb_store.py
from __future__ import annotations
import os, uuid
from typing import List, Dict, Any, Optional
import chromadb
from chromadb.utils import embedding_functions
from rapidfuzz import fuzz
import chromadb.api.client


def get_embedder():
    model = os.getenv("EMB_MODEL", "jhgan/ko-sroberta-multitask")
    return embedding_functions.SentenceTransformerEmbeddingFunction(model_name=model)

def get_client() -> chromadb.api.client.ClientAPI:
    """
    PersistentClient 사용: CHROMA_DB_DIR 경로에 디스크 영속.
    기본값 ./chroma_db
    """
    dbdir = os.getenv("CHROMA_DB_DIR", "./chroma_db")
    os.makedirs(dbdir, exist_ok=True)
    return chromadb.PersistentClient(path=dbdir)

class ChromaHybridIndex:
    """
    Chroma 기반 로컬 벡터 스토어 (+ 간단 fuzzy 보정)
    doc 메타 예: {id, title, snippet, price, source, category, listing_id, ts, ...}
    """
    def __init__(self, name: str, docs: Optional[List[Dict[str, Any]]] = None):
        self.client = get_client()
        self.ef = get_embedder()
        self.col = self.client.get_or_create_collection(name=name, embedding_function=self.ef)

        if docs:
            self.upsert_docs(docs)

    def upsert_docs(self, docs: List[Dict[str, Any]]):
        ids, texts, metas = [], [], []
        for d in docs:
            did = str(d.get("id") or d.get("listing_id") or uuid.uuid4())
            title = (d.get("title") or d.get("name") or "").strip()
            snippet = (d.get("snippet") or d.get("content") or d.get("body") or d.get("description") or "").strip()
            price = d.get("price")
            text = f"{title}\n{snippet}\nprice:{price}"
            ids.append(did)
            metas.append({k: v for k, v in d.items() if k not in ("id",)})
            texts.append(text)

        if ids:
            # id가 이미 있으면 upsert로 갱신/증분 반영
            self.col.upsert(ids=ids, documents=texts, metadatas=metas)

    def search(
        self,
        query: str,
        top_k: int = 8,
        where: Optional[Dict[str, Any]] = None,
        include_dist: bool = True,
    ) -> List[Dict[str, Any]]:
        kwargs = {"query_texts": [query], "n_results": top_k}
        if where:                 # 빈 dict, None 등은 건너뜀
            kwargs["where"] = where

        res = self.col.query(**kwargs)
        docs = res.get("documents", [[]])[0]
        metas = res.get("metadatas", [[]])[0]
        dists = res.get("distances", [[]])[0] if include_dist else [None] * len(metas)

        out = []
        for doc, meta, dist in zip(docs, metas, dists):
            title = meta.get("title") or meta.get("name")
            sim = 0.0
            if title:
                sim = fuzz.partial_ratio((query or "").lower(), (title or "").lower()) / 100.0
            base = (1.0 - float(dist)) if (dist is not None) else 0.0
            score = base + 0.05 * sim
            m = dict(meta)
            m["score"] = score
            out.append(m)
        out.sort(key=lambda x: x["score"], reverse=True)
        return out
