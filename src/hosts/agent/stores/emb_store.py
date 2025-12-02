# stores/emb_store.py
from __future__ import annotations
import os, uuid
from typing import List, Dict, Any, Optional
import chromadb
from chromadb.utils.embedding_functions import OpenAIEmbeddingFunction
from rapidfuzz import fuzz
import chromadb.api.client
from langchain_text_splitters import RecursiveCharacterTextSplitter 

## ---- 공통 설정 ------

def get_embedder():
    return OpenAIEmbeddingFunction(
        api_key=os.getenv("OPENAI_API_KEY"),
        model_name="text-embedding-3-small"
    )

def get_client() -> chromadb.api.client.ClientAPI:
    """
    PersistentClient 사용: CHROMA_DB_DIR 경로에 디스크 영속.
    기본값 ./chroma_db
    """
    dbdir = os.getenv("CHROMA_DB_DIR", "./chroma_db")
    os.makedirs(dbdir, exist_ok=True)
    return chromadb.PersistentClient(path=dbdir)

# ------ 1. 기존 상품(가격) 저장용 클래스 -------
class ChromaHybridIndex:
    """
    Chroma 기반 로컬 벡터 스토어 (+ 간단 fuzzy 보정)
    doc 메타 예: {id, title, snippet, price, source, category, listing_id, ts, ...}
    """
    def __init__(self, name: str = "market_items", docs: Optional[List[Dict[str, Any]]] = None):
        self.client = get_client()
        self.ef = get_embedder()
        # name 으로 market_items 컬렉션 생성
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
            # [중요] 문서 텍스트를 content/snippet 필드로 추가 (메타데이터에 없을 경우 대비)
            # 메타데이터의 snippet/content가 비어있거나 없으면 doc 텍스트를 사용
            existing_snippet = m.get("snippet", "").strip() if m.get("snippet") else ""
            existing_content = m.get("content", "").strip() if m.get("content") else ""
            doc_text = (doc or "").strip()
            
            # snippet 처리: 없거나 비어있으면 doc 텍스트 사용
            if not existing_snippet:
                m["snippet"] = doc_text
            else:
                m["snippet"] = existing_snippet
            
            # content 처리: 없거나 비어있으면 snippet 또는 doc 텍스트 사용
            if not existing_content:
                m["content"] = m.get("snippet") or doc_text
            else:
                m["content"] = existing_content
            
            # snippet과 content가 모두 비어있으면 doc 텍스트를 사용
            if not m.get("snippet") and not m.get("content") and doc_text:
                m["snippet"] = doc_text
                m["content"] = doc_text
            
            out.append(m)
        out.sort(key=lambda x: x["score"], reverse=True)
        return out

# ------ 2. 분쟁 조정 사례 저장용 클래스 -------
class DisputeIndex:
    """
    분쟁 조정 사례 데이터용 (규칙 생성/리스크 판단용)
    """
    def __init__(self, name:str = "dispute_cases"):
        self.client = get_client()
        self.ef = get_embedder()
        self.col = self.client.get_or_create_collection(name=name, embedding_function=self.ef)
        
        # [설정] 텍스트 스플리터 (너무 긴 텍스트를 잘라서 저장)
        self.splitter = RecursiveCharacterTextSplitter(
            chunk_size=2000,
            chunk_overlap=200
        )
    
    def upsert_cases(self, cases: List[Dict[str, Any]]):
        """
        PDF에서 파싱된 JSON 리스트를 저장 (자동 청킹 적용)
        """
        ids, texts, metas = [], [], []
        
        for c in cases:
            # 1. 저장할 전체 텍스트 구성 (변수명: text_content)
            text_content = (
                f"사건명: {c.get('title', '')}\n"
                f"분쟁상황(개요): {c.get('overview', '')}\n"
                f"양측주장: {c.get('arguments', '')}\n"
                f"조정결과(판단): {c.get('judgment', '')}"
            )
            
            # 2. 텍스트가 길면 자르기 (text_content를 split)
            chunks = self.splitter.split_text(text_content)
            
            for i, chunk in enumerate(chunks):
                # ID 분리: dispute-uuid_0, dispute-uuid_1 ...
                chunk_id = f"{c.get('id')}_{i}"
                
                ids.append(chunk_id)
                texts.append(chunk)
                
                # 메타데이터 복사
                meta = {
                    "category": c.get("category", "기타"),
                    "title": c.get("title", ""),
                    "parent_id": str(c.get("id")), 
                    "chunk_index": i
                }
                metas.append(meta)

        # 3. 배치 저장 (100개씩 끊어서 업로드)
        if ids:
            batch_size = 100
            for i in range(0, len(ids), batch_size):
                end_idx = min(i + batch_size, len(ids))
                self.col.upsert(
                    ids=ids[i:end_idx], 
                    documents=texts[i:end_idx], 
                    metadatas=metas[i:end_idx]
                )
        
    def search_rules(
        self,
        query: str,
        category: Optional[str] = None,
        top_k: int = 5
    ) -> List[Dict[str, Any]]:
        """
        RAG 용 검색: 규칙 생성에 필요한 '딕셔너리 리스트'를 반환
        카테고리 필터를 사용하지 않고 쿼리 기반으로만 검색하여 비슷한 카테고리 분쟁도 포함
        """
        # 카테고리 필터를 사용하지 않음 (category=None이면 필터 없이 검색)
        # 이렇게 하면 정확히 일치하지 않는 카테고리라도 관련 분쟁 사례를 찾을 수 있음
        where_clause = None

        # top_k를 더 크게 설정하여 더 많은 후보를 가져온 후 점수로 정렬
        results = self.col.query(
            query_texts=[query],
            n_results=top_k,
            where=where_clause
        )

        docs = results.get("documents", [[]])[0]
        metas = results.get("metadatas", [[]])[0]
        dists = results.get("distances", [[]])[0]
        if not dists or len(dists) < len(docs):
            dists = list(dists) + [None] * (len(docs) - len(dists))
        out = []
        for doc, meta, dist in zip(docs, metas, dists):
            score = None
            if dist is not None:
                # 그냥 -dist 로 뒤집어서 "클수록 좋은 값"으로 사용
                score = -dist
            
            # 카테고리 매칭 여부에 따라 점수 조정
            # 정확히 일치하는 카테고리는 높은 점수, 비슷한 카테고리도 포함되지만 점수는 낮음
            doc_category = meta.get("category", "").lower() if meta else ""
            if category:
                category_lower = category.lower()
                if doc_category == category_lower:
                    # 정확히 일치하는 카테고리는 점수 증가
                    if score is not None:
                        score += 0.3
                elif category_lower in doc_category or doc_category in category_lower:
                    # 부분 일치하는 카테고리는 약간의 가산점
                    if score is not None:
                        score += 0.1

            item = {
                "content": doc,
                "source": "dispute",
                "score": score,   # 🔹 여기 추가
                **meta,
         }
            out.append(item)
        
        # 점수 순으로 정렬하고 top_k만 반환
        out.sort(key=lambda x: x.get("score") or 0.0, reverse=True)
        return out[:top_k]