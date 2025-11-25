# run_experiment.py
from __future__ import annotations
from langsmith import Client, run_helpers as rh
from typing import List, Dict, Any
from tqdm import tqdm
from datetime import datetime
from .ls_retrieval_logger import log_retrieval_to_langsmith  # 우리가 만든 함수
from ..graph_pipeline import app_graph  # 너의 LangGraph 그래프 (retrieve + answer)
import asyncio

from dotenv import load_dotenv;
load_dotenv()

# === 설정 ===
DATASET_NAME = "RAG_RETRIEVAL_QC"
EXPERIMENT_NAME = f"experiment_{datetime.now().strftime('%m%d_%H%M')}"
QUERIES = [
    "아이유 응원봉 하루 대여가?",
    "전기자전거 1일 대여 규정",
    "삼성 라이온즈 유니폼 대여료 얼마야?",
    "카메라 렌탈 하루 보증금 평균은?",
]

def ensure_dataset(client: Client):
    # list_datasets() → Dataset 객체 이터레이터 (dict 아님!)
    existing = next((d for d in client.list_datasets() if d.name == DATASET_NAME), None)
    if existing:
        print(f"ℹ️ Dataset '{DATASET_NAME}' already exists (OK).")
        return

    # 없으면 생성 + 예시 업로드
    ds = client.create_dataset(
        dataset_name=DATASET_NAME,
        description="retrieval quality evaluation dataset"
    )
    client.create_examples(
        dataset_name=DATASET_NAME,
        examples=[{"inputs": {"query": q}} for q in QUERIES]
    )
    print(f"✅ Created dataset '{DATASET_NAME}' with {len(QUERIES)} queries.")
    
async def async_run_rag_pipeline(query: str) -> Dict[str, Any]:
    """LangGraph 앱 실행 및 LangSmith 기록"""
    state = {
        "inp": {
            "name": query,        # _build_rag_query에서 name/desc/category 사용
            "description": "",
            "category": ""
        },
        # 너 그래프에서 기본값으로 쓰는 필드가 있다면 여기서 같이 지정
        "k": 5,
        "freshness_days": 90,
        # 필요하면 info_need 같은 초기 옵션도 여기에
        # "info_need": "low",
    }
    # 디버깅용
    print("[DEBUG] initial state ->", state)
    result = await app_graph.ainvoke(state)  # graph 실행

    if isinstance(result, dict):
        internal_docs = result.get("internal_docs", [])
        web_docs = result.get("web_docs", [])
        retrieved = (internal_docs or []) + (web_docs or [])
    else:
        retrieved = []
        
    # retrieve 단계의 문서 품질을 평가하여 LangSmith에 기록
    retrieved = result.get("retrieved_docs", [])
    log_retrieval_to_langsmith(query=query, docs=retrieved, k=5, freshness_days=90, metadata={"node": "retrieve_both", "retriever": "hybrid-time-decay"})

    return {"answer": result.get("final_answer", ""), "retrieved_docs": retrieved}

@rh.traceable( name=EXPERIMENT_NAME)
def run_rag_pipeline(query: str) -> Dict[str, Any]:
    return asyncio.run(async_run_rag_pipeline(query))

def main():
    client = Client()
    ensure_dataset(client)

    print(f"\n🚀 Running experiment '{EXPERIMENT_NAME}' on dataset '{DATASET_NAME}'...\n")
    for q in tqdm(QUERIES):
        run_rag_pipeline(q)

    print(f"\n✅ Experiment '{EXPERIMENT_NAME}' completed.")
    print("👉 Visit https://smith.langchain.com/ to visualize your results.\n")

if __name__ == "__main__":
    main()
