import os
import uvicorn
import logging
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# 스키마
from hosts.agent.schemas import AgentInput ,EstimationResponse

from hosts.agent.chains.graph_pipeline import app_graph

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

app = FastAPI(title="Cohobby AI Price Estimator", version="1.1.0")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ALLOW_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── 앱 기동 시 환경/인덱스 점검 (선택) ─────────────────────────────
@app.on_event("startup")
async def on_startup():
    # 권장: ko 임베딩 사용 시 환경 기본값 지정
    os.environ.setdefault("EMB_MODEL", "jhgan/ko-sroberta-multitask")
    os.environ.setdefault("CHROMA_DB_DIR", "./chroma_db")
    logging.info("Startup OK: EMB_MODEL=%s CHROMA_DB_DIR=%s",
                 os.getenv("EMB_MODEL"), os.getenv("CHROMA_DB_DIR"))

@app.get("/health")
async def health():
    return {"status": "ok"}

# ── 가격/보증금/규칙 추정 API ─────────────────────────────────────
@app.post("/api/estimate-price", response_model=EstimationResponse)
async def estimate_price(payload: AgentInput):
    """
    물품명/카테고리/상태/구입시기 등을 받아
    그래프 파이프라인으로 RAG -> 분석 -> 가격/보증금/규칙을 산출
    """
    try:
        # 그래프 실행
        state = await app_graph.ainvoke({"inp": payload.model_dump()})
        return EstimationResponse(
            price=state.get("price_decision"),
            deposit=state.get("deposit_decision"),
            rules=state.get("rules_decision"),
            cache_hit=state.get("cache_hit", False),
            error=state.get("error"),
            evidence=state.get("evidence", []),
        )
    except Exception as e:
        logging.error("Pipeline failed for payload=%s", payload.model_dump(), exc_info=True)
        raise HTTPException(status_code=500, detail=f"graph pipeline failed: {e}")

# ── 디버그: 쿼리/증거 확인용(선택) ────────────────────────────────
@app.post("/api/debug/rag")
async def debug_rag(payload: AgentInput):
    try:
        # 디버그 플래그를 state에 심어서 전달(그래프에서 쓰든 말든 무해)
        state = await app_graph.ainvoke({"inp": payload.model_dump(), "__debug__": True})
        return {
            "rag_query": state.get("rag_query"),
            "rag_summary": state.get("rag_summary"),
            "evidence_count": len(state.get("evidence", []) or []),
            "evidence": state.get("evidence", []),
        }
    except Exception as e:
        logging.error("RAG debug failed", exc_info=True)
        raise HTTPException(status_code=500, detail=f"rag debug failed: {e}")

# ── 로컬 실행 ────────────────────────────────────────────────────
if __name__ == "__main__":
    host = os.getenv("SERVER_HOST", "0.0.0.0")
    port = int(os.getenv("SERVER_PORT", "8080"))
    uvicorn.run("main:app", host=host, port=port, reload=os.getenv("RELOAD", "false") == "true")
