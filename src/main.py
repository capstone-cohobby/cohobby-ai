import os
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# 네 프로젝트 스키마/체인 import
from hosts.agent.schemas import AgentInput, DecisionOutput
from hosts.agent.chains.judge import judge_once  # 이미 하이브리드 fallback 통합된 버전 기준

# ─────────────────────────────────────────────────────────────
# FastAPI App
# ─────────────────────────────────────────────────────────────
app = FastAPI(title="Cohobby AI Price Estimator", version="1.0.0")

# CORS (필요 시 도메인 제한)
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ALLOW_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────────────────────
# Health
# ─────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {"status": "ok"}

# ─────────────────────────────────────────────────────────────
# Price estimation endpoint
# ─────────────────────────────────────────────────────────────
@app.post("/api/estimate-price", response_model=DecisionOutput)
async def estimate_price(payload: AgentInput):
    """
    사용자 입력(물품명/상태/구입시기 등)을 받아
    judge pipeline(LLM self-confidence + Redis/Batch/MCP)을 수행하고
    DecisionOutput을 반환.
    """
    try:
        result = await judge_once(payload)
        # pydantic 모델 그대로 반환 (FastAPI가 json 직렬화)
        return result
    except Exception as e:
        # 로깅은 필요 시 Sentry/Loguru로 확장
        raise HTTPException(status_code=500, detail=f"judge failed: {str(e)}")


# ─────────────────────────────────────────────────────────────
# Local run
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    host = os.getenv("SERVER_HOST", "0.0.0.0")
    port = int(os.getenv("SERVER_PORT", "8080"))
    uvicorn.run("main:app", host=host, port=port, reload=os.getenv("RELOAD", "false") == "true")

