# graph_pipeline.py
import json
import asyncio
from typing import Dict, Any

from langgraph.graph import StateGraph, END

# --- 의존성 임포트 ---
# 1. 상태 정의
from ..schemas import GraphState, AgentInput, PriceDecision

# 2. 조립된 LCEL 체인
from .judge import (
    chain_probe, chain_rag_summarizer, chain_parallel_finalize
)

# 3. 개별 툴
from ..tools.cache_tools import make_signature, get_verdict, set_verdict
from ..tools.rag_tools import retrieve_internal, retrieve_web, merge_evidence


# --- LangGraph 노드(Node) 정의 ---

def enrich_input(state: GraphState) -> GraphState:
    """입력 전처리 및 서명 생성"""
    inp = dict(state["inp"])
    desc = (inp.get("description") or "").strip()
    tail = []
    if inp.get("condition"): tail.append(f"상태: {inp['condition']}")
    if inp.get("bought_at"): tail.append(f"구입시기: {inp['bought_at']}")
    if tail:
        inp["description"] = (desc + "\n" if desc else "") + "\n".join(tail)
    
    state["inp"] = inp
    state["signature"] = make_signature(inp)
    return state

def check_verdict_cache(state: GraphState) -> GraphState:
    """최종 판단 캐시 체크"""
    verdict = get_verdict(state["signature"])
    if verdict:
        state["price_decision"] = verdict
        state["cache_hit"] = True
        state["info_need"] = "none"
        print("[Graph] Cache HIT")
    else:
        state["cache_hit"] = False
        print("[Graph] Cache MISS")
    return state

async def run_probe(state: GraphState) -> GraphState:
    """1차 Probe 실행 (Pydantic In -> Pydantic Out)"""
    try:
        ai = AgentInput(**state["inp"])
        probe_out = await chain_probe.ainvoke(ai)
        
        state["probe"] = probe_out
        state["category"] = probe_out.category or state["inp"].get("category") or "unknown"
        state["info_need"] = probe_out.info_need
        print(f"[Graph] Probe OK. Info Need: {state['info_need']}")
    except Exception as e:
        print(f"[Graph] Probe ERROR: {e}")
        state["info_need"] = "medium" # 에러 시 안전하게 medium으로
    return state

async def retrieve_internal_node(state: GraphState) -> GraphState:
    """내부 RAG 실행"""
    q = state["inp"].get("name", "")
    state["internal_docs"] = await retrieve_internal(q)
    print(f"[Graph] Internal RAG: Found {len(state['internal_docs'])} docs")
    return state

async def retrieve_web_node(state: GraphState) -> GraphState:
    """외부 RAG 실행"""
    q = state["inp"].get("name", "")
    state["web_docs"] = await retrieve_web(q)
    print(f"[Graph] Web RAG: Found {len(state['web_docs'])} docs")
    return state

async def retrieve_both_node(state: GraphState) -> GraphState:
    """RAG 병렬 실행"""
    print("[Graph] RAG Both")
    await asyncio.gather(retrieve_internal_node(state), retrieve_web_node(state))
    return state

def merge_evidence_node(state: GraphState) -> GraphState:
    """RAG 증거 병합"""
    state["evidence"] = merge_evidence(
        state.get("internal_docs"), 
        state.get("web_docs")
    )
    print(f"[Graph] RAG Merged: Total {len(state['evidence'])} evidences")
    return state

async def summarize_rag_evidence(state: GraphState) -> GraphState:
    """(신규) RAG 결과 요약 LLM 호출"""
    evidence = state.get("evidence")
    if not evidence:
        print("[Graph] RAG Summarizer: No evidence to summarize.")
        state["rag_summary"] = None
        return state
    
    try:
        # chain_rag_summarizer는 List[dict]를 입력받음
        summary = await chain_rag_summarizer.ainvoke(evidence)
        state["rag_summary"] = summary
        print(f"[Graph] RAG Summarizer OK: {summary[:50]}...")
    except Exception as e:
        print(f"[Graph] RAG Summarizer ERROR: {e}")
        state["error"] = f"rag_summary_error: {e}"
    return state

async def run_batch_node(state: GraphState) -> GraphState:
    """S3 배치 툴 실행"""
    try:
        summary = await run_batch_and_cache(
            name=state["inp"].get("name", ""),
            category=state["category"],
            signature=state["signature"]
        )
        state["batch_summary"] = summary
        print(f"[Graph] Batch OK: {summary['decision'] if summary else 'No result'}")
    except Exception as e:
        print(f"[Graph] Batch ERROR: {e}")
        state["error"] = f"batch_error: {e}"
    return state

async def finalize_parallel(state: GraphState) -> GraphState:
    """최종 판단 (병렬 LLM 호출)"""
    try:
        # 1. 최종 입력을 위한 AgentInput 모델 준비
        #    RAG/Batch 요약 결과를 주입
        final_inp_dict = dict(state["inp"])
        final_inp_dict["category"] = state.get("category")
        final_inp_dict["rag_summary"] = state.get("rag_summary")
        final_inp_dict["batch_summary"] = state.get("batch_summary")
        
        ai = AgentInput(**final_inp_dict)

        # 2. 병렬 체인 호출 (Pydantic In -> Dict[str, Pydantic] Out)
        print("[Graph] Finalize Parallel: Invoking Price, Deposit, Rules...")
        parallel_results = await chain_parallel_finalize.ainvoke(ai)

        # 3. Pydantic 모델을 State에 저장
        price_decision = parallel_results.get("price")
        if price_decision:
            state["price_decision"] = price_decision
            # 가격 결정만 캐시
            set_verdict(state["signature"], price_decision)
            print("[Graph] Finalize Parallel: Price OK")

        state["deposit_decision"] = parallel_results.get("deposit")
        print("[Graph] Finalize Parallel: Deposit OK")
        
        state["rules_decision"] = parallel_results.get("rules")
        print("[Graph] Finalize Parallel: Rules OK")
        
    except Exception as e:
        print(f"[Graph] Finalize Parallel ERROR: {e}")
        state["error"] = f"finalize_error: {e}"
    return state


# --- LangGraph 엣지(Edge) / 게이트(Gate) 정의 ---

def gate_after_cache(state: GraphState) -> str:
    """캐시 히트 여부 분기"""
    return "finalize_parallel" if state.get("cache_hit") else "run_probe"

def gate_after_probe(state: GraphState) -> str:
    """정보 필요도(info_need)에 따른 RAG 분기"""
    need = state.get("info_need", "low")
    if need == "none":
        return "run_batch" # RAG 생략
    if need in ("medium", "high"):
        return "retrieve_both" # 내부 + 외부 RAG
    return "retrieve_internal" # low (내부 RAG만)


# --- 그래프 배선 ---

graph = StateGraph(GraphState)

# 노드 추가
graph.add_node("enrich_input", enrich_input)
graph.add_node("check_verdict_cache", check_verdict_cache)
graph.add_node("run_probe", run_probe)
graph.add_node("retrieve_internal", retrieve_internal_node)
graph.add_node("retrieve_web", retrieve_web_node)
graph.add_node("retrieve_both", retrieve_both_node)
graph.add_node("merge_evidence", merge_evidence_node)
graph.add_node("summarize_rag_evidence", summarize_rag_evidence)
graph.add_node("run_batch", run_batch_node)
graph.add_node("finalize_parallel", finalize_parallel)

# 엣지 연결
graph.set_entry_point("enrich_input")
graph.add_edge("enrich_input", "check_verdict_cache")

# 1. 캐시 분기
graph.add_conditional_edges("check_verdict_cache", gate_after_cache, {
    "finalize_parallel": "finalize_parallel", # 캐시 히트 시 바로 종료
    "run_probe": "run_probe",           # 캐시 미스 시 Probe
})

# 2. Probe -> RAG 분기
graph.add_conditional_edges("run_probe", gate_after_probe, {
    "run_batch": "run_batch",             # RAG 생략
    "retrieve_both": "retrieve_both",     # RAG (둘 다)
    "retrieve_internal": "retrieve_internal" # RAG (내부만)
})

# 3. RAG -> RAG 요약 -> 배치
graph.add_edge("retrieve_internal", "merge_evidence")
graph.add_edge("retrieve_web", "merge_evidence") # (retrieve_both는 둘 다 실행 후 자동으로 여기로 감)
graph.add_edge("retrieve_both", "merge_evidence")
graph.add_edge("merge_evidence", "summarize_rag_evidence")
graph.add_edge("summarize_rag_evidence", "run_batch")

# 4. 배치 -> 최종
graph.add_edge("run_batch", "finalize_parallel")
graph.add_edge("finalize_parallel", END)

# 컴파일
app = graph.compile()


# --- 실행 헬퍼 ---
async def run_once(payload: Dict[str, Any]) -> Dict[str, Any]:
    state: GraphState = {"inp": payload}
    final_state = await app.ainvoke(state)
    
    print("\n--- Final State ---")
    # Pydantic 모델을 dict로 변환하여 출력
    output = {
        "price": final_state.get("price_decision").model_dump() if final_state.get("price_decision") else None,
        "deposit": final_state.get("deposit_decision").model_dump() if final_state.get("deposit_decision") else None,
        "rules": final_state.get("rules_decision").model_dump() if final_state.get("rules_decision") else None,
        "cache_hit": final_state.get("cache_hit", False),
        "error": final_state.get("error"),
    }
    return output

if __name__ == "__main__":
    sample = {
        "name": "전기자전거 KCS 48V 450W 15.6Ah",
        "description": "배터리 최근 교체, 경량 프레임. 시승 가능.",
        "category": "e-bike",
        "condition": "A",
    }
    print("[Graph] Running sample...")
    
    decision = asyncio.run(run_once(sample))
    
    print(json.dumps(decision, ensure_ascii=False, indent=2))