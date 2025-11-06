# graph_pipeline.py
import json
import asyncio
from typing import Dict, Any

from langgraph.graph import StateGraph, END

# --- 의존성 임포트 ---
# 1. 상태 정의
from ..schemas import GraphState, AgentInput

# 2. 조립된 LCEL 체인
from .judge import (
    chain_rag_summarizer, chain_parallel_finalize
)

# 3. 개별 툴
from ..tools.cache_tools import make_signature, get_verdict, set_verdict
from ..tools.rag_tools import retrieve_internal, retrieve_external_web, merge_evidence

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
        state["info_need"] = "none" # 캐시 히트 시 RAG 불필요
        print("[Graph] Cache HIT")
    else:
        state["cache_hit"] = False
        state["info_need"] = "high" # 캐시 미스 시 RAG 필요
        print("[Graph] Cache MISS")
    return state

async def _build_rag_query(state: GraphState) -> str:
    inp = state["inp"]
    name = (inp.get("name") or "").strip()
    category = (inp.get("category") or "").strip()
    # 대여/렌탈 중심 키워드 강제 부착
    rental_terms = "대여 렌탈 일일 하루 요금 대여료 보증금"
    # 부가 설명도 퀴리 가중치로 사용 (선택)
    desc = (inp.get("description") or "").strip()
    q = " ".join([name, category, rental_terms, desc]).strip()
    return q

async def retrieve_internal_node(state: GraphState) -> GraphState:
    """내부 RAG 실행"""
    q = await _build_rag_query(state)
    state["internal_docs"] = await retrieve_internal(q)
    print(f"[Graph] Internal RAG: Found {len(state['internal_docs'])} docs")
    return state

async def retrieve_web_node(state: GraphState) -> GraphState:
    """외부 RAG 실행"""
    q = await _build_rag_query(state)
    state["web_docs"] = await retrieve_external_web(q)
    print(f"[Graph] Web RAG: Found {len(state['web_docs'])} docs")
    return state

async def retrieve_both_node(state: GraphState) -> GraphState:
    """RAG 병렬 실행 (Internal + Web)"""
    print("[Graph] RAG Both (Internal + Web)")
    q = await _build_rag_query(state)
    
    # 두 RAG 툴을 동시에 호출
    internal_task = retrieve_internal(q)
    web_task = retrieve_external_web(q)
    
    internal_docs, web_docs = await asyncio.gather(internal_task, web_task)
    
    # state에 결과 저장
    state["internal_docs"] = internal_docs
    state["web_docs"] = web_docs
    
    print(f"[Graph] Internal RAG: Found {len(state['internal_docs'])} docs")
    print(f"[Graph] Web RAG: Found {len(state['web_docs'])} docs")
    return state

async def merge_evidence_node(state: GraphState) -> GraphState:
    """RAG 증거 병합"""
    state["evidence"] = merge_evidence(
        state.get("internal_docs"), 
        state.get("web_docs")
    )
    print(f"[Graph] RAG Merged: Total {len(state['evidence'])} evidences")
    return state

async def summarize_rag_evidence(state: GraphState) -> GraphState:
    """RAG 결과 분석 및 요약 LLM 호출 (Analyst)"""
    evidence = state.get("evidence")
    if not evidence:
        print("[Graph] RAG Analyst: No evidence to summarize.")
        state["rag_analysis_report"] = None # [수정] 키 변경
        return state
    
    try:
        # chain_rag_summarizer가 이제 JSON(Dict)을 반환한다고 가정
        analysis_report = await chain_rag_summarizer.ainvoke(evidence)
        
        # [수정] rag_summary 대신 rag_analysis_report에 저장
        state["rag_analysis_report"] = analysis_report
        
        summary_text = "No summary text"
        if isinstance(analysis_report, dict):
            summary_text = analysis_report.get('summary_text', 'No summary text')
            
        print(f"[Graph] RAG Analyst OK: {summary_text[:50]}...")
        
    except Exception as e:
        print(f"[Graph] RAG Analyst ERROR: {e}")
        state["error"] = f"rag_summary_error: {e}"
    return state

async def finalize_parallel(state: GraphState) -> GraphState:
    """최종 판단 (병렬 LLM 호출)"""
    try:
        # 1. 최종 입력을 위한 AgentInput 모델 준비
        #    RAG 요약 결과를 주입
        final_inp_dict = dict(state["inp"])
        final_inp_dict["category"] = state["inp"].get("category") 
        final_inp_dict["rag_analyis_report"] = state.get("rag_analysis_report")
        
        # (선택) 하위 호환성을 위해 rag_summary에도 텍스트 요약본 주입
        if isinstance(state.get("rag_analysis_report"), dict):
            final_inp_dict["rag_summary"] = state["rag_analysis_report"].get("summary_text")
        else:
            final_inp_dict["rag_summary"] = None
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
    if state.get("cache_hit"):
        return "finalize_parallel"
    return "retrieve_both"  # 캐시 미스 시 RAG 수행

# --- 그래프 배선 ---

graph = StateGraph(GraphState)

# --- [수정] 1. 노드 추가 (먼저 정의) ---
graph.add_node("enrich_input", enrich_input)
graph.add_node("check_verdict_cache", check_verdict_cache)
graph.add_node("retrieve_both", retrieve_both_node) 
graph.add_node("merge_evidence", merge_evidence_node)
graph.add_node("summarize_rag_evidence", summarize_rag_evidence)
graph.add_node("finalize_parallel", finalize_parallel)


# --- [수정] 2. 엣지 연결 (한 번만 정의) ---
graph.set_entry_point("enrich_input")
graph.add_edge("enrich_input", "check_verdict_cache")

# 1. 캐시 분기 
graph.add_conditional_edges("check_verdict_cache", gate_after_cache, {
    "finalize_parallel": "finalize_parallel", # 캐시 히트 시
    "retrieve_both": "retrieve_both"  # 캐시 미스 시
})

# 2.  RAG 
graph.add_edge("retrieve_both", "merge_evidence")

#3. Merge Evidence -> RAG Summarizer
graph.add_edge("merge_evidence", "summarize_rag_evidence")
graph.add_edge("summarize_rag_evidence", "finalize_parallel")

# 4. 최종 노드
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
        
        # RAG Analyst가 본 원본 증거(evidence)를 출력합니다.
        "evidence": final_state.get("evidence", [])
    }
    return output

if __name__ == "__main__":
    sample = {
        "name": "배드민턴 채",
        "condition": "상태 이상 없음",
        "bought_at": "2023-05"
    }
    print("[Graph] Running sample...")
    
    decision = asyncio.run(run_once(sample))
    
    print(json.dumps(decision, ensure_ascii=False, indent=2))