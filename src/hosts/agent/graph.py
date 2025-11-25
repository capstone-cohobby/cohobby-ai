# graph.py
import json
import asyncio
from typing import Dict, Any

from langgraph.graph import StateGraph, END
from .chains.adapters import normalize_doc
# --- 의존성 임포트 ---
# 1. 상태 정의
from .schemas import GraphState, AgentInput

# 2. 조립된 LCEL 체인
from .chains.judge import (
    chain_rag_summarizer, chain_parallel_finalize, chain_derive_rental_price
)

# 3. 개별 툴
from .tools.cache_tools import make_signature, get_verdict, set_verdict
from .tools.rag_tools import retrieve_internal, retrieve_external_web, merge_evidence, retrieve_sale_price_web, retrieve_used_price_web, retrieve_dispute_cases
from .chains.ls_retrieval_logger import log_retrieval_to_langsmith
# --- LangGraph 노드(Node) 정의 ---

def enrich_input(state: GraphState) -> GraphState:
    """입력 전처리 및 서명 생성"""
    inp = dict(state["inp"])
    tail = []
    if inp.get("condition"): tail.append(f"상태: {inp['condition']}")
    if inp.get("bought_at"): tail.append(f"구입시기: {inp['bought_at']}")
    if inp.get("category"): tail.append(f"카테고리: {inp['category']}")
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
    cat = (inp.get("category") or "").strip()
    terms =[]
    if name:
        terms.append(name)
    if cat: terms.append(cat)
    # name/desc에 이미 "대여/렌탈" 류가 없을 때만
    base = " ".join(x for x in [name, cat] if x).strip()
    tail = "대여 렌탈 일일 요금 대여료 보증금"
    q = base if any(t in base for t in ["대여","렌탈","대여료","요금"]) else f"{base} {tail}".strip()
    print(f"[Graph] RAG Query = {q}")  # 쿼리 로깅
    
    return q

async def _build_sale_query(state: GraphState) -> str:
    """'중고/판매' 쿼리 생성기"""
    inp = state["inp"]
    name = (inp.get("name") or "").strip()
    q = " ".join([name]).strip()
    print(f"[Graph] RAG Query = {q}")  # 쿼리 로깅
    return q

async def _build_dispute_query(state: GraphState) -> str:
    """분쟁 조정 사례 쿼리 생성기"""
    inp = state["inp"]
    name = (inp.get("name") or "").strip()
    category = (inp.get("category") or "").strip()
    q = f"{name} {category} 분쟁 파손 하자".strip()
    print(f"[Graph] Dispute RAG Query = {q}")  # 쿼리 로깅
    return q

async def retrieve_all_node(state: GraphState) -> GraphState:
    """
    [통합 RAG 노드]
    1. 가격 산정용 (Internal + Web)
    2. 규칙 생성용 (Dispute)
    데이터를 한 번에 병렬로 가져오기
    """
    print("[Graph] Retrieving ALL Evidence (Internal + Web + Dispute)...")
    
    # 1. 쿼리 생성 (목적에 따라 다르게 생성 가능)
    q_price = await _build_rag_query(state)      # 예: "맥북 프로 대여"
    q_dispute = await _build_dispute_query(state) # 예: "맥북 프로 디지털가전"

    # 2. 3개 채널 병렬 실행
    # (내부 DB, 외부 웹, 분쟁 DB)
    task_internal = retrieve_internal(q_price)
    task_web = retrieve_external_web(q_price)
    task_dispute = retrieve_dispute_cases(q_dispute)
    
    internal_docs, web_docs, dispute_docs = await asyncio.gather(
        task_internal, task_web, task_dispute
    )
    
    # 3. State에 원본 저장
    state["internal_docs"] = internal_docs
    state["web_docs"] = web_docs
    state["dispute_evidence"] = dispute_docs # [New] 분쟁 데이터는 따로 저장 (규칙 생성용)
    
    print(f"[Graph] Docs Found -> Internal: {len(internal_docs)}, Web: {len(web_docs)}, Dispute: {len(dispute_docs)}")

    # -------------------------------------------------------
    # 4. [중요] 가격 산정용 데이터 정규화 및 병합 (기존 로직 유지)
    # -------------------------------------------------------
    # Analyst LLM은 포맷이 통일된 리스트를 원하므로 정규화 필수
    k = state.get("k", 8)
    
    # (1) 정규화 (Internal, Web 문서를 동일한 스키마로 변환)
    norm_internal = [normalize_doc(d, "internal") for d in (internal_docs or [])]
    norm_web = [normalize_doc(d, "web") for d in (web_docs or [])]
    
    # (2) 병합 (중복 제거 및 점수 기반 정렬)
    # merge_evidence 함수 내부에서 중복 제거나 정렬을 수행한다고 가정
    # 만약 merge_evidence가 정규화된 것을 받지 않는다면, 
    # 기존처럼 raw를 넘기고 내부에서 처리하거나 여기서 합쳐줍니다.
    # 여기서는 직관적으로 합쳐서 state["evidence"]에 넣습니다.
    
    # *참고: merge_evidence 함수가 dict 리스트를 받아 중복제거 후 top_k를 반환한다고 가정
    merged_price_evidence = merge_evidence(norm_internal,norm_web , top_k=k)
    state["evidence"] = merged_price_evidence 
    
    print(f"[Graph] Price Evidence Merged: {len(state['evidence'])} items")

    # -------------------------------------------------------
    # 5. [복구] LangSmith 로깅
    # -------------------------------------------------------
    # 가격 산정용 데이터(evidence)에 대해서만 로깅을 수행합니다.
    # (분쟁 데이터는 성격이 달라 리트리버 평가 지표가 다를 수 있으므로 제외하거나 별도 로깅)
    
    # 로깅을 위해 정규화된 리스트를 다시 만듦 (merge_evidence가 raw를 반환하는 경우 대비)
    # 실제 로깅엔 'merged_price_evidence' 내용을 기반으로 넘기는 것이 좋습니다.
    
    try:
        log_retrieval_to_langsmith(
            query=q_price,
            docs=merged_price_evidence, # 최종적으로 LLM이 볼 데이터
            k=k,
            freshness_days=state.get("freshness_days", 90),
            metadata={
                "node": "retrieve_all", 
                "retriever": "hybrid-triple-source",
                "dispute_count": len(dispute_docs) # 메타데이터에 분쟁 검색 결과 수 포함
            },
        )
    except Exception as e:
        print(f"[Graph] Logging Warning: {e}")

    return state

async def summarize_rag_evidence(state: GraphState) -> GraphState:
    """RAG 결과 분석 및 요약 LLM 호출 (Analyst)"""
    evidence = state.get("evidence")
    if not evidence:
        print("[Graph] RAG Analyst: No evidence to summarize.")
        state["rag_analysis_report"] = None 
        return state
    
    try:
        # chain_rag_summarizer가 이제 JSON(Dict)을 반환한다고 가정
        analysis_report = await chain_rag_summarizer.ainvoke(evidence)
        
        # -rag_analysis_report에 저장
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
        final_inp_dict["rag_analysis_report"] = state.get("rag_analysis_report")
        final_inp_dict["evidence"] = state.get("evidence",[])
        final_inp_dict["dispute_evidence"] = state.get("dispute_evidence", [])
        
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

async def retrieve_used_price_node(state: GraphState) -> GraphState:
    """ 중고가 RAG 실행"""
    print("[Graph] Fallback: Retrieving Used Price data...")
    q = await _build_sale_query(state)
    
    #  중고가 검색
    used_docs = await retrieve_used_price_web(q)
    
    state["used_evidence"] = used_docs
    print(f"[Graph] Fallback Used RAG: Found {len(used_docs)} docs")
    return state

async def retrieve_sale_price_node(state: GraphState) -> GraphState:
    """ 판매가 RAG 실행"""
    print("[Graph] Fallback: Retrieving Sale Price data...")
    q = await _build_sale_query(state)
    
    # [수정] 판매가 검색은 웹만 사용 (내부 DB는 대여가 중심이라 가정)
    sale_docs = await retrieve_sale_price_web(q)
    
    state["sale_evidence"] = sale_docs
    print(f"[Graph] Fallback Sale RAG: Found {len(sale_docs)} docs")
    return state

async def derive_rental_price_node(state: GraphState) -> GraphState:
    """ 판매가 기반 대여가 추론 LLM 호출"""
    print("[Graph] Fallback: Invoking Deriver LLM...")

    try:
        # [수정] chain_derive_rental_price는 state 딕셔너리의 일부를 입력받음
        deriver_input = {
            "inp": state.get("inp"),
            "sale_evidence": state.get("sale_evidence"),
            "used_evidence": state.get("used_evidence", []),
        }
        derived_price_decision = await chain_derive_rental_price.ainvoke(deriver_input)
        
        # 기존 PriceDecision을 덮어쓰기
        state["price_decision"] = derived_price_decision
        print("[Graph] Fallback Deriver: Price OK (Overwritten)")
        
        # 최종 결정된 가격을 캐시에 저장
        set_verdict(state["signature"], derived_price_decision)
        print("[Cache] SET OK (Derived):", state["signature"])
        
    except Exception as e:
        print(f"[Graph] Fallback Deriver ERROR: {e}")
        state["error"] = f"deriver_error: {e}"
        # 추론 실패 시 기존 'uncertain' 결정이 캐시되지 않고 유지됨
        
    return state

async def retrieve_dispute_cases_node(state: GraphState) -> GraphState:
    """ 분쟁 조정 사례 RAG 실행"""
    print("[Graph] Retrieving Dispute Cases...")
    q = await _build_dispute_query(state)
    
    # 분쟁 조정 사례 검색
    dispute_docs = await retrieve_dispute_cases(q)
    
    state["dispute_evidence"] = dispute_docs
    print(f"[Graph] Dispute Cases RAG: Found {len(dispute_docs)} docs")
    return state

# --- LangGraph 엣지(Edge) / 게이트(Gate) 정의 ---
def gate_after_cache(state: GraphState) -> str:
    """캐시 히트 여부 분기"""
    if state.get("cache_hit"):
        return "finalize_parallel"
    return "retrieve_all"  # 캐시 미스 시 RAG 수행

# [신규] 가격 결정 후 Fallback 여부 분기
def gate_after_price(state: GraphState) -> str:
    """가격 결정의 신뢰도에 따라 Fallback 실행 여부 결정"""
    price_decision = state.get("price_decision")
    
    if price_decision and price_decision.decision == "reasonable":
        print("[Graph] Gate: Price is 'reasonable'. Caching and Ending.")
        # [신규] 'reasonable'일 때만 캐시 저장
        set_verdict(state["signature"], price_decision)
        print("[Cache] SET OK (Reasonable):", state["signature"])
        return "END"
    else:
        decision_str = price_decision.decision if price_decision else "None"
        print(f"[Graph] Gate: Price is '{decision_str}'. Triggering Fallback Sale RAG.")
        return "fallback_sale_search"
# --- 그래프 배선 ---

graph = StateGraph(GraphState)

# --- [수정] 1. 노드 추가 (먼저 정의) ---
graph.add_node("enrich_input", enrich_input)
graph.add_node("check_verdict_cache", check_verdict_cache)
graph.add_node("retrieve_all", retrieve_all_node) 
graph.add_node("summarize_rag_evidence", summarize_rag_evidence)
graph.add_node("finalize_parallel", finalize_parallel)
graph.add_node("retrieve_sale_price_node", retrieve_sale_price_node)
graph.add_node("derive_rental_price_node", derive_rental_price_node)

# --- [수정] 2. 엣지 연결 (한 번만 정의) ---
graph.set_entry_point("enrich_input")
graph.add_edge("enrich_input", "check_verdict_cache")

# 1. 캐시 분기 
graph.add_conditional_edges("check_verdict_cache", gate_after_cache, {
    "finalize_parallel": "finalize_parallel", # 캐시 히트 시
    "retrieve_all": "retrieve_all"  # 캐시 미스 시
})

# 2.  RAG 
graph.add_edge("retrieve_all", "summarize_rag_evidence")

#3. Merge Evidence -> RAG Summarizer
graph.add_edge("summarize_rag_evidence", "finalize_parallel")

# 4. [신규] 최종 결정 후 Fallback 분기
graph.add_conditional_edges("finalize_parallel", gate_after_price, {
    "END": END, # 'reasonable'일 때
    "fallback_sale_search": "retrieve_sale_price_node" # 'uncertain'일 때
})
graph.add_edge("retrieve_sale_price_node", "derive_rental_price_node")
graph.add_edge("derive_rental_price_node", END)

# 컴파일
app_graph = graph.compile()

# --- 실행 헬퍼 ---
async def run_once(payload: Dict[str, Any]) -> Dict[str, Any]:
    state: GraphState = {"inp": payload}
    final_state = await app_graph.ainvoke(state)
    
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

## 예시 실행
if __name__ == "__main__":
    sample = {
        "name": "exo 응원봉",
        "condition": "상태 이상 없음",
        "bought_at": "2023-05",
    }
    print("[Graph] Running sample...")
    
    decision = asyncio.run(run_once(sample))
    
    print(json.dumps(decision, ensure_ascii=False, indent=2))