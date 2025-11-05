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
    # --- [수정] --- chain_probe는 더 이상 사용하지 않습니다.
    chain_rag_summarizer, chain_parallel_finalize
)

# 3. 개별 툴
from ..tools.cache_tools import make_signature, get_verdict, set_verdict
from ..tools.rag_tools import retrieve_internal, retrieve_external_web, merge_evidence
# run_batch_and_cache는 사용하지 않습니다.

# --- LangGraph 노드(Node) 정의 ---
import re

RENTAL_TERMS = ["대여", "렌탈", "일일", "하루", "요금", "대여료", "보증금"]
SALE_TERMS   = ["판매", "구매", "매매", "정가", "팝니다", "사세요"]

CATEGORY_ALIASES = {
    "전기자전거": ["전기자전거", "e-bike", "전동자전거"],
    "자전거":     ["자전거", "road bike", "로드바이크", "mtb", "하이브리드 자전거"],
    "킥보드":     ["전동킥보드", "e-scooter", "킥보드"],
}

WON_PATTERN = r'([0-9]{1,3}(?:,[0-9]{3})*|[0-9]+)\s*원'
WINDOW = 24  # 가격 주변 문맥 길이

def _candidate_categories(inp: dict) -> list[str]:
    seed = [inp.get("category")] if inp.get("category") else []
    text = " ".join([inp.get("name",""), inp.get("description","")]).lower()
    guesses = []
    if any(k in text for k in ["e-bike","전기자전거","전동자전거"]): guesses.append("전기자전거")
    if any(k in text for k in ["킥보드","e-scooter"]): guesses.append("킥보드")
    if any(k in text for k in ["자전거","road","mtb","하이브리드"]): guesses.append("자전거")
    guesses.append("자전거")  # 초상위 백오프
    ordered = []
    for c in seed + guesses:
        if c and c not in ordered:
            ordered.append(c)
    return ordered[:3]

def _expand_terms(cats: list[str]) -> list[str]:
    terms = []
    for c in cats:
        terms.extend(CATEGORY_ALIASES.get(c, [c]))
    # unique 유지
    return list(dict.fromkeys(terms))

def _blend_queries(inp: dict) -> tuple[list[str], list[str]]:
    """모델명이 빗나가도 회수율을 확보하기 위한 다중 가설/백오프 쿼리"""
    name = (inp.get("name") or "").strip()
    cats = _candidate_categories(inp)
    cat_terms = _expand_terms(cats)
    rental = " ".join(RENTAL_TERMS)

    q1 = " ".join([name, " ".join(cat_terms), rental]).strip()
    q2 = " ".join([" ".join(cat_terms), rental]).strip()
    q3 = " ".join(["자전거 전동자전거 e-bike 전동킥보드", rental, "서울 당근 네이버카페 중고나라"]).strip()
    return [q1, q2, q3], cats

async def _retrieve_with_blend(retrieve_fn, inp: dict, top_k: int = 10):
    queries, cats = _blend_queries(inp)
    all_docs = []
    for i, q in enumerate(queries, 1):
        docs = await retrieve_fn(q)
        print(f"[RAG] Try{i} q=`{q}` -> {len(docs)} docs")
        all_docs.extend(docs)
    return all_docs, cats, queries

def _doc_text(doc) -> str:
    # retriever별 스키마 차이 방지
    title  = (getattr(doc, "title", None)  or doc.get("title")  or "") 
    snip   = (getattr(doc, "snippet", None)or doc.get("snippet")or "")
    body   = (getattr(doc, "text", None)   or doc.get("text")   or "")
    return f"{title}\n{snip}\n{body}"

def score_doc_for_rental(doc, cats: list[str]) -> float:
    t = _doc_text(doc).lower()
    s = 0.0
    # 대여 용어 보너스
    for k in RENTAL_TERMS:
        if k in t: s += 1.5
    # 판매 용어 패널티
    for k in SALE_TERMS:
        if k in t: s -= 1.2
    # 카테고리 동의어 매칭 가중치
    for c in cats:
        for alias in CATEGORY_ALIASES.get(c, [c]):
            if alias.lower() in t:
                s += 1.5
    return s

def rerank_docs(docs: list, cats: list[str], topk: int = 12) -> list:
    scored = [(d, score_doc_for_rental(d, cats)) for d in docs]
    scored.sort(key=lambda x: x[1], reverse=True)
    return [d for d, _ in scored[:topk]]

def extract_rental_prices(txt: str) -> list[int]:
    prices = []
    for m in re.finditer(WON_PATTERN, txt):
        start = max(0, m.start()-WINDOW); end = m.end()+WINDOW
        window = txt[start:end]
        # 가격 주변에 대여 문맥이 동시 등장할 때만 유효
        if any(k in window for k in RENTAL_TERMS):
            v = int(m.group(1).replace(",", ""))
            prices.append(v)
    return prices

def compute_price_stats_from_docs(docs: list) -> dict:
    """상위 K 문서에서 대여 문맥의 가격 숫자들을 수집하고 통계 산출"""
    vals = []
    for d in docs[:12]:
        vals += extract_rental_prices(_doc_text(d))
    n = len(vals)
    if n == 0:
        return {"n": 0, "values": [], "low": None, "point": None, "high": None}
    vals.sort()
    def q_idx(p):  # 분위수 인덱스
        return min(max(int(round(p*(n-1))), 0), n-1)
    low   = vals[q_idx(0.25)]
    point = vals[q_idx(0.50)]
    high  = vals[q_idx(0.75)]
    return {"n": n, "values": vals, "low": low, "point": point, "high": high}

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
    docs, cats, queries = await _retrieve_with_blend(retrieve_internal, state["inp"])
    # cat 가설은 웹에서 이미 들어갔을 수 있으니 유지/병합
    state["cat_hypotheses"] = state.get("cat_hypotheses", cats)
    state["internal_queries"] = queries
    state["internal_docs"] = rerank_docs(docs, state["cat_hypotheses"])
    print(f"[Graph] Internal RAG: Found {len(state['internal_docs'])} docs")
    return state

async def retrieve_web_node(state: GraphState) -> GraphState:
    """외부 RAG 실행"""
    docs, cats, queries = await _retrieve_with_blend(retrieve_external_web, state["inp"])
    state["cat_hypotheses"] = cats
    state["web_queries"] = queries
    # 리랭킹
    state["web_docs"] = rerank_docs(docs, cats)
    print(f"[Graph] Web RAG: Found {len(state['web_docs'])} docs")
    return state

async def retrieve_both_node(state: GraphState) -> GraphState:
    """RAG 병렬 실행"""
    print("[Graph] RAG Both (Internal + Web)")
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
    """RAG 결과 요약 LLM 호출"""
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

async def finalize_parallel(state: GraphState) -> GraphState:
    """최종 판단 (병렬 LLM 호출)"""
    try:
        # 1. 최종 입력을 위한 AgentInput 모델 준비
        #    RAG 요약 결과를 주입
        final_inp_dict = dict(state["inp"])
        
        # --- [수정] --- 
        # category는 probe가 아닌 원본 입력(inp)에서 가져옵니다.
        final_inp_dict["category"] = state["inp"].get("category") 
        final_inp_dict["rag_summary"] = state.get("rag_summary")
        # batch_summary는 None으로 고정 (제거됨)
        final_inp_dict["batch_summary"] = None 
        
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
    return "retrieve_both"



# --- 그래프 배선 ---

graph = StateGraph(GraphState)

# --- [수정] 1. 노드 추가 (먼저 정의) ---
graph.add_node("enrich_input", enrich_input)
graph.add_node("check_verdict_cache", check_verdict_cache)
graph.add_node("retrieve_internal", retrieve_internal_node)
graph.add_node("retrieve_web", retrieve_web_node)  
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

# 2. RAG -> RAG 요약 -> 최종 판단
graph.add_edge("retrieve_both", "merge_evidence")
graph.add_edge("merge_evidence", "summarize_rag_evidence")
graph.add_edge("summarize_rag_evidence", "finalize_parallel")

# 3. 최종 노드
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
        "name": "exo 응원봉",
        "condition": "불 잘 들어옴",
        "bought_at": "2023-05"
    }
    print("[Graph] Running sample...")
    
    decision = asyncio.run(run_once(sample))
    
    print(json.dumps(decision, ensure_ascii=False, indent=2))