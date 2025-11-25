# judge.py
import json, re
from typing import Any, Optional, Dict, List
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableLambda, RunnableParallel

# --- 의존성 임포트 ---
from ..llm import chat_decision, chat_summarizer# 1. LLM
from ..schemas import (      # 2. Schemas
    AgentInput, PriceDecision, DepositDecision, RulesDecision
)
from ..prompts.prompt import (       # 3. Prompts
    SYSTEM_PROMPT_PROBE, SYSTEM_PROMPT_RAG_SUMMARIZER,
    SYSTEM_PROMPT_PRICE, SYSTEM_PROMPT_DEPOSIT, SYSTEM_PROMPT_RULES, SYSTEM_PROMPT_DERIVER
)

# --- 1. LLM 응답 파서 (공통 유틸) ---

def _extract_json_from_content(content: Any) -> str:
    """AIMessage.content에서 <thinking> 태그와 코드 블록을 제거하고 순수 JSON 추출"""
    if not content:
        raise ValueError("No content received from LLM")
        
    s = str(content) # AIMessage.content가 문자열이라고 가정
    
    pos = s.rfind("</thinking>")
    if pos != -1:
        s = s[pos + len("</thinking>"):]
    
    s = s.strip()
    
    match = re.search(r"\{.*\}", s, re.DOTALL)
    if match:
        return match.group(0)
    
    raise ValueError(f"No valid JSON object found in content: {s[:200]}...")

# --- [추가] RAG 증거(List[dict])를 프롬프트용 단일 문자열로 변환 ---
def _format_evidence_list_to_string(evidence_list: List[Dict[str, Any]]) -> Dict[str, str]:
    """RAG 검색 결과(문서 리스트)를 LLM 프롬프트에 넣을 단일 문자열로 변환"""
    if not evidence_list:
        return {"source": "검색된 참고 문서가 없습니다."}
    
    formatted_summaries = []
    # 리스트를 반복하며 각 문서를 포매팅합니다.
    for i, doc in enumerate(evidence_list):
        title = doc.get("title", "No Title")
        body_text = doc.get("snippet") or doc.get("content") or doc.get("body") or "No Content"
        price = doc.get("price", "N/A")
        url = doc.get("url", "No URL")
        
        header = f"--- 문서 {i+1}: {title} (가격: {price}) ---"
        source_line = f"URL: {url}"
        body = body_text.strip()[:500]
        formatted_summaries.append(f"{header}\n{source_line}\n{body}")
    
    # 프롬프트 템플릿의 {evidence} 변수에 주입될 딕셔너리 반환
    return {"source": "\n\n".join(formatted_summaries)}

def _prepare_price_input(ai: AgentInput) -> Dict[str, str]:
    """[Price] 가격 결정 체인용 입력: User Info + Analyst Report + Raw Evidence"""
    # 1. 사용자 입력 (상품명, 상태 등)
    user_data = ai.model_dump_json(exclude={"evidence", "rag_analysis_report", "rag_summary"})
    
    # 2. RAG 분석가 리포트
    report = ai.rag_analysis_report or {}
    if isinstance(report, dict):
        report_str = json.dumps(report, ensure_ascii=False, indent=2)
    else:
        report_str = str(report)
        
    # 3. 원본 증거 (혹시 프롬프트에서 필요하다면)
    # evidence 리스트를 문자열로 변환
    ev_list = ai.evidence or []
    evidence_str = _format_evidence_list_to_string(ev_list).get("source")

    return {
        "user_json": user_data,
        "rag_analysis_report": report_str, # 프롬프트의 {{rag_analysis_report}} 와 매칭
        "evidence_str": evidence_str       # 원본 증거
    }

# [신규 Helper] 각 LLM에게 필요한 정보만 골라주는 포매터
def _prepare_deposit_rules_input(ai_input: AgentInput) -> Dict[str, str]:
    """AgentInput에서 분쟁 사례 리스트를 꺼내 문자열로 변환"""
    user_json = ai_input.model_dump_json(exclude={"evidence", "dispute_evidence","rag_analysis_report","rag_summary"})
    
    # evidence 필드에 들어있는 분쟁 사례 리스트를 문자열로 변환
    dispute_list = getattr(ai_input, "dispute_evidence", None) or []
    
    # 포매팅 재활용
    if not dispute_list:
        dispute_str = "검색된 유사 분쟁 사례가 없습니다. 일반적인 안전 수칙을 제안해 주세요."
    else:
        formatted_data = _format_evidence_list_to_string(dispute_list)
        dispute_str = formatted_data.get("source","")
    
    return {
        "user_json": user_json,
        "dispute_evidence_str": dispute_str
    }
    
# --- 2. Pydantic In -> Pydantic Out 체인 정의 ---

# [Helper] AgentInput Pydantic 모델을 LLM 입력(JSON 문자열)으로 변환
agent_input_to_json_str = (
    RunnableLambda(lambda x: x.model_dump_json(exclude_unset=True))
    | RunnableLambda(lambda json_str: {"user_json": json_str})
)

# [Helper] LLM 출력(AIMessage)을 Pydantic 모델로 변환 (파서 사용)
def create_pydantic_output_parser(pydantic_model: Any):
    return (
        RunnableLambda(lambda msg: _extract_json_from_content(getattr(msg, "content", None)))
        | RunnableLambda(lambda json_str: pydantic_model.model_validate_json(json_str))
    )

# --- 2a. Probe 체인 ---
prompt_probe = ChatPromptTemplate.from_messages([("system", SYSTEM_PROMPT_PROBE), ("human", "{user_json}")])

chain_probe = (
    agent_input_to_json_str
    | prompt_probe
    | chat_decision
)

# --- 2b. RAG 요약 체인 ---
prompt_rag_summarizer = ChatPromptTemplate.from_template(SYSTEM_PROMPT_RAG_SUMMARIZER)
# 이 체인은 AgentInput가 아닌 List[dict]를 받음
chain_rag_summarizer = (
    RunnableLambda(_format_evidence_list_to_string)
    | prompt_rag_summarizer
    | chat_summarizer
    | RunnableLambda(lambda msg: _extract_json_from_content(getattr(msg, "content", None)))
    | RunnableLambda(lambda json_str: json.loads(json_str))
)

prompt_price = ChatPromptTemplate.from_messages([("system", SYSTEM_PROMPT_PRICE), ("human", "{user_json}")])
# 1. Price 체인 (기존 유지 - rag_analysis_report 사용)
chain_price = (
    RunnableLambda(_prepare_price_input) # 포매터 교체
    | prompt_price
    | chat_decision
    | create_pydantic_output_parser(PriceDecision)
)

# 2. Deposit 체인 (수정 - dispute_evidence_str 주입)
prompt_deposit = ChatPromptTemplate.from_messages([
    ("system", SYSTEM_PROMPT_DEPOSIT), 
    ("human", "{user_json}")
])

chain_deposit = (
    RunnableLambda(_prepare_deposit_rules_input) # 포매터 교체
    | prompt_deposit
    | chat_decision
    | create_pydantic_output_parser(DepositDecision)
)

# 3. Rules 체인 (수정 - dispute_evidence_str 주입)
prompt_rules = ChatPromptTemplate.from_messages([
    ("system", SYSTEM_PROMPT_RULES), 
    ("human", "{user_json}")
])

chain_rules = (
    RunnableLambda(_prepare_deposit_rules_input) # 포매터 교체
    | prompt_rules
    | chat_decision
    | create_pydantic_output_parser(RulesDecision)
)

# --- 3. 최종 병렬 체인 (Graph가 호출할 메인 체인) ---

chain_parallel_finalize = RunnableParallel(
    price=chain_price,
    deposit=chain_deposit,
    rules=chain_rules,
)

# --- [신규] 4. Fallback 대여가 추론기 체인 ---

# [신규 Helper] (Deriver용) 입력 포매터
def _prepare_deriver_input(state_dict: Dict[str, Any]) -> Dict[str, str]:
    """GraphState의 inp와 sale_evidence를 {user_json}과 {sale_evidence_str}로 변환"""
    inp = state_dict.get("inp", {})
    sale_evidence = state_dict.get("sale_evidence", [])
    
    # 1. user_json 생성
    user_json_str = json.dumps(inp, ensure_ascii=False)
    
    # 2. sale_evidence_str 생성 (기존 포매터 재활용)
    sale_evidence_str = _format_evidence_list_to_string(sale_evidence).get("source")
    
    return {
        "user_json": user_json_str,
        "sale_evidence_str": sale_evidence_str
    }

#  대여가 추론기 체인
prompt_deriver = ChatPromptTemplate.from_messages([
    ("system", SYSTEM_PROMPT_DERIVER), # {sale_evidence_str} 변수 사용
    ("human", "{user_json}")
])
chain_derive_rental_price = (
    RunnableLambda(_prepare_deriver_input)
    | prompt_deriver
    | chat_decision # 결정용 LLM 사용
    | create_pydantic_output_parser(PriceDecision)
)