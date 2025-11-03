# judge.py
import json, re
from typing import Any, Optional, List
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableLambda, RunnableParallel

# --- 의존성 임포트 ---
from ..llm import chat_claude # 1. LLM
from ..schemas import (      # 2. Schemas
    AgentInput, ProbeOutput, PriceDecision, DepositDecision, RulesDecision
)
from ..prompts.prompt import (       # 3. Prompts
    SYSTEM_PROMPT_PROBE, SYSTEM_PROMPT_RAG_SUMMARIZER,
    SYSTEM_PROMPT_PRICE, SYSTEM_PROMPT_DEPOSIT, SYSTEM_PROMPT_RULES
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
    | chat_claude
    | create_pydantic_output_parser(ProbeOutput)
)

# --- 2b. RAG 요약 체인 ---
prompt_rag_summarizer = ChatPromptTemplate.from_template(SYSTEM_PROMPT_RAG_SUMMARIZER)
# 이 체인은 AgentInput가 아닌 List[dict]를 받음
chain_rag_summarizer = (
    prompt_rag_summarizer
    | chat_claude
    | RunnableLambda(lambda msg: str(getattr(msg, "content", ""))) # 순수 텍스트 반환
)

# --- 2c. Finalize (Price) 체인 ---
prompt_price = ChatPromptTemplate.from_messages([("system", SYSTEM_PROMPT_PRICE), ("human", "{user_json}")])
chain_price = (
    agent_input_to_json_str
    | prompt_price
    | chat_claude
    | create_pydantic_output_parser(PriceDecision)
)

# --- 2d. Finalize (Deposit) 체인 ---
prompt_deposit = ChatPromptTemplate.from_messages([("system", SYSTEM_PROMPT_DEPOSIT), ("human", "{user_json}")])
chain_deposit = (
    agent_input_to_json_str
    | prompt_deposit
    | chat_claude
    | create_pydantic_output_parser(DepositDecision)
)

# --- 2e. Finalize (Rules) 체인 ---
prompt_rules = ChatPromptTemplate.from_messages([("system", SYSTEM_PROMPT_RULES), ("human", "{user_json}")])
chain_rules = (
    agent_input_to_json_str
    | prompt_rules
    | chat_claude
    | create_pydantic_output_parser(RulesDecision)
)

# --- 3. 최종 병렬 체인 (Graph가 호출할 메인 체인) ---

chain_parallel_finalize = RunnableParallel(
    price=chain_price,
    deposit=chain_deposit,
    rules=chain_rules,
)

# --- (참고) 이전의 judge_once 함수는 더 이상 필요 없음 ---