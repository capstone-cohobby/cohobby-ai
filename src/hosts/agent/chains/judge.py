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
    original_s = s  # 디버깅용 원본 저장
    
    # </thinking> 태그 제거
    pos = s.rfind("</thinking>")
    if pos != -1:
        s = s[pos + len("</thinking>"):]
    
    s = s.strip()
    
    # 1. ```json 코드 블록 우선 검색 (가장 신뢰할 만함)
    json_block_match = re.search(r"```(?:json)?\s*(\{[\s\S]*?\})\s*```", s, re.DOTALL)
    if json_block_match:
        candidate = json_block_match.group(1).strip()
        try:
            # 유효한 JSON인지 검증
            json.loads(candidate)
            return candidate
        except (json.JSONDecodeError, ValueError) as e:
            print(f"[JSON Extract] Code block JSON parse failed: {e}")
    
    # 2. 중괄호로 시작하는 모든 가능한 JSON 객체 시도
    # 가장 큰 중괄호 블록부터 시도 (중첩된 JSON 처리)
    brace_starts = [m.start() for m in re.finditer(r"\{", s)]
    
    # 가장 큰 블록부터 시도 (가장 완전한 JSON일 가능성이 높음)
    for start_pos in brace_starts:
        # 끝에서부터 시작하여 유효한 JSON을 찾음
        for end_pos in range(len(s), start_pos + 1, -1):
            candidate = s[start_pos:end_pos].strip()
            if not candidate.endswith("}"):
                continue
            try:
                # 유효한 JSON인지 검증
                parsed = json.loads(candidate)
                # 파싱 성공 시 반환
                return candidate
            except json.JSONDecodeError as e:
                # JSON 에러가 발생했지만, 불완전한 JSON일 수 있으므로 계속 시도
                continue
            except (ValueError, TypeError) as e:
                continue
    
    # 3. 불완전한 JSON 복구 시도 (마지막 중괄호가 없거나 잘린 경우)
    # 첫 번째 { 부터 끝까지를 시도하고, 누락된 } 추가 시도
    if brace_starts:
        first_brace = brace_starts[0]
        candidate = s[first_brace:].strip()
        
        # 누락된 닫는 중괄호 추가 시도
        open_count = candidate.count("{")
        close_count = candidate.count("}")
        missing_closes = open_count - close_count
        
        if missing_closes > 0:
            # 누락된 닫는 중괄호 추가
            candidate_fixed = candidate + "}" * missing_closes
            try:
                parsed = json.loads(candidate_fixed)
                print(f"[JSON Extract] Fixed incomplete JSON by adding {missing_closes} closing braces")
                return candidate_fixed
            except (json.JSONDecodeError, ValueError):
                pass
        
        # 또는 마지막 불완전한 필드 제거 시도
        # 마지막 쉼표나 불완전한 필드를 제거
        lines = candidate.split("\n")
        for i in range(len(lines), 0, -1):
            candidate_truncated = "\n".join(lines[:i]).rstrip().rstrip(",")
            if candidate_truncated.endswith("}"):
                try:
                    parsed = json.loads(candidate_truncated)
                    print(f"[JSON Extract] Fixed incomplete JSON by truncating")
                    return candidate_truncated
                except (json.JSONDecodeError, ValueError):
                    continue
    
    # 4. 실패 시 상세한 디버깅 정보와 함께 에러
    print(f"[JSON Extract] Failed to extract JSON. Content length: {len(s)}")
    print(f"[JSON Extract] First 1000 chars: {s[:1000]}")
    print(f"[JSON Extract] Last 500 chars: {s[-500:]}")
    print(f"[JSON Extract] Brace count - Open: {s.count('{')}, Close: {s.count('}')}")
    
    raise ValueError(f"No valid JSON object found in content. Content length: {len(s)}, First 1000 chars: {s[:1000]}...")

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
        # price 필드가 없을 수 있으므로 content에서 추출 시도 또는 N/A 표시
        price = doc.get("price")
        if price is None:
            # content에서 가격 정보를 찾으려고 시도 (예: "15,000원", "15000원" 등)
            price_match = re.search(r'(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*원', body_text)
            if price_match:
                price = price_match.group(1) + "원"
            else:
                price = "가격 정보 없음"
        url = doc.get("url") or doc.get("link", "No URL")
        
        header = f"--- 문서 {i+1}: {title} ---"
        price_line = f"가격 정보: {price}"
        source_line = f"URL: {url}"
        body = body_text.strip()[:500]  # 본문은 500자로 제한
        formatted_summaries.append(f"{header}\n{price_line}\n{source_line}\n본문: {body}")
    
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
    
    # [검증] Price 체인에 전달되는 데이터 확인
    print(f"[Chain] Price Input: evidence count={len(ev_list)}, report exists={bool(report)}")
    if ev_list:
        print(f"[Chain] Price Input: First evidence sample - {str(ev_list[0])[:100]}...")

    return {
        "user_json": user_data,
        "rag_analysis_report": report_str, # 프롬프트의 {{rag_analysis_report}} 와 매칭
        "evidence_str": evidence_str       # 원본 증거
    }

# [신규 Helper] 각 LLM에게 필요한 정보만 골라주는 포매터
def _prepare_deposit_rules_input(ai_input: AgentInput) -> Dict[str, str]:
    """AgentInput에서 분쟁 사례 리스트를 꺼내 문자열로 변환"""
    user_json = ai_input.model_dump_json(exclude={"evidence", "dispute_evidence","rag_analysis_report","rag_summary"})
    
    # dispute_evidence 필드에서 분쟁 사례 리스트를 가져옴
    dispute_list = getattr(ai_input, "dispute_evidence", None) or []
    
    # [검증] Deposit/Rules 체인에 전달되는 데이터 확인
    print(f"[Chain] Deposit/Rules Input: dispute_evidence count={len(dispute_list)}")
    if dispute_list:
        print(f"[Chain] Deposit/Rules Input: First dispute sample - {str(dispute_list[0])[:100]}...")
    
    # 포매팅 재활용
    if not dispute_list:
        dispute_str = "검색된 유사 분쟁 사례가 없습니다. 일반적인 안전 수칙을 제안해 주세요."
    else:
        formatted_data = _format_evidence_list_to_string(dispute_list)
        dispute_str = formatted_data.get("source","")
        print(f"[Chain] Deposit/Rules Input: Formatted dispute_str length={len(dispute_str)}")
    
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
    def extract_and_parse(msg):
        """JSON 추출 및 파싱 (에러 핸들링 포함)"""
        try:
            # 0. 응답이 잘렸는지 확인 (finish_reason 체크)
            finish_reason = getattr(msg, "response_metadata", {}).get("finish_reason") if hasattr(msg, "response_metadata") else None
            if not finish_reason:
                # LangChain의 다른 형식 시도
                finish_reason = getattr(msg, "finish_reason", None)
            
            if finish_reason and finish_reason in ["length", "max_tokens"]:
                print(f"[Chain] WARNING: Response was truncated! finish_reason={finish_reason}")
                print(f"[Chain] This may cause incomplete JSON. Consider increasing max_tokens.")
            
            # 1. JSON 문자열 추출
            content = getattr(msg, "content", None)
            content_str = str(content) if content else ""
            print(f"[Chain] Original content length: {len(content_str)}")
            if finish_reason in ["length", "max_tokens"]:
                print(f"[Chain] Content ends with: ...{content_str[-200:]}")
            
            json_str = _extract_json_from_content(content)
            print(f"[Chain] Extracted JSON length: {len(json_str)}")
            print(f"[Chain] Extracted JSON preview (first 500): {json_str[:500]}...")
            if len(json_str) > 500:
                print(f"[Chain] Extracted JSON preview (last 200): ...{json_str[-200:]}")
            
            # 응답이 잘렸고 JSON이 불완전할 가능성이 있는 경우 경고
            if finish_reason in ["length", "max_tokens"]:
                if not json_str.rstrip().endswith("}"):
                    print(f"[Chain] WARNING: JSON appears incomplete (doesn't end with '}}'). Response was likely truncated.")
            
            # 2. JSON 유효성 검증 (Pydantic 전에 먼저 확인)
            try:
                parsed_json = json.loads(json_str)
                print(f"[Chain] JSON parse successful, keys: {list(parsed_json.keys()) if isinstance(parsed_json, dict) else 'not a dict'}")
                
                # 중첩된 구조 확인
                if isinstance(parsed_json, dict) and "price" in parsed_json:
                    price_obj = parsed_json.get("price")
                    if isinstance(price_obj, dict):
                        print(f"[Chain] Price object keys: {list(price_obj.keys())}")
                        if "reference_url" in price_obj:
                            ref_url = price_obj.get("reference_url")
                            print(f"[Chain] reference_url type: {type(ref_url)}, value: {str(ref_url)[:100] if ref_url else None}")
                
            except json.JSONDecodeError as e:
                print(f"[Chain] JSON decode error: {e}")
                print(f"[Chain] Error at line {e.lineno}, column {e.colno}")
                print(f"[Chain] Problematic JSON around error: {json_str[max(0, e.pos-100):e.pos+100]}")
                
                # 응답이 잘린 경우 더 명확한 에러 메시지
                if finish_reason in ["length", "max_tokens"]:
                    raise ValueError(f"JSON parsing failed - response was truncated (finish_reason={finish_reason}). "
                                   f"Consider increasing max_tokens. Error: {e.msg} at line {e.lineno}, column {e.colno}") from e
                else:
                    raise ValueError(f"Invalid JSON format at line {e.lineno}, column {e.colno}: {e.msg}") from e
            
            # 3. Pydantic 모델로 변환
            try:
                result = pydantic_model.model_validate(parsed_json)
                print(f"[Chain] Pydantic validation successful")
                return result
            except Exception as e:
                print(f"[Chain] Pydantic validation error: {type(e).__name__}: {e}")
                print(f"[Chain] Parsed JSON structure (first 1000 chars):")
                print(json.dumps(parsed_json, ensure_ascii=False, indent=2)[:1000])
                
                # 특정 필드 문제 확인
                if isinstance(parsed_json, dict):
                    for key, value in parsed_json.items():
                        if not isinstance(key, str):
                            print(f"[Chain] WARNING: Non-string key found: {key} (type: {type(key)})")
                        if isinstance(value, dict):
                            for sub_key, sub_value in value.items():
                                if not isinstance(sub_key, str):
                                    print(f"[Chain] WARNING: Non-string sub-key in {key}: {sub_key} (type: {type(sub_key)})")
                
                raise
        
        except ValueError as e:
            # JSON 추출 실패
            print(f"[Chain] JSON extraction failed: {e}")
            content = getattr(msg, "content", None)
            if content:
                content_str = str(content)
                print(f"[Chain] Original content length: {len(content_str)}")
                print(f"[Chain] Original content preview (first 1000): {content_str[:1000]}...")
                if len(content_str) > 1000:
                    print(f"[Chain] Original content preview (last 500): ...{content_str[-500:]}")
            raise
        except Exception as e:
            print(f"[Chain] Unexpected error in extract_and_parse: {type(e).__name__}: {e}")
            import traceback
            print(f"[Chain] Traceback: {traceback.format_exc()}")
            raise
    
    return RunnableLambda(extract_and_parse)

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
    
    # [검증] 입력 데이터 확인
    print(f"[Chain] Deriver Input: inp keys={list(inp.keys())}")
    print(f"[Chain] Deriver Input: sale_evidence count={len(sale_evidence)}")
    if sale_evidence:
        print(f"[Chain] Deriver Input: First sale_evidence keys={list(sale_evidence[0].keys())}")
        print(f"[Chain] Deriver Input: First sale_evidence sample - {str(sale_evidence[0])[:150]}...")
    
    # 1. user_json 생성
    user_json_str = json.dumps(inp, ensure_ascii=False)
    
    # 2. sale_evidence_str 생성 (기존 포매터 재활용)
    sale_evidence_str = _format_evidence_list_to_string(sale_evidence).get("source")
    print(f"[Chain] Deriver Input: sale_evidence_str length={len(sale_evidence_str)}")
    print(f"[Chain] Deriver Input: sale_evidence_str preview - {sale_evidence_str[:200]}...")
    
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