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

def _extract_json_from_content(content: Any, msg: Any = None) -> str:
    """AIMessage.content에서 <thinking> 태그와 코드 블록을 제거하고 순수 JSON 추출
    
    Args:
        content: AIMessage.content (문자열 또는 None)
        msg: 전체 AIMessage 객체 (content가 비어있을 때 다른 필드 확인용)
    """
    # content가 비어있을 때 다른 곳에서 데이터 찾기 시도
    if not content or (isinstance(content, str) and len(content.strip()) == 0):
        if msg is not None:
            # tool_calls 확인 (Function Calling 사용 시)
            tool_calls = getattr(msg, "tool_calls", None) or getattr(msg, "tool_calls", [])
            if tool_calls:
                print(f"[JSON Extract] Content is empty, but found {len(tool_calls)} tool_calls")
                # tool_calls에서 JSON 추출 시도
                for tool_call in tool_calls:
                    if hasattr(tool_call, "args"):
                        args = tool_call.args
                        if isinstance(args, dict):
                            # args를 JSON 문자열로 변환
                            return json.dumps(args, ensure_ascii=False)
                    elif isinstance(tool_call, dict) and "args" in tool_call:
                        args = tool_call["args"]
                        if isinstance(args, dict):
                            return json.dumps(args, ensure_ascii=False)
            
            # response_metadata 확인
            response_metadata = getattr(msg, "response_metadata", {})
            if response_metadata:
                print(f"[JSON Extract] Content is empty, checking response_metadata: {response_metadata}")
                # metadata에 JSON이 있을 수 있음
                if "output" in response_metadata:
                    output = response_metadata["output"]
                    if isinstance(output, dict):
                        return json.dumps(output, ensure_ascii=False)
                    elif isinstance(output, str):
                        s = str(output)
                        if s.strip():
                            return s
        
        # 모든 시도 실패
        raise ValueError("No content received from LLM (content is empty or None). "
                        "This may happen if the model used function calling or structured output. "
                        "Check if the LLM response has tool_calls or response_metadata.")
        
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
        
        # 불완전한 필드 복구 시도: rules 배열이 비어있거나 잘린 경우 복구
        # 예: {"rules": [], "reasoning": "..."  같은 경우
        if "rules" in candidate and '"rules"' in candidate:
            # rules 필드가 있는 경우, 불완전한 JSON 복구 시도
            # 마지막 불완전한 필드 제거 시도
            lines = candidate.split("\n")
            for i in range(len(lines), 0, -1):
                candidate_truncated = "\n".join(lines[:i]).rstrip().rstrip(",")
                # 마지막에 불완전한 필드가 있으면 제거하고 닫는 중괄호 추가
                if not candidate_truncated.endswith("}"):
                    candidate_truncated = candidate_truncated.rstrip().rstrip(",")
                    # rules 배열이 있으면 닫기
                    if '"rules"' in candidate_truncated and '"rules"' not in candidate_truncated.split('"rules"')[1].split('}')[0]:
                        # rules 배열 닫기 시도
                        if candidate_truncated.count('[') > candidate_truncated.count(']'):
                            candidate_truncated += "]"
                    candidate_truncated += "}"
                if candidate_truncated.endswith("}"):
                    try:
                        parsed = json.loads(candidate_truncated)
                        print(f"[JSON Extract] Fixed incomplete JSON by truncating and closing")
                        return candidate_truncated
                    except (json.JSONDecodeError, ValueError):
                        continue
        
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
    
    # 4. 마지막 시도: 원본 문자열에서 직접 JSON 패턴 찾기 (잘린 경우 대비)
    # 원본 문자열에서 {"rules" 패턴을 찾아서 부분 JSON 복구 시도
    if len(s) < 100 and "rules" in original_s.lower():
        # 원본에서 {"rules" 부터 시작하는 부분 찾기
        rules_match = re.search(r'\{\s*"rules"\s*:', original_s, re.IGNORECASE)
        if rules_match:
            start_pos = rules_match.start()
            # 시작부터 끝까지 가져오기
            partial_json = original_s[start_pos:].strip()
            # 닫는 중괄호 추가 시도
            open_count = partial_json.count("{")
            close_count = partial_json.count("}")
            if open_count > close_count:
                partial_json += "}" * (open_count - close_count)
            # 최소한의 JSON 구조 복구 시도
            if '"rules"' in partial_json and ']' not in partial_json.split('"rules"')[1].split('}')[0]:
                # rules 배열이 닫히지 않은 경우
                rules_part = partial_json.split('"rules"')[1]
                if '[' in rules_part and ']' not in rules_part.split('}')[0]:
                    # rules 배열 닫기
                    before_close = partial_json.rfind('}')
                    if before_close > 0:
                        partial_json = partial_json[:before_close] + ']' + partial_json[before_close:]
            
            try:
                parsed = json.loads(partial_json)
                print(f"[JSON Extract] Recovered partial JSON from original content")
                return partial_json
            except (json.JSONDecodeError, ValueError):
                pass
    
    # 5. 실패 시 상세한 디버깅 정보와 함께 에러
    print(f"[JSON Extract] Failed to extract JSON. Content length: {len(s)}")
    print(f"[JSON Extract] Original content length: {len(original_s)}")
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
    """[Price] 가격 결정 체인용 입력: User Info + Analyst Report + Raw Evidence (대여 시장만)"""
    # 1. 사용자 입력 (상품명, 상태 등) - 불필요한 evidence 제외
    user_data = ai.model_dump_json(exclude={"evidence", "used_evidence", "dispute_evidence", "rag_analysis_report", "rag_summary"})
    
    # 2. RAG 분석가 리포트
    report = ai.rag_analysis_report or {}
    if isinstance(report, dict):
        report_str = json.dumps(report, ensure_ascii=False, indent=2)
    else:
        report_str = str(report)
        
    # 3. 원본 증거 (대여 시장만 - used_evidence 제외 확인 및 필터링)
    # evidence 리스트를 문자열로 변환
    ev_list = ai.evidence or []
    
    # [중요] used_evidence가 섞여 있는지 확인하고 제거
    used_list = getattr(ai, "used_evidence", None) or []
    if used_list and ev_list:
        # used_evidence의 URL/ID를 수집하여 evidence에서 제외
        used_urls = {doc.get("url", "") for doc in used_list if doc.get("url")}
        used_ids = {doc.get("id", "") for doc in used_list if doc.get("id")}
        used_titles = {doc.get("title", "") for doc in used_list if doc.get("title")}
        
        # evidence에서 used_evidence와 겹치는 항목 제거
        filtered_ev_list = []
        removed_count = 0
        for doc in ev_list:
            doc_url = doc.get("url", "")
            doc_id = doc.get("id", "")
            doc_title = doc.get("title", "")
            
            # used_evidence와 겹치지 않는 경우만 포함
            if (doc_url not in used_urls and 
                doc_id not in used_ids and 
                doc_title not in used_titles):
                filtered_ev_list.append(doc)
            else:
                removed_count += 1
        
        if removed_count > 0:
            print(f"[Chain] Price Input: Removed {removed_count} items from evidence that overlap with used_evidence")
            ev_list = filtered_ev_list
    
    evidence_str = _format_evidence_list_to_string(ev_list).get("source")
    
    # [검증] Price 체인에 전달되는 데이터 확인
    print(f"[Chain] Price Input: evidence count={len(ev_list)}, used_evidence count={len(used_list)}, report exists={bool(report)}")
    if ev_list:
        print(f"[Chain] Price Input: First evidence sample - {str(ev_list[0])[:100]}...")
        # evidence의 source 확인 (대여 시장만 있어야 함)
        ev_sources = {doc.get("source", "unknown") for doc in ev_list}
        print(f"[Chain] Price Input: Evidence sources: {ev_sources}")

    return {
        "user_json": user_data,
        "rag_analysis_report": report_str, # 프롬프트의 {{rag_analysis_report}} 와 매칭
        "evidence_str": evidence_str       # 원본 증거
    }

# [신규 Helper] Deposit 체인용 입력 포매터
def _prepare_deposit_input(ai_input: AgentInput) -> Dict[str, str]:
    """AgentInput에서 중고가 정보만 꺼내 문자열로 변환 (분쟁 사례 제외 - 토큰 절약)"""
    user_json = ai_input.model_dump_json(exclude={"evidence", "used_evidence", "dispute_evidence","rag_analysis_report","rag_summary"})
    
    # used_evidence 필드에서 중고가 정보만 가져옴 (분쟁 사례 제외)
    used_list = getattr(ai_input, "used_evidence", None) or []
    
    # [검증] Deposit 체인에 전달되는 데이터 확인
    print(f"[Chain] Deposit Input: used_evidence count={len(used_list)}")
    if used_list:
        print(f"[Chain] Deposit Input: First used_evidence sample - {str(used_list[0])[:100]}...")
    
    # 중고가 정보 포매팅
    if not used_list:
        used_price_str = "검색된 중고가 정보가 없습니다."
    else:
        formatted_used = _format_evidence_list_to_string(used_list)
        used_price_str = formatted_used.get("source","")
        print(f"[Chain] Deposit Input: Formatted used_price_str length={len(used_price_str)}")
    
    return {
        "user_json": user_json,
        "used_price_evidence_str": used_price_str
    }

# [신규 Helper] Rules 체인용 입력 포매터 (기존 유지)
def _prepare_rules_input(ai_input: AgentInput) -> Dict[str, str]:
    """AgentInput에서 분쟁 사례 리스트를 꺼내 문자열로 변환
    
    빈 값 처리:
    - 빈 리스트 [], None, 빈 문자열 "", "없음", "[]" 등의 경우를 모두 빈 값으로 간주
    """
    user_json = ai_input.model_dump_json(exclude={"evidence", "used_evidence", "dispute_evidence","rag_analysis_report","rag_summary"})
    
    # dispute_evidence 필드에서 분쟁 사례 리스트를 가져옴
    dispute_list = getattr(ai_input, "dispute_evidence", None) or []
    
    # [검증] Rules 체인에 전달되는 데이터 확인
    print(f"[Chain] Rules Input: dispute_evidence count={len(dispute_list)}")
    if dispute_list:
        print(f"[Chain] Rules Input: First dispute sample - {str(dispute_list[0])[:100]}...")
    
    # 빈 값 체크 강화: 빈 리스트, None, 빈 문자열, "없음", "[]" 등 모두 체크
    def _is_empty_dispute(dispute_list) -> bool:
        """분쟁 사례가 실제로 비어있는지 체크"""
        if not dispute_list:
            return True
        if isinstance(dispute_list, str):
            # 문자열인 경우 빈 값 체크
            stripped = dispute_list.strip().lower()
            return not stripped or stripped in ["없음", "[]", "null", "none", ""]
        if isinstance(dispute_list, list):
            # 리스트인 경우 실제 유의미한 데이터가 있는지 체크
            if len(dispute_list) == 0:
                return True
            # 리스트의 각 항목이 실제로 유의미한지 체크 (모든 키가 비어있는 경우 등)
            for item in dispute_list:
                if isinstance(item, dict):
                    # 최소한 하나의 키에 실제 값이 있는지 확인
                    has_content = any(
                        v and str(v).strip() and str(v).strip().lower() not in ["없음", "[]", "null", "none", ""]
                        for v in item.values()
                    )
                    if has_content:
                        return False
            return True  # 모든 항목이 비어있으면 빈 값으로 간주
        return True
    
    # 포매팅 재활용 (토큰 절약: 상위 3개만 사용, 본문 300자로 제한)
    is_empty = _is_empty_dispute(dispute_list)
    
    if is_empty:
        # CASE B: 데이터 없음 - 명확하게 빈 값임을 표시
        # 빈 문자열 또는 명시적 빈 값 표시를 사용하여 프롬프트에서 CASE B로 처리되도록 함
        dispute_str = ""  # 빈 문자열로 설정하여 프롬프트에서 CASE B로 처리되도록 함
        print(f"[Chain] Rules Input: Dispute evidence is empty - CASE B: 카테고리별 점검 가이드 항목 추가")
    else:
        # CASE A: 데이터 있음 - 분쟁 사례 기반 방어 규칙 생성
        # 상위 3개만 사용하고 본문을 300자로 제한
        limited_dispute = dispute_list[:3]
        formatted_data = _format_evidence_list_to_string(limited_dispute)
        # 본문을 더 짧게 만들기 위해 추가 처리
        dispute_str = formatted_data.get("source","")
        
        # 포매팅 결과가 빈 값 메시지인지 확인 (추가 안전장치)
        if not dispute_str or dispute_str.strip() in ["검색된 참고 문서가 없습니다.", "검색된 유사 분쟁 사례가 없습니다."]:
            # 실제로 빈 값으로 판단하여 CASE B로 처리
            dispute_str = ""
            print(f"[Chain] Rules Input: Formatted result is empty message - CASE B: 카테고리별 점검 가이드 항목 추가")
        else:
            # 각 문서의 본문을 300자로 제한 (이미 _format_evidence_list_to_string에서 500자로 제한되어 있지만, 더 줄이기)
            lines = dispute_str.split("\n\n")
            shortened_lines = []
            for line in lines:
                if "본문:" in line:
                    # 본문 부분만 300자로 제한
                    parts = line.split("본문:")
                    if len(parts) == 2:
                        body = parts[1].strip()[:300]
                        shortened_lines.append(parts[0] + "본문: " + body)
                    else:
                        shortened_lines.append(line)
                else:
                    shortened_lines.append(line)
            dispute_str = "\n\n".join(shortened_lines)
            print(f"[Chain] Rules Input: Formatted dispute_str length={len(dispute_str)} (limited to top 3, 300 chars per body) - CASE A: 방어 규칙 생성")
    
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
def create_pydantic_output_parser(pydantic_model: Any, chain_name: str = "Unknown"):
    """Pydantic 모델 파서 생성 (체인 이름을 받아 로깅에 사용)"""
    def extract_and_parse(msg):
        """JSON 추출 및 파싱 (에러 핸들링 포함)"""
        try:
            print(f"[Chain][{chain_name}] Starting JSON extraction and parsing...")
            
            # 0. 응답이 잘렸는지 확인 (finish_reason 체크)
            finish_reason = getattr(msg, "response_metadata", {}).get("finish_reason") if hasattr(msg, "response_metadata") else None
            if not finish_reason:
                # LangChain의 다른 형식 시도
                finish_reason = getattr(msg, "finish_reason", None)
            
            if finish_reason and finish_reason in ["length", "max_tokens"]:
                print(f"[Chain][{chain_name}] WARNING: Response was truncated! finish_reason={finish_reason}")
                print(f"[Chain][{chain_name}] This may cause incomplete JSON. Consider increasing max_tokens.")
            
            # 1. JSON 문자열 추출
            content = getattr(msg, "content", None)
            content_str = str(content) if content else ""
            print(f"[Chain][{chain_name}] Original content length: {len(content_str)}")
            
            # content가 비어있을 때 추가 정보 확인
            if not content_str or len(content_str.strip()) == 0:
                print(f"[Chain][{chain_name}] ERROR: Content is empty!")
                # tool_calls 확인
                tool_calls = getattr(msg, "tool_calls", None)
                if tool_calls:
                    print(f"[Chain][{chain_name}] Found {len(tool_calls)} tool_calls: {tool_calls}")
                # response_metadata 확인
                response_metadata = getattr(msg, "response_metadata", {})
                if response_metadata:
                    print(f"[Chain][{chain_name}] Response metadata: {response_metadata}")
                # additional_kwargs 확인
                additional_kwargs = getattr(msg, "additional_kwargs", {})
                if additional_kwargs:
                    print(f"[Chain][{chain_name}] Additional kwargs: {additional_kwargs}")
            
            if finish_reason in ["length", "max_tokens"]:
                print(f"[Chain][{chain_name}] Content ends with: ...{content_str[-200:]}")
            
            json_str = _extract_json_from_content(content, msg)
            print(f"[Chain][{chain_name}] Extracted JSON length: {len(json_str)}")
            print(f"[Chain][{chain_name}] Extracted JSON preview (first 500): {json_str[:500]}...")
            if len(json_str) > 500:
                print(f"[Chain][{chain_name}] Extracted JSON preview (last 200): ...{json_str[-200:]}")
            
            # 응답이 잘렸고 JSON이 불완전할 가능성이 있는 경우 경고
            if finish_reason in ["length", "max_tokens"]:
                if not json_str.rstrip().endswith("}"):
                    print(f"[Chain][{chain_name}] WARNING: JSON appears incomplete (doesn't end with '}}'). Response was likely truncated.")
            
            # 2. JSON 유효성 검증 (Pydantic 전에 먼저 확인)
            try:
                parsed_json = json.loads(json_str)
                print(f"[Chain][{chain_name}] JSON parse successful, keys: {list(parsed_json.keys()) if isinstance(parsed_json, dict) else 'not a dict'}")
                
                # Rules 체인 특화: rules가 빈 배열이면 경고 (토큰 제한으로 잘린 가능성)
                if chain_name == "Rules" and isinstance(parsed_json, dict):
                    rules = parsed_json.get("rules", [])
                    if not rules or len(rules) == 0:
                        print(f"[Chain][{chain_name}] WARNING: rules array is empty! This may indicate response was truncated.")
                        if finish_reason in ["length", "max_tokens"]:
                            print(f"[Chain][{chain_name}] ERROR: Response was truncated and rules array is empty. This is likely a token limit issue.")
                
                # Deposit 체인 특화 확인
                if chain_name == "Deposit" and isinstance(parsed_json, dict):
                    deposit_amount = parsed_json.get("deposit_amount")
                    print(f"[Chain][{chain_name}] deposit_amount: {deposit_amount} (type: {type(deposit_amount)})")
                    if deposit_amount is None:
                        print(f"[Chain][{chain_name}] WARNING: deposit_amount is None!")
                
                # 중첩된 구조 확인 (Price 체인용)
                if isinstance(parsed_json, dict) and "price" in parsed_json:
                    price_obj = parsed_json.get("price")
                    if isinstance(price_obj, dict):
                        print(f"[Chain][{chain_name}] Price object keys: {list(price_obj.keys())}")
                        if "reference_url" in price_obj:
                            ref_url = price_obj.get("reference_url")
                            print(f"[Chain][{chain_name}] reference_url type: {type(ref_url)}, value: {str(ref_url)[:100] if ref_url else None}")
                
            except json.JSONDecodeError as e:
                print(f"[Chain][{chain_name}] JSON decode error: {e}")
                print(f"[Chain][{chain_name}] Error at line {e.lineno}, column {e.colno}")
                print(f"[Chain][{chain_name}] Problematic JSON around error: {json_str[max(0, e.pos-100):e.pos+100]}")
                
                # 응답이 잘린 경우 더 명확한 에러 메시지
                if finish_reason in ["length", "max_tokens"]:
                    raise ValueError(f"[{chain_name}] JSON parsing failed - response was truncated (finish_reason={finish_reason}). "
                                   f"Consider increasing max_tokens. Error: {e.msg} at line {e.lineno}, column {e.colno}") from e
                else:
                    raise ValueError(f"[{chain_name}] Invalid JSON format at line {e.lineno}, column {e.colno}: {e.msg}") from e
            
            # 3. Pydantic 모델로 변환
            try:
                # Rules 체인 특화: rules가 빈 배열이고 응답이 잘린 경우 에러 메시지 개선
                if chain_name == "Rules" and isinstance(parsed_json, dict):
                    rules = parsed_json.get("rules", [])
                    if not rules or len(rules) == 0:
                        if finish_reason in ["length", "max_tokens"]:
                            raise ValueError(
                                f"[Rules] Response was truncated (finish_reason={finish_reason}) and rules array is empty. "
                                f"This indicates the LLM did not generate any rules before hitting the token limit. "
                                f"Consider: 1) Increasing max_tokens, 2) Simplifying the prompt, 3) Checking if category matching failed. "
                                f"Parsed JSON: {json.dumps(parsed_json, ensure_ascii=False)[:500]}"
                            )
                
                result = pydantic_model.model_validate(parsed_json)
                print(f"[Chain][{chain_name}] Pydantic validation successful")
                if chain_name == "Deposit":
                    print(f"[Chain][{chain_name}] Final deposit_amount: {result.deposit_amount}")
                return result
            except Exception as e:
                print(f"[Chain][{chain_name}] Pydantic validation error: {type(e).__name__}: {e}")
                print(f"[Chain][{chain_name}] Parsed JSON structure (first 1000 chars):")
                print(json.dumps(parsed_json, ensure_ascii=False, indent=2)[:1000])
                
                # 특정 필드 문제 확인
                if isinstance(parsed_json, dict):
                    for key, value in parsed_json.items():
                        if not isinstance(key, str):
                            print(f"[Chain][{chain_name}] WARNING: Non-string key found: {key} (type: {type(key)})")
                        if isinstance(value, dict):
                            for sub_key, sub_value in value.items():
                                if not isinstance(sub_key, str):
                                    print(f"[Chain][{chain_name}] WARNING: Non-string sub-key in {key}: {sub_key} (type: {type(sub_key)})")
                
                raise
        
        except ValueError as e:
            # JSON 추출 실패
            print(f"[Chain][{chain_name}] JSON extraction failed: {e}")
            content = getattr(msg, "content", None)
            if content:
                content_str = str(content)
                print(f"[Chain][{chain_name}] Original content length: {len(content_str)}")
                print(f"[Chain][{chain_name}] Original content preview (first 1000): {content_str[:1000]}...")
                if len(content_str) > 1000:
                    print(f"[Chain][{chain_name}] Original content preview (last 500): ...{content_str[-500:]}")
            raise
        except Exception as e:
            print(f"[Chain][{chain_name}] Unexpected error in extract_and_parse: {type(e).__name__}: {e}")
            import traceback
            print(f"[Chain][{chain_name}] Traceback: {traceback.format_exc()}")
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
    | RunnableLambda(lambda msg: _extract_json_from_content(getattr(msg, "content", None), msg))
    | RunnableLambda(lambda json_str: json.loads(json_str))
)

prompt_price = ChatPromptTemplate.from_messages([("system", SYSTEM_PROMPT_PRICE), ("human", "{user_json}")])
# 1. Price 체인 (기존 유지 - rag_analysis_report 사용)
chain_price = (
    RunnableLambda(_prepare_price_input) # 포매터 교체
    | prompt_price
    | chat_decision
    | create_pydantic_output_parser(PriceDecision, "Price")
)

# 2. Deposit 체인 (수정 - used_price_evidence_str 주입)
# from_template을 사용하여 {user_json}과 {used_price_evidence_str} 변수를 모두 지원
prompt_deposit = ChatPromptTemplate.from_template(SYSTEM_PROMPT_DEPOSIT)

chain_deposit = (
    RunnableLambda(_prepare_deposit_input) # [신규] 중고가 정보 포함 포매터
    | prompt_deposit
    | chat_decision
    | create_pydantic_output_parser(DepositDecision, "Deposit")
)

# 3. Rules 체인 (수정 - dispute_evidence_str 주입)
prompt_rules = ChatPromptTemplate.from_messages([
    ("system", SYSTEM_PROMPT_RULES), 
    ("human", "{user_json}")
])

chain_rules = (
    RunnableLambda(_prepare_rules_input) # [신규] Rules 전용 포매터
    | prompt_rules
    | chat_decision
    | create_pydantic_output_parser(RulesDecision, "Rules")
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
    """GraphState의 inp, sale_evidence, used_evidence를 {user_json}, {sale_evidence_str}, {used_price_evidence_str}로 변환"""
    inp = state_dict.get("inp", {})
    sale_evidence = state_dict.get("sale_evidence", [])
    used_evidence = state_dict.get("used_evidence", [])  # [신규] Fallback 경로에서도 used_evidence 활용
    
    # [검증] 입력 데이터 확인
    print(f"[Chain] Deriver Input: inp keys={list(inp.keys())}")
    print(f"[Chain] Deriver Input: sale_evidence count={len(sale_evidence)}")
    print(f"[Chain] Deriver Input: used_evidence count={len(used_evidence)}")
    if sale_evidence:
        print(f"[Chain] Deriver Input: First sale_evidence keys={list(sale_evidence[0].keys())}")
        print(f"[Chain] Deriver Input: First sale_evidence sample - {str(sale_evidence[0])[:150]}...")
    if used_evidence:
        print(f"[Chain] Deriver Input: First used_evidence keys={list(used_evidence[0].keys())}")
        print(f"[Chain] Deriver Input: First used_evidence sample - {str(used_evidence[0])[:150]}...")
    
    # 1. user_json 생성
    user_json_str = json.dumps(inp, ensure_ascii=False)
    
    # 2. sale_evidence_str 생성 (기존 포매터 재활용)
    sale_evidence_str = _format_evidence_list_to_string(sale_evidence).get("source")
    print(f"[Chain] Deriver Input: sale_evidence_str length={len(sale_evidence_str)}")
    print(f"[Chain] Deriver Input: sale_evidence_str preview - {sale_evidence_str[:200]}...")
    
    # 3. [신규] used_evidence_str 생성 (Fallback 경로에서도 중고가 정보 활용)
    if not used_evidence:
        used_evidence_str = "검색된 중고가 정보가 없습니다."
    else:
        formatted_used = _format_evidence_list_to_string(used_evidence)
        used_evidence_str = formatted_used.get("source", "")
        print(f"[Chain] Deriver Input: used_evidence_str length={len(used_evidence_str)}")
        print(f"[Chain] Deriver Input: used_evidence_str preview - {used_evidence_str[:200]}...")
    
    return {
        "user_json": user_json_str,
        "sale_evidence_str": sale_evidence_str,
        "used_price_evidence_str": used_evidence_str  # [신규] Fallback 경로에서도 중고가 정보 전달
    }

#  대여가 추론기 체인
prompt_deriver = ChatPromptTemplate.from_template(SYSTEM_PROMPT_DERIVER)  # {user_json}, {sale_evidence_str}, {used_price_evidence_str} 변수 사용
chain_derive_rental_price = (
    RunnableLambda(_prepare_deriver_input)
    | prompt_deriver
    | chat_decision # 결정용 LLM 사용
    | create_pydantic_output_parser(PriceDecision)
)