from __future__ import annotations
import json, os, asyncio, datetime, random, time, re
from typing import Any, Dict

from langchain_core.prompts import ChatPromptTemplate
from langchain_core.messages import AIMessage

from hosts.agent.llm import chat_claude
from hosts.agent.llm import make_agent
agent = make_agent()
from hosts.agent.prompts.prompt import SYSTEM_PROMPT
from hosts.agent.schemas import AgentInput, DecisionOutput
from hosts.agent.tools.mcp import TOOLS

from cache.redis_client import get_cached, set_cached
from hosts.agent.chains.batch_executor import run_batch_judgment
from .classifier import classify_category


prompt = ChatPromptTemplate.from_messages([
    ("system", SYSTEM_PROMPT),
    ("human", "{user_json}")
])

chain = (
    {"user_json": lambda x: x}
    | prompt
    | chat_claude.bind_tools(TOOLS)
)
# ─────────────────────────────────────────────────────────────
# 작은 유틸들
# ─────────────────────────────────────────────────────────────
def _json_from_text_blocks_only(content) -> str:
    """
    Anthropic messages 응답의 content에서
    - output_json 블록 우선 사용
    - 그 외에는 type='text' 블록들만 합쳐서 JSON을 '실제로 파싱 가능한 것'만 골라 반환
    - tool_use / tool_result 등은 절대 보지 않음(여기서 섞이면 파이썬 repr로 깨짐)
    """
    # 1) 리스트형 블록 처리
    if isinstance(content, list):
        # a) output_json 최우선
        for b in content:
            if isinstance(b, dict) and b.get("type") == "output_json":
                data = b.get("output_json")
                return json.dumps(data, ensure_ascii=False) if isinstance(data, (dict, list)) else str(data)

        # b) text 블록만 합치기
        text = "".join(
            (b.get("text", "") or "")
            for b in content
            if isinstance(b, dict) and b.get("type") == "text"
        ).strip()
    else:
        # 2) 문자열이면 그 자체가 텍스트로 간주
        text = (content or "").strip()

    if not text:
        raise ValueError("No text blocks to extract JSON from")

    # 코드블록 제거
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text, flags=re.DOTALL).strip()

    # 우선 순위 1: ```json ... ``` 안에 있는 JSON이 있으면 그걸 시도
    code_blocks = re.findall(r"```(?:json)?\s*([\s\S]*?)\s*```", text)
    for cb in code_blocks:
        try:
            json.loads(cb)
            return cb
        except Exception:
            pass  # 다음 후보

    # 우선 순위 2: 텍스트 전체에서 중괄호 덩어리들을 전부 찾아 하나씩 json.loads 시도
    #   - 첫 { 로부터 } 까지 늘려가며 여러 후보를 테스트
    #   - 성공하는 첫 번째 후보를 반환
    brace_positions = [m.start() for m in re.finditer(r"\{", text)]
    for start in brace_positions:
        # 끝에서부터 줄여가며 시도 (너무 크면 비용이 커지니 적당히 제한 걸어도 됨)
        for end in range(len(text), start + 1, -1):
            cand = text[start:end].strip()
            if not cand.endswith("}"):
                continue
            try:
                json.loads(cand)
                return cand
            except Exception:
                continue

    # 그래도 없으면 실패 처리 (여기서 tool_use로 후퇴하지 말 것!)
    raise ValueError("No valid JSON found in text blocks")

def _to_json_str(d: Dict[str, Any]) -> str:
    return json.dumps(d, ensure_ascii=False)

def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()

def _extract_json(s: str) -> str:
    closing_tag = "</thinking>"
    closing_tag_pos = s.rfind(closing_tag)
    if closing_tag_pos != -1:
        s = s[closing_tag_pos + len(closing_tag):]
    start = s.find("{")
    end = s.rfind("}")
    if start != -1 and end != -1 and end > start:
        return s[start:end+1]
    return s.strip()

async def _ainvoke_with_retry(chain, user_json: str, retries: int = 2, backoff: float = 0.8) -> AIMessage:
    err = None
    for i in range(retries + 1):
        try:
            # ❗️ 이제 반환 타입은 AIMessage 입니다.
            return await chain.ainvoke(user_json)
        except Exception as e:
            err = e
            if i < retries:
                await asyncio.sleep(backoff * (2 ** i) + random.random() * 0.2)
    raise err

def _maybe_fill_category(first: DecisionOutput, payload: AgentInput) -> str:
    cat = (first.category or getattr(payload, "category", None) or "").strip()
    if cat:
        return cat
    print("[judge] 카테고리 정보 없음 → LLM 분류기 호출")
    classified_cat = classify_category(
        name=getattr(payload, "name", ""),
        condition=getattr(payload, "condition", "중고"),
        bought_at=getattr(payload, "bought_at", "1년 이내")
    )
    return classified_cat or "일반"

# ─────────────────────────────────────────────────────────────
# 메인 judge
# ─────────────────────────────────────────────────────────────
async def judge_once(payload: AgentInput) -> DecisionOutput:
    """LLM의 응답이 '도구 사용 요청'인지 '최종 답변'인지 분기하여 처리합니다."""

    # 1) 1차 판단 호출
    user_json_first = _to_json_str(AgentInput(**payload.model_dump()).model_dump())
    raw_message = await _ainvoke_with_retry(chain, user_json_first)
    
    first: DecisionOutput

    # 2) ❗️ LLM의 응답이 '도구 사용 요청'인지 확인합니다.
    if raw_message.tool_calls:
        print(f"[judge] LLM이 도구 사용을 요청했습니다: {raw_message.tool_calls[0]['name']}")
        # 도구를 요청했다는 것은 정보가 더 필요하다는 의미이므로,
        # info_need='medium'으로 간주하고 다음 단계로 넘어갑니다.
        # 이 경우, LLM의 응답에서 decision 등을 파싱할 필요가 없습니다.
        first = DecisionOutput(
            decision="uncertain", 
            confidence=0.5, 
            reasoning="1차 판단에서 정보 부족으로 도구 사용이 필요하다고 판단됨.",
            info_need="medium"
        )
    else:
        # ❗️ '도구 사용 요청'이 아니라면, 최종 답변(JSON)으로 간주하고 파싱합니다.
        print("[judge] LLM이 최종 답변을 반환했습니다.")
        raw1=await _ainvoke_with_retry(chain, user_json_first)
        try:
            first_json = _json_from_text_blocks_only(str(raw_message.content))
            first = DecisionOutput.model_validate_json(first_json)
        except Exception as e:
            preview = str(raw_message.content)[:500]
            raise RuntimeError(f"Claude JSON parse error (first): {e}\nRaw Content: {preview}") from e

    # --- 이하 로직은 거의 동일 ---
    # 3) info_need 체크 (LLM이 직접 판단했거나, 도구 사용 요청으로 간주됨)
    if first.info_need in ("low", "none"):
        first.timestamp = first.timestamp or _now_iso()
        first.evidence_source = first.evidence_source or "none"
        return first

    # 4) category 확보 및 추가 정보 탐색
    category = _maybe_fill_category(first, payload)
    print(f"[judge] 추가 정보 필요: info_need={first.info_need}, category={category}")
    
    cached = get_cached(category)
    if cached:
        print(f"[judge] ✅ Redis hit for '{category}'")
        # (이하 캐시 적중 시 로직은 기존과 동일)
        first.evidence_summary = cached
        first.evidence_source = "redis"
        first.confidence = max(first.confidence, 0.85)
        first.timestamp = first.timestamp or _now_iso()
        return first

    print(f"[judge] 🚀 Redis miss → Batch execution for '{category}'")
    batch_result = await asyncio.to_thread(run_batch_judgment, category)
    set_cached(category, batch_result)
    
    # 5) 2차 보강 판단
    enriched_payload: Dict[str, Any] = AgentInput(**payload.model_dump()).model_dump()
    enriched_payload.update({
        "category": category,
        "evidence_summary": batch_result,
        "evidence_source": "batch",
    })
    user_json_second = _to_json_str(enriched_payload)

    raw2 = await _ainvoke_with_retry(chain, user_json_second)
    try:
        second_json = _json_from_text_blocks_only(getattr(raw2, "content", None))
        second = DecisionOutput.model_validate_json(second_json)
    except Exception as e:
        preview = str(getattr(raw2, "content", ""))[:500]
        raise RuntimeError(f"Claude JSON parse error (second): {e}\nRaw Content: {preview}") from e

    # (이하 최종 결과 보정 로직은 기존과 동일)
    second.evidence_summary = second.evidence_summary or batch_result
    second.evidence_source = second.evidence_source or "batch"
    second.confidence = min(1.0, (second.confidence + 0.9) / 2)
    second.timestamp = second.timestamp or _now_iso()
    return second