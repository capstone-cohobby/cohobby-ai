from __future__ import annotations
import json, os, asyncio, datetime, random, time
from typing import Any, Dict

from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate

from hosts.agent.llm import chat_claude
from hosts.agent.prompts.prompt import SYSTEM_PROMPT
from hosts.agent.schemas import AgentInput, DecisionOutput
from hosts.agent.tools.mcp import TOOLS

from hosts.cache.redis_client import get_cached, set_cached
from hosts.agent.chains.batch_executor import run_batch_judgment

parser = StrOutputParser()

prompt = ChatPromptTemplate.from_messages([
    ("system", SYSTEM_PROMPT),
    ("human", "{user_json}")
])

first_chain = (
    {"user_json": lambda x: x}  # 아래에서 JSON 문자열 만들어 주입
    | prompt
    | chat_claude.bind_tools(TOOLS)
    | parser
)

second_chain = (
    {"user_json": lambda x: x}
    | prompt
    | chat_claude.bind_tools(TOOLS)
    | parser
)

# ─────────────────────────────────────────────────────────────
# 작은 유틸들
# ─────────────────────────────────────────────────────────────
def _to_json_str(d: Dict[str, Any]) -> str:
    return json.dumps(d, ensure_ascii=False)

def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()

def _extract_json(s: str) -> str:
    """
    모델이 JSON 앞뒤에 프리텍스트/펜스를 붙였을 때를 대비해
    가장 큰 중괄호 블록을 추출.
    """
    start = s.find("{")
    end = s.rfind("}")
    if start != -1 and end != -1 and end > start:
        return s[start:end+1]
    return s  # 그대로 시도

async def _ainvoke_with_retry(chain, user_json: str, retries: int = 2, backoff: float = 0.8) -> str:
    err = None
    for i in range(retries + 1):
        try:
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
    # 간단 보조 규칙(원하면 LLM 보조 분류기 호출로 대체 가능)
    name = getattr(payload, "name", "") or ""
    if any(k in name for k in ["텐트", "캠핑", "코펠", "버너"]):
        return "캠핑용품"
    if any(k in name for k in ["아이패드", "갤럭시 탭", "맥북", "노트북", "태블릿"]):
        return "전자기기"
    return "일반"

# ─────────────────────────────────────────────────────────────
# 메인 judge
# ─────────────────────────────────────────────────────────────
async def judge_once(payload: AgentInput) -> DecisionOutput:
    """자기 신뢰 판단 + Redis/Batch/MCP fallback 포함"""

    # 1) 1차 판단 호출
    user_json_first = _to_json_str(AgentInput(**payload.model_dump()).model_dump())
    raw1 = await _ainvoke_with_retry(first_chain, user_json_first)

    # 2) JSON 파싱
    try:
        first_json = _extract_json(raw1)
        first = DecisionOutput.model_validate_json(first_json)
    except Exception as e:
        raise RuntimeError(f"Claude JSON parse error: {e}\nRaw: {raw1[:400]}") from e

    # 3) info_need 체크
    if first.info_need in ("low", "none"):
        # 메타 보강
        first.timestamp = first.timestamp or _now_iso()
        first.evidence_source = first.evidence_source or "none"
        return first

    # 4) category 확보
    category = _maybe_fill_category(first, payload)
    print(f"[judge] 추가 정보 필요: info_need={first.info_need}, category={category}")

    # 5) Redis 캐시 확인
    cached = get_cached(category)
    if cached:
        print(f"[judge] ✅ Redis hit for '{category}'")
        first.evidence_summary = cached
        first.evidence_source = "redis"
        first.confidence = max(first.confidence, 0.85)
        first.timestamp = first.timestamp or _now_iso()
        return first

    # 6) Redis miss → Batch 실행 (비동기 thread offload)
    print(f"[judge] 🚀 Redis miss → Batch execution for '{category}'")
    batch_result = await asyncio.to_thread(run_batch_judgment, category)

    # 캐시에 저장
    set_cached(category, batch_result)

    # 7) 2차 보강 판단: evidence 주입해서 다시 호출
    enriched_payload: Dict[str, Any] = AgentInput(**payload.model_dump()).model_dump()
    enriched_payload.update({
        "category": category,
        "evidence_summary": batch_result,
        "evidence_source": "batch",
    })
    user_json_second = _to_json_str(enriched_payload)

    raw2 = await _ainvoke_with_retry(second_chain, user_json_second)
    try:
        second_json = _extract_json(raw2)
        second = DecisionOutput.model_validate_json(second_json)
    except Exception as e:
        raise RuntimeError(f"Claude JSON parse error(2nd): {e}\nRaw: {raw2[:400]}") from e

    # 8) 최종 메타/신뢰도 보정
    second.evidence_summary = second.evidence_summary or batch_result
    second.evidence_source = second.evidence_source or "batch"
    second.confidence = min(1.0, (second.confidence + 0.9) / 2)  # Batch evidence 반영 상승
    second.timestamp = second.timestamp or _now_iso()
    return second
