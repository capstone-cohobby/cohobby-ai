from __future__ import annotations

import json, re, os
from typing import Any, Optional
import asyncio
from cache.redis_client import set_cached, get_cached
from hosts.agent.llm import chat_claude, TOOLS
from langchain_core.prompts import ChatPromptTemplate
from hosts.agent.prompts.prompt import SYSTEM_PROMPT_BATCH
from hosts.agent.tools.mcp_tools import get_price_summary_from_s3  # MCP 툴 (S3→records)

BUCKET = os.getenv("AWS_S3_BUCKET")
KEY    = os.getenv("S3_INPUT_KEY", "out.jsonl")

# LLM 체인 (배치)
_batch_prompt = ChatPromptTemplate.from_messages([
    ("system", SYSTEM_PROMPT_BATCH),
    ("human", "{user_json}")
])
# 배치에서는 툴이 꼭 필요하진 않지만, 기존 구조 유지
_chain_batch = _batch_prompt | chat_claude


# --------------------------------------------------------------------
# judge.py와 동일한 스타일의 LLM 재시도 + 로깅
# --------------------------------------------------------------------
async def _ainvoke_with_retry(chain, user_json: str, max_retries: int = 2):
    last = None
    for attempt in range(max_retries + 1):
        try:
            print(f"[BATCH] LLM call attempt={attempt} payload_len={len(user_json)}")
            res = await chain.ainvoke({"user_json": user_json})
            print(f"[BATCH] LLM call success on attempt={attempt}")
            return res
        except Exception as e:
            last = e
            print(f"[BATCH][ERROR] attempt={attempt} failed: {type(e).__name__}: {e}")
    print("[BATCH][FATAL] All retries exhausted. Raising last error.")
    if last:
        raise last


# --------------------------------------------------------------------
# judge.py와 동일한 JSON 파싱 로직
# --------------------------------------------------------------------
def _extract_json(s: str) -> Optional[str]:
    if not s:
        return None
    # </thinking> 이후부터 잘라내기 (모델이 사고 과정 토큰을 섞었을 때 대비)
    pos = s.rfind("</thinking>")
    if pos != -1:
        s = s[pos + len("</thinking>"):]
    text = s.strip()
    if not text:
        return None
    # ```json 코드펜스 우선
    for m in re.finditer(r"```(?:json)?\s*([\s\S]*?)\s*```", text):
        cand = m.group(1).strip()
        try:
            json.loads(cand)
            return cand
        except Exception:
            pass
    # 가장 큰 중괄호 블록 스캔
    braces = [m.start() for m in re.finditer(r"\{", text)]
    for st in braces:
        for ed in range(len(text), st + 1, -1):
            cand = text[st:ed].strip()
            if not cand.endswith("}"):
                continue
            try:
                json.loads(cand)
                return cand
            except Exception:
                continue
    return None


def _json_from_content(content: Any) -> str:
    # Anthropic output_json 블록이 있으면 최우선 사용
    if isinstance(content, list):
        for b in content:
            if isinstance(b, dict) and b.get("type") == "output_json" and "output_json" in b:
                return json.dumps(b["output_json"], ensure_ascii=False)
    # 텍스트 합치기
    if isinstance(content, list):
        text = "".join((b.get("text", "") or "") for b in content if isinstance(b, dict) and b.get("type") == "text")
    else:
        text = str(content or "")
    cand = _extract_json(text)
    if cand:
        return cand

    # 디버그 덤프
    print("=" * 50)
    print("DEBUG[Batch]: No JSON found in raw content from LLM. Raw text was:")
    print(text)
    print("=" * 50)

    raise ValueError("No valid JSON found in content")


# --------------------------------------------------------------------
# 캐시 키
# --------------------------------------------------------------------
def _batch_key(category: str, signature: str) -> str:
    return f"batch:{category}:{signature}"


# --------------------------------------------------------------------
# 메인: Redis miss면 MCP(S3) → LLM 배치 → Redis 캐시
# judge 스타일의 JSON 파서로 결과 처리
# --------------------------------------------------------------------
async def run_batch_and_cache(name: str, category: str, signature: str) -> Optional[dict]:
    """Redis miss 시 S3→LLM 배치 판단을 수행하고 Redis에 저장."""
    # 0) 캐시 확인
    print(f"[BATCH] run_batch_and_cache: name={name}, category={category}, signature={signature}")
    print(f"[BATCH] S3 target: bucket={BUCKET}, key={KEY}")
    try:
        cached = get_cached(_batch_key(category, signature))
        if isinstance(cached, dict) and cached:
            return cached
        if isinstance(cached, str) and cached.strip():
            return json.loads(cached)
    except Exception:
        # Redis 장애시에도 배치 판단은 계속 시도
        pass

    # 1) MCP로 S3 records 수집
    try:
        records = await asyncio.to_thread(
            get_price_summary_from_s3,
            bucket=BUCKET, key=KEY, limit=50
        )
        print(f"[MCP] fetched records: {0 if records is None else len(records)}")
    except Exception as e:
        print(f"[MCP][ERROR] fetch_core_from_s3 failed: {type(e).__name__}: {e}")
        return None
    if not records:
        print(f"[MCP] no records for bucket={BUCKET} key={KEY}")
        return None

    # 2) LLM 배치 판단
    user_obj = {"name": name, "category": category, "raw": records}
    raw = await _ainvoke_with_retry(_chain_batch, json.dumps(user_obj, ensure_ascii=False))

    # judge.py와 동일한 방식으로 메시지 content에서 JSON 추출
    try:
        content = getattr(raw, "content", None)
        s = _json_from_content(content)
        analysis = json.loads(s)
    except Exception as e:
        print(f"[Batch][ERROR] JSON parse failed: {type(e).__name__}: {e}")
        print(f"[Batch][DUMP] type(raw)={type(raw)} repr(raw)={repr(raw)[:600]}")
        return None

    # 3) Redis 저장 (가능할 때만)
    try:
        set_cached(_batch_key(category, signature), analysis)
    except Exception:
        pass

    return analysis
