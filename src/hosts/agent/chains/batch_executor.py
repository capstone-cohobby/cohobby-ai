from __future__ import annotations

import json, re, os
from typing import Any, Optional

from cache.redis_client import set_cached, get_cached
from hosts.agent.llm import chat_claude, TOOLS
from langchain_core.prompts import ChatPromptTemplate
from hosts.agent.prompts.prompt import SYSTEM_PROMPT_BATCH
from hosts.agent.tools.mcp_tools import fetch_core_from_s3_tool  # MCP 툴 (S3→records)

BUCKET = os.getenv("AWS_S3_BUCKET")
KEY    = os.getenv("S3_INPUT_KEY", "out.jsonl")

# LLM 체인
_batch_prompt = ChatPromptTemplate.from_messages([
    ("system", SYSTEM_PROMPT_BATCH),
    ("human", "{{user_json}}")
])
_llm_batch = chat_claude.bind_tools(TOOLS, max_tokens=1200)
_chain_batch = _batch_prompt | _llm_batch


async def _ainvoke_with_retry(chain, user_json: str, max_retries: int = 2):
    last = None
    for _ in range(max_retries + 1):
        try:
            return await chain.ainvoke({"user_json": user_json})
        except Exception as e:
            last = e
    if last:
        raise last


def _json_from_content(content: Any) -> str:
    # Anthropic output_json 우선
    if isinstance(content, list):
        for b in content:
            if isinstance(b, dict) and b.get("type") == "output_json" and "output_json" in b:
                return json.dumps(b["output_json"], ensure_ascii=False)
    # 텍스트 합침
    if isinstance(content, list):
        text = "".join((b.get("text", "") or "") for b in content if isinstance(b, dict) and b.get("type") == "text")
    else:
        text = str(content or "")
    # ```json 코드펜스
    for m in re.finditer(r"```(?:json)?\s*([\s\S]*?)\s*```", text):
        cand = (m.group(1) or "").strip()
        try:
            json.loads(cand); return cand
        except Exception:
            pass
    # 중괄호 스캔
    s = text.strip()
    for st in [m.start() for m in re.finditer(r"\{", s)]:
        for ed in range(len(s), st + 1, -1):
            frag = s[st:ed].strip()
            if not frag.endswith("}"): continue
            try:
                json.loads(frag); return frag
            except Exception:
                continue
    raise ValueError("No valid JSON found in content")


def _batch_key(category: str, signature: str) -> str:
    return f"batch:{category}:{signature}"


async def run_batch_and_cache(name: str, category: str, signature: str) -> Optional[dict]:
    """Redis miss 시 S3→LLM 배치 판단을 수행하고 Redis에 저장."""
    # 0) 캐시 확인
    try:
        cached = get_cached(_batch_key(category, signature))
        if isinstance(cached, dict) and cached:
            return cached
        if isinstance(cached, str) and cached.strip():
            return json.loads(cached)
    except Exception:
        # Redis 장애시에도 계속 진행(근거 수집 시도)
        pass

    # 1) MCP로 S3 records 수집
    try:
        records = fetch_core_from_s3_tool.run({"bucket": BUCKET, "key": KEY, "limit": 500})
    except Exception as e:
        print(f"[MCP] fetch_core_from_s3 failed: {e}")
        return None
    if not records:
        print(f"[MCP] no records for bucket={BUCKET} key={KEY}")
        return None

    # 2) LLM 배치 판단
    user_obj = {"name": name, "category": category, "raw": records}
    raw = await _ainvoke_with_retry(_chain_batch, json.dumps(user_obj, ensure_ascii=False))
    content = getattr(raw, "content", None)
    try:
        s = _json_from_content(content)
        analysis = json.loads(s)
    except Exception as e:
        print(f"[Batch] JSON parse failed: {e}")
        return None

    # 3) Redis 저장 (가능할 때만)
    try:
        set_cached(_batch_key(category, signature), analysis)
    except Exception:
        pass
    return analysis
