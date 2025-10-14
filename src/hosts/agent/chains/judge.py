from __future__ import annotations

import json, re, hashlib, unicodedata
from datetime import datetime, timezone
from typing import Any, Optional

from cache.redis_client import get_cached, set_cached
from langchain_core.prompts import ChatPromptTemplate

from hosts.agent.llm import chat_claude, TOOLS
from hosts.agent.prompts.prompt import (
    SYSTEM_PROMPT_PROBE, SYSTEM_PROMPT_FINAL
)
from hosts.agent.schemas import (
    AgentInput, ProbeOutput, BatchSummaryOutput, DecisionOutput
)
from hosts.agent.chains.batch_executor import run_batch_and_cache  # S3 fetch + 배치 판단


# ─────────────────────────────────────────────────────────────
# 공통 유틸
# ─────────────────────────────────────────────────────────────
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _to_json_str(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False)

async def _ainvoke_with_retry(chain, user_json: str, max_retries: int = 2):
    last_err = None
    for _ in range(max_retries + 1):
        try:
            return await chain.ainvoke({"user_json": user_json})
        except Exception as e:
            last_err = e
    if last_err:
        raise last_err


# ─────────────────────────────────────────────────────────────
# JSON 파서
# ─────────────────────────────────────────────────────────────
def _extract_json(s: str) -> Optional[str]:
    if not s: return None
    # </thinking> 이후부터
    pos = s.rfind("</thinking>")
    if pos != -1:
        s = s[pos + len("</thinking>"):]
    text = s.strip()
    if not text: return None
    # 코드펜스
    for m in re.finditer(r"```(?:json)?\s*([\s\S]*?)\s*```", text):
        cand = m.group(1).strip()
        try:
            json.loads(cand); return cand
        except Exception:
            pass
    # 중괄호 스캔
    braces = [m.start() for m in re.finditer(r"\{", text)]
    for st in braces:
        for ed in range(len(text), st + 1, -1):
            cand = text[st:ed].strip()
            if not cand.endswith("}"): continue
            try:
                json.loads(cand); return cand
            except Exception:
                continue
    return None

def _json_from_content(content: Any) -> str:
    if isinstance(content, list):
        # output_json 우선
        for b in content:
            if isinstance(b, dict) and b.get("type") == "output_json" and "output_json" in b:
                return json.dumps(b["output_json"], ensure_ascii=False)
    # 텍스트 합치기
    if isinstance(content, list):
        text = "".join((b.get("text","") or "") for b in content if isinstance(b, dict) and b.get("type")=="text")
    else:
        text = str(content or "")
    cand = _extract_json(text)
    if cand: return cand
    
    # ===> 이 부분을 추가하세요! <===
    print("="*50)
    print("DEBUG: No JSON found in raw content from LLM. Raw text was:")
    print(text)
    print("="*50)
    # ===> 여기까지 <===
    
    raise ValueError("No valid JSON found in content")


# ─────────────────────────────────────────────────────────────
# 캐시 키 유틸
# ─────────────────────────────────────────────────────────────
def _safe_get(payload, key: str) -> str:
    try:
        v = getattr(payload, key)
    except Exception:
        v = payload.get(key) if isinstance(payload, dict) else None
    return "" if v is None else str(v)

def _norm(text: str) -> str:
    s = (text or "").strip().lower()
    return unicodedata.normalize("NFKC", s)

def _make_signature(payload) -> str:
    parts = [
        _norm(_safe_get(payload, "name")),
        _norm(_safe_get(payload, "description")),
        _norm(_safe_get(payload, "category")),
    ]
    base = "|".join([p for p in parts if p]) or "unknown"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()[:16]

def _verdict_key(category: str, signature: str) -> str:
    return f"verdict:{category}:{signature}"

def _verdict_get(category: str, signature: str) -> Optional[DecisionOutput]:
    try:
        raw = get_cached(_verdict_key(category, signature))
        if not raw:
            return None
        if isinstance(raw, dict):
            return DecisionOutput.model_validate(raw)
        if isinstance(raw, str):
            return DecisionOutput.model_validate_json(raw)
        return DecisionOutput.model_validate_json(json.dumps(raw, ensure_ascii=False))
    except Exception:
        return None

def _verdict_set(category: str, signature: str, out: DecisionOutput) -> None:
    try:
        set_cached(_verdict_key(category, signature), out.model_dump())
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────
# LLM 체인
# ─────────────────────────────────────────────────────────────
_probe_prompt = ChatPromptTemplate.from_messages([("system", SYSTEM_PROMPT_PROBE), ("human", "{user_json}")])
_final_prompt = ChatPromptTemplate.from_messages([("system", SYSTEM_PROMPT_FINAL), ("human", "{user_json}")])

_llm_probe = chat_claude.bind_tools(TOOLS, max_tokens=600)
_llm_final = chat_claude.bind_tools(TOOLS, max_tokens=1500)

_chain_probe = _probe_prompt | _llm_probe
_chain_final = _final_prompt | _llm_final


# ─────────────────────────────────────────────────────────────
# 메인 파이프라인: Quick Pass → 필요시 배치 → 재판단
# ─────────────────────────────────────────────────────────────
async def judge_once(payload: AgentInput) -> DecisionOutput:
    inp = AgentInput(**payload.model_dump())
    if inp.condition or inp.bought_at:
        new_description = []
        if inp.condition: new_description.append(f"상태: {inp.condition}")
        if inp.bought_at: new_description.append(f"구입 시기: {inp.bought_at}")

        if inp.description: # 기존 description이 있다면 합쳐주기
            inp.description = inp.description + "\n" + "\n".join(new_description)
        else:
            inp.description = "\n".join(new_description)
            
    sig = _make_signature(payload)

    # 0) 과거 최종판단 캐시가 있으면 즉시 반환
    cached = _verdict_get(payload.category or "기타", sig)
    if cached:
        return cached

    # 1) Probe: 카테고리/정보충분도만
    raw_probe = await _ainvoke_with_retry(_chain_probe, _to_json_str(inp.model_dump()))
    probe = ProbeOutput.model_validate_json(_json_from_content(getattr(raw_probe, "content", None)))
    category = (probe.category or payload.category or "기타").strip()
    info_need = probe.info_need

    # 2) Quick Pass: 증거 없이 자기신뢰 평가
    enriched = inp.model_dump()
    enriched.update({
        "category": category,
        "evidence_summary": None,
        "evidence_source": "none",
    })
    raw_final_q = await _ainvoke_with_retry(_chain_final, _to_json_str(enriched))
    out_q = DecisionOutput.model_validate_json(_json_from_content(getattr(raw_final_q, "content", None)))

    CONF_TH = 0.7
    quick_ok = (out_q.confidence or 0.0) >= CONF_TH and out_q.decision != "uncertain"

    #  Quick Pass 근거 구성 (배치 없을 때 쓸 기본 근거)
    quick_evidence = {
        "source": "self-quick-pass",
        "reasoning": out_q.reasoning,
    }

    if quick_ok and info_need in ("none", "low"):
        # Quick Pass로 충분하면 그대로 종료, 근거는 self-quick-pass로 남김
        out_q.category = category
        out_q.evidence_summary = quick_evidence
        out_q.evidence_source = "self-quick-pass"
        out_q.timestamp = out_q.timestamp or _now_iso()
        _verdict_set(category, sig, out_q)
        return out_q

    # 3) Quick Pass 불충분 → 배치 근거 수집(Redis miss면 S3→LLM)
    analysis = await run_batch_and_cache(name=inp.name, category=category, signature=sig)

    # evidence 선택: 배치가 있으면 배치, 없으면 Quick Pass 근거
    evidence_summary = analysis if analysis else quick_evidence
    evidence_source  = "batch-llm" if analysis else "self-quick-pass"

    enriched2 = inp.model_dump()
    enriched2.update({
        "category": category,
        "evidence_summary": evidence_summary,
        "evidence_source": evidence_source,
    })
    raw_final = await _ainvoke_with_retry(_chain_final, _to_json_str(enriched2))
    out = DecisionOutput.model_validate_json(_json_from_content(getattr(raw_final, "content", None)))

    # 방어적 정리(혹시 모델이 넣으면 제거)
    if isinstance(out.price, dict):
        out.price.pop("currency", None)
        out.price.pop("unit", None)

    out.category = category
    out.timestamp = out.timestamp or _now_iso()
    _verdict_set(category, sig, out)
    return out