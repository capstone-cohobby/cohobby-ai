from __future__ import annotations
import json, re, hashlib, unicodedata
from datetime import datetime, timezone
from typing import Any, Optional

from langchain_core.prompts import ChatPromptTemplate
from langchain_core.messages import AIMessage

from hosts.agent.llm import chat_claude
from hosts.agent.llm import make_agent
agent = make_agent()
from hosts.agent.prompts.prompt import SYSTEM_PROMPT
from hosts.agent.schemas import AgentInput, DecisionOutput,PriceEstimate
from hosts.agent.tools.mcp import TOOLS

from cache.redis_client import get_cached, set_cached
from hosts.agent.chains.batch_executor import run_batch_judgment
from .classifier import classify_category


prompt = ChatPromptTemplate.from_messages([
    ("system", SYSTEM_PROMPT),
    ("human", "{user_json}")
])


# 1차는 짧게 (빠른 판단, 적은 증거)
llm_first = chat_claude.bind_tools(TOOLS, max_tokens=700)

# 2차는 넉넉히 (evidence 다 포함)
llm_second = chat_claude.bind_tools(TOOLS, max_tokens=1500)

# 체인 분리
chain_first = prompt | llm_first
chain_second = prompt | llm_second

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

def _json_from_text_blocks_only(content: Any) -> str:
    """
    Anthropic Messages API의 content(list of blocks)에서 JSON만 추출.
    - 우선 output_json 블록
    - 다음 코드펜스 ```json
    - 다음 텍스트에서 { ... } 가장 그럴듯한 블록
    실패 시 ValueError
    """
    # 1) output_json
    if isinstance(content, list):
        for b in content:
            if isinstance(b, dict) and b.get("type") == "output_json" and "output_json" in b:
                return json.dumps(b["output_json"], ensure_ascii=False)

    # 2) 코드펜스 / 3) 텍스트
    if isinstance(content, list):
        text = "".join((b.get("text", "") or "") for b in content if isinstance(b, dict) and b.get("type") == "text")
    else:
        text = str(content or "")

    cand = _extract_json(text)
    if cand:
        return cand
    raise ValueError("No valid JSON found in text blocks")

# ─────────────────────────────────────────────────────────────
# Verdict 캐시 유틸 (안전 버전)
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
        _norm(_safe_get(payload, "condition")),
        _norm(_safe_get(payload, "bought_at")),
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

def _is_fresh(ts_iso: Optional[str], max_days: int = 30) -> bool:
    if not ts_iso:
        return False
    try:
        ts = datetime.fromisoformat(ts_iso)
        now = datetime.now(ts.tzinfo or timezone.utc)
        return (now - ts).days <= max_days
    except Exception:
        return False

def _days_old(ts_iso: Optional[str]) -> int:
    if not ts_iso:
        return 10**6
    try:
        ts = datetime.fromisoformat(ts_iso)
        now = datetime.now(ts.tzinfo or timezone.utc)
        return max(0, (now - ts).days)
    except Exception:
        return 10**6

def _apply_time_decay(conf: float, days_old: int, half_life_days: int = 30) -> float:
    import math
    decay = 0.5 ** (max(days_old, 0) / max(half_life_days, 1))
    return max(0.5, min(1.0, 0.5 + (conf - 0.5) * decay))

# ─────────────────────────────────────────────────────────────
# JSON 추출 폴백
# ─────────────────────────────────────────────────────────────
def _extract_json(s: str) -> Optional[str]:
    """
    </thinking> 이후 JSON을 최대한 복원:
      1) 코드펜스 ```json ... ```
      2) 중괄호 블록 스캔
    실패 시 None
    """
    if not s:
        return None

    closing = "</thinking>"
    pos = s.rfind(closing)
    if pos != -1:
        s = s[pos + len(closing):]
    text = s.strip()
    if not text:
        return None

    # 코드펜스 우선
    for m in re.finditer(r"```(?:json)?\s*([\s\S]*?)\s*```", text):
        cand = m.group(1).strip()
        try:
            json.loads(cand)
            return cand
        except Exception:
            pass

    # 중괄호 블록 스캔
    brace_pos = [m.start() for m in re.finditer(r"\{", text)]
    for st in brace_pos:
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

# ─────────────────────────────────────────────────────────────
# 가격 자동 채움 (evidence에서)
# ─────────────────────────────────────────────────────────────
def _fill_price_from_evidence(out: DecisionOutput) -> DecisionOutput:
    try:
        if getattr(out, "price", None) and (out.price and (out.price.point or out.price.low or out.price.high)):
            return out

        stats = out.stats_summary or {}
        ev = out.evidence_summary or {}

        def pick(keys, src):
            for k in keys:
                v = src.get(k) if isinstance(src, dict) else None
                if isinstance(v, (int, float)):
                    return float(v)
            return None

        median = pick(["median", "p50", "q2"], stats) or pick(["median", "p50", "q2"], ev)
        mean   = pick(["mean", "avg"], stats)        or pick(["mean", "avg"], ev)
        q1     = pick(["q1", "p25"], stats)          or pick(["q1", "p25"], ev)
        q3     = pick(["q3", "p75"], stats)          or pick(["q3", "p75"], ev)

        pe = PriceEstimate(currency="KRW", unit="per_day")
        if median is not None:
            pe.point = median
            pe.basis = (pe.basis or "median")
        elif mean is not None:
            pe.point = mean
            pe.basis = (pe.basis or "mean")

        if q1 is not None and q3 is not None:
            pe.low, pe.high = q1, q3
            pe.basis = "IQR (q1~q3)" if pe.basis is None else f"{pe.basis}+IQR"

        if pe.point or pe.low or pe.high:
            out.price = pe
    except Exception:
        pass
    return out

# ─────────────────────────────────────────────────────────────
# 카테고리 보완 (프로젝트의 기존 구현 그대로 사용해도 됨)
# ─────────────────────────────────────────────────────────────
def _maybe_fill_category(first: DecisionOutput, payload: AgentInput) -> str:
    if first and first.category:
        return first.category
    # 필요하면 간단 분류기/규칙 사용. 여기선 payload.category 우선
    return (payload.category or "기타").strip()

# ─────────────────────────────────────────────────────────────
# 메인: 판단 파이프라인
# ─────────────────────────────────────────────────────────────
async def judge_once(payload: AgentInput) -> DecisionOutput:
    # 체인(프롬프트+모델) 초기화는 프로젝트 기존 코드를 사용
    from langchain_core.prompts import ChatPromptTemplate
    from hosts.agent.llm import chat_claude, TOOLS  # 기존 경로 유지
    prompt = ChatPromptTemplate.from_messages([("system", SYSTEM_PROMPT), ("human", "{user_json}")])

    # 1) 1차 호출
    user_json_first = _to_json_str(AgentInput(**payload.model_dump()).model_dump())
    raw_message = await _ainvoke_with_retry(chain_first, user_json_first)

    # 1-1) tool 호출 여부 확인
    if getattr(raw_message, "tool_calls", None):
        # 정보 부족 신호로 간주 — 최소 스켈레톤 생성
        first = DecisionOutput(
            decision="uncertain", confidence=0.5,
            reasoning="도구 사용 필요", info_need="medium",
            category=None, evidence_summary=None, evidence_source=None,
            stats_summary=None, price=None, timestamp=_now_iso()
        )
    else:
        # text/output_json에서 JSON 파싱
        first_json = _json_from_text_blocks_only(getattr(raw_message, "content", None))
        first = DecisionOutput.model_validate_json(first_json)

    # 1-2) 충분하면 즉시 반환 (가격 보강 + verdict 저장)
    if first.info_need in ("low", "none"):
        first.timestamp = first.timestamp or _now_iso()
        first.evidence_source = first.evidence_source or "none"
        first = _fill_price_from_evidence(first)
        try:
            category_for_cache = _maybe_fill_category(first, payload)
            _verdict_set(category_for_cache, _make_signature(payload), first)
        except Exception:
            pass
        return first

    # 2) 카테고리 확정
    category = _maybe_fill_category(first, payload)
    print(f"[judge] 추가 정보 필요: info_need={first.info_need}, category={category}")

    # 2-1) verdict 캐시 1차 조회
    sig = _make_signature(payload)
    cached_verdict = _verdict_get(category, sig)
    if cached_verdict and _is_fresh(cached_verdict.timestamp, max_days=30):
        print(f"[judge] ✅ Verdict cache hit for '{category}' (sig={sig})")
        age_days = _days_old(cached_verdict.timestamp)
        cached_verdict.confidence = _apply_time_decay(cached_verdict.confidence, age_days, half_life_days=30)
        return cached_verdict

    # 3) evidence 캐시 조회 (프로젝트의 기존 캐시 키/구조를 그대로 사용)
    batch_result = get_cached(f"evidence:{category}")
    if not batch_result:
        print(f"[judge] 🚀 Redis miss → Batch execution for '{category}'")
        batch_result = run_batch_judgment(category)  # 동기 함수면 그대로, 비동기면 await
        if batch_result:
            set_cached(f"evidence:{category}", batch_result)

    # 4) 2차 판단 호출 (evidence 주입)
    enriched_payload = AgentInput(**payload.model_dump()).model_dump()
    enriched_payload.update({
        "category": category,
        "evidence_summary": batch_result,
        "evidence_source": "batch",
    })
    raw2 = await _ainvoke_with_retry(chain_second, _to_json_str(enriched_payload))
    content = getattr(raw2, "content", None)

    try:
        second_json = _json_from_text_blocks_only(content)
        second = DecisionOutput.model_validate_json(second_json)
    except Exception:
        # 폴백 1: 텍스트 합치고 </thinking> 이후에서 JSON 추출
        if isinstance(content, list):
            text = "".join((b.get("text", "") or "") for b in content if isinstance(b, dict) and b.get("type") == "text")
        else:
            text = str(content or "")
        cand = _extract_json(text)
        if cand:
            try:
                second = DecisionOutput.model_validate_json(cand)
            except Exception as e:
                preview = text[:500]
                raise RuntimeError(f"Claude JSON parse error (second): {e}\nRaw Content: {preview}") from e
        else:
            # 폴백 2: JSON-only 강제 재요청
            repair_payload = dict(enriched_payload)
            repair_payload["__format__"] = "JSON_ONLY"
            repair_payload["__note__"] = "DO NOT OUTPUT ANY TEXT. RETURN ONLY ONE JSON OBJECT CONFORMING TO THE SCHEMA."
            raw3 = await _ainvoke_with_retry(chain_second, _to_json_str(repair_payload))
            try:
                second_json = _json_from_text_blocks_only(getattr(raw3, "content", None))
                second = DecisionOutput.model_validate_json(second_json)
            except Exception as e:
                c3 = getattr(raw3, "content", None)
                if isinstance(c3, list):
                    t3 = "".join((b.get("text", "") or "") for b in c3 if isinstance(b, dict) and b.get("type") == "text")
                else:
                    t3 = str(c3 or "")
                cand3 = _extract_json(t3)
                if cand3:
                    second = DecisionOutput.model_validate_json(cand3)
                else:
                    preview = (t3 or text)[:500]
                    raise RuntimeError(f"Claude JSON parse error (second, retry): {e}\nRaw Content: {preview}") from e

    # 5) 최종 보정 + 가격 자동 채움 + verdict 캐시 저장
    second.evidence_summary = second.evidence_summary or batch_result
    second.evidence_source = second.evidence_source or "batch"
    second.confidence = min(1.0, (second.confidence + 0.9) / 2)
    second.timestamp = second.timestamp or _now_iso()
    second = _fill_price_from_evidence(second)

    try:
        _verdict_set(category, sig, second)
    except Exception:
        pass

    return second