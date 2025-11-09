# tools/cache_tools.py
import hashlib, json, unicodedata
from typing import Optional, Any, Dict
from ..schemas import PriceDecision # Pydantic 모델 의존

# (가정) Redis 클라이언트 설정
# from redis import Redis
# redis_client = Redis(decode_responses=True)
print("[Cache] Mock Redis Client Initialized.")

def _norm(text: str) -> str:
    s = (text or "").strip().lower()
    return unicodedata.normalize("NFKC", s)

def make_signature(payload: Dict[str, Any]) -> str:
    """입력 페이로드 기반으로 고유 서명 생성"""
    parts = [
        _norm(payload.get("name")),
        _norm(payload.get("description")),
        _norm(payload.get("category")),
    ]
    base = "|".join([p for p in parts if p]) or "unknown"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()[:16]

def _verdict_key(signature: str) -> str:
    return f"verdict:price:{signature}"

def get_verdict(signature: str) -> Optional[PriceDecision]:
    """Redis에서 최종 가격 판단 캐시 조회"""
    key = _verdict_key(signature)
    try:
        # raw = redis_client.get(key)
        raw = None # (Mock) 캐시 없음
        if not raw:
            return None
        return PriceDecision.model_validate_json(raw)
    except Exception as e:
        print(f"[Cache] GET Error: {e}")
        return None

def set_verdict(signature: str, decision: PriceDecision) -> None:
    """Redis에 최종 가격 판단 캐시 저장"""
    key = _verdict_key(signature)
    try:
        # redis_client.set(key, decision.model_dump_json(), ex=3600 * 24)
        print(f"[Cache] SET OK: {key}")
    except Exception as e:
        print(f"[Cache] SET Error: {e}")