# s3_reader.py

import json, gzip
from json import JSONDecodeError
from typing import List, Dict, Any, Optional
import boto3
from botocore.config import Config

_s3 = boto3.client("s3", config=Config(signature_version="s3v4"))

def _maybe_decompress(b: bytes) -> bytes:
    return gzip.decompress(b) if b[:2] == b"\x1f\x8b" else b

def _load_json_or_jsonl(text: str) -> List[Dict[str, Any]]:
    """
    JSON 배열, 단일 JSON, JSON Lines, 연속 JSON({}{}) 모두 안전 처리.
    실패 시 dict 하나짜리 에러 배열을 반환.
    """
    s = text.lstrip("\ufeff \t\r\n")  # BOM/공백 제거
    if not s:
        return []

    # 1) 정상 JSON 먼저 시도: [ ... ] 또는 { ... }
    if s[:1] in ("[", "{"):
        try:
            obj = json.loads(s)
            if isinstance(obj, list):
                return [row for row in obj if isinstance(row, dict)]
            if isinstance(obj, dict):
                return [obj]
        except JSONDecodeError:
            # 정상 JSON이 아니면 아래 단계로 폴백
            pass

    # 2) JSONL (줄 단위) 시도
    out: List[Dict[str, Any]] = []
    for raw_line in s.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        # 흔한 케이스 방어: 라인 끝의 쉼표 제거
        if line.endswith(",") and not line.endswith(r"\,"):
            line = line[:-1].rstrip()
        try:
            o = json.loads(line)
            if isinstance(o, dict):
                out.append(o)
        except JSONDecodeError:
            # 라인 파싱 실패 → 나중에 연속 JSON 파싱으로 재도전
            pass
    if out:
        return out

    # 3) (특수) 줄바꿈 없이 JSON이 연속된 경우: raw_decode로 스트리밍 파싱
    dec = json.JSONDecoder()
    i = 0
    n = len(s)
    out2: List[Dict[str, Any]] = []
    while i < n:
        while i < n and s[i] in " \t\r\n":
            i += 1
        if i >= n:
            break
        try:
            obj, j = dec.raw_decode(s, idx=i)
        except JSONDecodeError:
            break
        if isinstance(obj, dict):
            out2.append(obj)
        i = j
    if out2:
        return out2

    # 4) 그래도 안 되면, 디버깅에 도움 되는 에러 메시지 리턴
    try:
        json.loads(s)
    except JSONDecodeError as e:
        return [{"error": f"JSON parse failed: {e}"}]
    return [{"error": "JSON parse failed: unknown format"}]

def read_jsonl_s3(bucket: str, key: str, *, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    obj = _s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()
    text = _maybe_decompress(body).decode("utf-8", errors="replace")

    # ↓↓↓ 디버깅 로그(필요 시 주석 해제)
    print("S3 object preview:", repr(text[:300]))

    rows = _load_json_or_jsonl(text)
    if limit is not None:
        rows = rows[: max(0, int(limit))]
    return rows
