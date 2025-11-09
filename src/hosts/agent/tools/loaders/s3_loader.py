from __future__ import annotations
import os, json, gzip
from typing import Iterable, Dict, Any, Optional
import boto3

def iter_s3_records(bucket: str, prefix: Optional[str]=None, key: Optional[str]=None, region: Optional[str]=None) -> Iterable[Dict[str, Any]]:
    s3 = boto3.client("s3", region_name=region or os.getenv("AWS_REGION","ap-northeast-2"))
    if key:
        yield from _read_single_key(s3, bucket, key)
        return

    if not prefix:
        raise RuntimeError("prefix 또는 key 중 하나는 필요합니다.")

    cont = None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix}
        if cont:
            kw["ContinuationToken"] = cont
        resp = s3.list_objects_v2(**kw)
        for obj in resp.get("Contents", []):
            k = obj["Key"]
            if not (k.endswith(".json") or k.endswith(".jsonl") or k.endswith(".jsonl.gz") or k.endswith(".json.gz")):
                continue
            yield from _read_single_key(s3, bucket, k)
        if resp.get("IsTruncated"):
            cont = resp.get("NextContinuationToken")
        else:
            break

def _read_single_key(s3, bucket: str, key: str) -> Iterable[Dict[str, Any]]:
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()
    if key.endswith(".gz"):
        body = gzip.decompress(body)
    text = body.decode("utf-8", errors="ignore")
    if key.endswith(".json") or text.strip().startswith("["):
        for r in json.loads(text):
            yield r
    else:
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue
