# server.py — HTTP JSON-RPC 단일 모드 (Smithery 호환)
from __future__ import annotations

from typing import Iterable, List, Dict, Any, Optional
import json, gzip, re

import boto3
from botocore.config import Config
from fastmcp import FastMCP
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.requests import Request
from starlette.routing import Route
from starlette.middleware.cors import CORSMiddleware
import uvicorn

# --- project-local utils ---
from cohobby_mcp.servers.datalookup.utils.classifier import classify_listing
from cohobby_mcp.servers.datalookup.utils.extractor import extract_product_and_category
from cohobby_mcp.servers.datalookup.utils.normalizer import (
    parse_price_krw, days_from_duration, extract_rental_price
)
from cohobby_mcp.servers.datalookup.utils.pricing_utils import iqr_filter, summarize
from cohobby_mcp.servers.datalookup.utils.s3_reader import read_jsonl_s3  # ★ s3_reader로 변경
# ----------------- MCP core -----------------
PROTOCOL_VERSION = "2025-06-18"
SERVER_NAME = "data-lookup"
SERVER_TITLE = "Data Lookup MCP Server"
SERVER_VERSION = "1.0.0"

mcp = FastMCP(SERVER_NAME)

# Tools (실제 로직)
s3 = boto3.client("s3", config=Config(signature_version="s3v4"))
_GZIP_MAGIC = b"\x1f\x8b"

def _is_gzip(data: bytes) -> bool:
    return len(data) >= 2 and data[:2] == _GZIP_MAGIC

def _decode_obj_body(obj: Dict[str, Any]) -> str:
    body: bytes = obj["Body"].read()
    if _is_gzip(body):
        try:
            body = gzip.decompress(body)
        except OSError:
            pass
    return body.decode("utf-8", errors="replace")

def _normalize_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    rid = str(raw.get("post_id") or raw.get("id") or raw.get("post_link") or raw.get("url") or "")
    title = (raw.get("product_name") or raw.get("title") or "").strip()
    url = (raw.get("post_link") or raw.get("url") or "").strip()
    body: Optional[str] = (raw.get("body") or "").strip() or None
    location = raw.get("location_name") or raw.get("location") or None
    location_id = raw.get("location_id")
    category_seed = raw.get("category") or None

    listing_type, signals = classify_listing(title, body)
    model_hint, category_hint = extract_product_and_category(title, body)
    if not category_hint and category_seed:
        category_hint = category_seed

    price_total: Optional[int] = None
    if isinstance(raw.get("rental_price_total_krw"), (int, float)) and raw["rental_price_total_krw"] > 0:
        price_total = int(round(float(raw["rental_price_total_krw"])))
    else:
        price_total = parse_price_krw(raw.get("rental_price_raw"))

    if price_total is None and listing_type == "rental":
        extracted = extract_rental_price(title, body)
        if isinstance(extracted, int) and extracted > 0:
            price_total = extracted

    price_per_day: Optional[int] = None
    if price_total and price_total > 0:
        days = days_from_duration(raw.get("rental_duration_raw"))
        price_per_day = int(round(price_total / max(1.0, days)))

    deposit_krw: Optional[int] = None
    if isinstance(raw.get("deposit_krw"), (int, float)) and raw["deposit_krw"] > 0:
        deposit_krw = int(round(float(raw["deposit_krw"])))

    out: Dict[str, Any] = {
        "id": rid,
        "url": url,
        "title": title,
        "listing_type": listing_type,
        "model_hint": model_hint,
        "category_hint": category_hint,
        "signals": signals,
        "location": location,
    }
    if location_id is not None:
        out["location_id"] = str(location_id)
    if price_total:
        out["rental_price"] = price_total
    if price_per_day:
        out["rental_price_per_day"] = price_per_day
    if deposit_krw is not None:
        out["deposit_krw"] = deposit_krw
    if raw.get("scraped_at"):
        out["scraped_at"] = raw["scraped_at"]
    return out

def _iter_normalized(items: Iterable[Dict[str, Any]]):
    for raw in items:
        try:
            yield _normalize_record(raw)
        except Exception as e:
            yield {"error": str(e), "raw_id": raw.get("id") or raw.get("post_id") or raw.get("url")}

def fetch_and_normalize_from_s3(bucket: str, key: str, limit: int = 500) -> List[Dict[str, Any]]:
     print(f"[server] fetch_and_normalize_from_s3 -> delegating to s3_reader (b={bucket}, k={key}, limit={limit})", flush=True)
     rows = read_jsonl_s3(bucket, key, limit=limit)  # ★ 이제 s3_reader가 반드시 호출됨
     out: List[Dict[str, Any]] = []
     for raw in rows:
         if not isinstance(raw, dict):
             continue
         try:
             out.append(_normalize_record(raw))
         except Exception as e:
             out.append({"error": str(e)})
     print(f"[server] normalized rows: {len(out)}", flush=True)
     return out

def fetch_core_from_s3(bucket: str, key: str, limit: int = 500) -> List[Dict[str, Any]]:
    records = fetch_and_normalize_from_s3(bucket=bucket, key=key, limit=limit)
    core: List[Dict[str, Any]] = []
    for r in records:
        if not r or "error" in r:
            continue
        item = {
            "id": r.get("id"),
            "url": r.get("url"),
            "title": r.get("title"),
            "listing_type": r.get("listing_type"),
            "model_hint": r.get("model_hint"),
            "category_hint": r.get("category_hint"),
            "location": r.get("location"),
            "location_id": r.get("location_id"),
        }
        if r.get("rental_price") is not None:
            item["rental_price"] = r["rental_price"]
        if r.get("rental_price_per_day") is not None:
            item["rental_price_per_day"] = r["rental_price_per_day"]
        if r.get("deposit_krw") is not None:
            item["deposit_krw"] = r["deposit_krw"]
        core.append(item)
    return core

def summarize_rental_prices(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    prices = [float(r["rental_price"]) for r in records if r and r.get("rental_price") is not None]
    if not prices:
        return {"count": 0, "message": "no prices"}
    kept, meta = iqr_filter(prices)
    summary = summarize(kept)
    summary["iqr_bounds"] = meta
    summary["count_after_iqr"] = len(kept)
    summary["count_before_iqr"] = len(prices)
    return summary

fetch_and_normalize_from_s3_tool = mcp.tool()(fetch_and_normalize_from_s3)
fetch_core_from_s3_tool            = mcp.tool()(fetch_core_from_s3)
summarize_rental_prices_tool       = mcp.tool()(summarize_rental_prices)

# --------- HTTP JSON-RPC plumbing (수동 구현) ---------

# well-known (Smithery가 참고)
async def well_known(req: Request):
    return JSONResponse({
        "name": SERVER_NAME,
        "title": SERVER_TITLE,
        "version": SERVER_VERSION,
        "protocolVersion": PROTOCOL_VERSION,
        "transport": "http",
        "endpoint": "/",
        "tools": [
            {"name": "fetch_core_from_s3"},
            {"name": "fetch_and_normalize_from_s3"},
            {"name": "summarize_rental_prices"},
        ],
    })

def _tools_schema() -> Dict[str, Any]:
    # 최소한의 입력 스키마 제공 (Smithery가 읽기 충분)
    return {
        "tools": [
            {
                "name": "fetch_core_from_s3",
                "description": "Read normalized core fields from S3 (bucket+key).",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "bucket": {"type": "string"},
                        "key": {"type": "string"},
                        "limit": {"type": "integer"},
                    },
                    "required": ["bucket", "key"]
                },
            },
            {
                "name": "fetch_and_normalize_from_s3",
                "description": "Read and normalize rental listings from S3 (bucket+key).",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "bucket": {"type": "string"},
                        "key": {"type": "string"},
                        "limit": {"type": "integer"},
                    },
                    "required": ["bucket", "key"]
                },
            },
            {
                "name": "summarize_rental_prices",
                "description": "IQR-filtered summary statistics of rental_price.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "records": {"type": "array", "items": {"type": "object"}}
                    },
                    "required": ["records"]
                },
            },
        ]
    }

async def rpc_root(req: Request):
    # ── 로깅: 요청 메타/바디 찍기 ──────────────────────────────────────────
    raw = await req.body()
    try:
        raw_text = raw.decode("utf-8", errors="replace")
    except Exception:
        raw_text = str(raw)
    print("====== 📥 INCOMING REQUEST ======")
    print(f"➡️ {req.method} {req.url.path}")
    try:
        client = getattr(req, "client", None)
        print(f"👤 client={getattr(client, 'host', '?')}:{getattr(client, 'port', '?')}")
    except Exception:
        pass
    print("🧾 headers=", {k: v for k, v in req.headers.items()})
    print("📦 raw_body=", raw_text[:2000])  # 과하면 잘라서
    print("=================================")

    # JSON 직접 파싱(실패하면 400 + RAW 바디 일부 포함)
    try:
        body = json.loads(raw_text) if raw_text else {}
    except Exception as e:
        print(f"❌ [Parse error] {e}")
        return JSONResponse(
            {"jsonrpc": "2.0", "id": "server-error",
             "error": {"code": -32700, "message": f"Parse error: {e}", "data": raw_text[:500]}},
            status_code=400
        )

    method = (body or {}).get("method")
    req_id = (body or {}).get("id")
    params = (body or {}).get("params") or {}
    
    print(f"🔧 method={method}, id={req_id}, params_keys={list(params.keys())}")

    # initialize
    if method == "initialize":
        result = {
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {
                "logging": {},
                "prompts": {"listChanged": True},
                "resources": {"subscribe": True, "listChanged": True},
                "tools": {"listChanged": True},
            },
            "serverInfo": {
                "name": SERVER_NAME,
                "title": SERVER_TITLE,
                "version": SERVER_VERSION,
            },
            "instructions": "Use tools via JSON-RPC methods: tools/list, tools/call.",
        }
        return JSONResponse({"jsonrpc": "2.0", "id": req_id, "result": result})

    # notifications/initialized (ACK)
    if method == "notifications/initialized":
        return JSONResponse({"jsonrpc": "2.0", "id": req_id, "result": {}})

    # tools/list
    if method == "tools/list":
        return JSONResponse({"jsonrpc": "2.0", "id": req_id, "result": _tools_schema()})

    # tools/call
    if method == "tools/call":
        name = (params or {}).get("name")
        args = (params or {}).get("arguments") or {}
        try:
            if name == "fetch_core_from_s3":
                res = fetch_core_from_s3(**args)
                return JSONResponse({"jsonrpc": "2.0", "id": req_id, "result": {"content": [{"type": "json", "value": res}]}})
            elif name == "fetch_and_normalize_from_s3":
                res = fetch_and_normalize_from_s3(**args)
                return JSONResponse({"jsonrpc": "2.0", "id": req_id, "result": {"content": [{"type": "json", "value": res}]}})
            elif name == "summarize_rental_prices":
                res = summarize_rental_prices(**args)
                return JSONResponse({"jsonrpc": "2.0", "id": req_id, "result": {"content": [{"type": "json", "value": res}]}})
            else:
                print(f"⚠️ Unknown tool: {name}")
                return JSONResponse({"jsonrpc": "2.0", "id": req_id,
                                     "error": {"code": -32601, "message": f"Unknown tool: {name}"}},
                                    status_code=404)
        except TypeError as e:
            return JSONResponse({"jsonrpc": "2.0", "id": req_id,
                                 "error": {"code": -32602, "message": f"Invalid params: {e}"}},
                                status_code=400)
        except Exception as e:
            return JSONResponse({"jsonrpc": "2.0", "id": req_id,
                                 "error": {"code": -32000, "message": f"Tool error: {e}"}},
                                status_code=500)
    print(f"⛔ Method not found: {method}")
    # 미지원 메서드
    return JSONResponse({"jsonrpc": "2.0", "id": req_id,
                         "error": {"code": -32601, "message": f"Method not found: {method}"}},
                        status_code=404)

# Starlette 앱 구성 (CORS + 라우팅)
routes = [
    Route("/.well-known/mcp-config", well_known, methods=["GET"]),
    Route("/", rpc_root, methods=["POST"]),
]
app = Starlette(routes=routes)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

def main():
    uvicorn.run(app, host="0.0.0.0", port=8765)

if __name__ == "__main__":
    main()
