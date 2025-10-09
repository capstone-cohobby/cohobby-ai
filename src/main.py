from __future__ import annotations

import argparse
import asyncio
import json

from src.hosts.agent.chains.judge import judge_once 
from src.hosts.agent.schemas import AgentInput, CacheContext


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--category", required=True)
    p.add_argument("--product", required=True)
    p.add_argument("--region", required=True)
    p.add_argument("--cache-median", type=float, default=0.018)
    p.add_argument("--cache-n", type=int, default=120)
    p.add_argument("--cache-iqr", type=float, default=0.004)
    p.add_argument("--cache-updated-at", default="2025-09-20")
    p.add_argument("--p10", type=float, default=0.012)
    p.add_argument("--p50", type=float, default=0.018)
    p.add_argument("--p90", type=float, default=0.028)
    return p.parse_args()

async def main():
    a = parse_args()
    cache = CacheContext(
        median_ratio=a.cache_median,
        n=a.cache_n,
        iqr=a.cache_iqr,
        updated_at=a.cache_updated_at,
        p10=a.p10,
        p50=a.p50,
        p90=a.p90,
    )
    payload = AgentInput(
        category=a.category,
        product=a.product,
        region=a.region,
        cache=cache,
    )
    result = await judge_once(payload)
    print(json.dumps(result.model_dump(), ensure_ascii=False, indent=2))

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
