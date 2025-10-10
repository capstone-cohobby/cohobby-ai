import os, requests, json
from src.cache.redis_client import set_cached
from src.mcp.servers.datalookup.server import read_jsonl_filtered

API_KEY = os.getenv("ANTHROPIC_API_KEY")

def run_batch_judgment(category: str):
    print(f"⚙️ Batch: {category} 품목 평가 중...")

    data = read_jsonl_filtered(
        bucket="cohobby-crawler",
        key="crawler-results/2025-10-08/daangn_1759912633.json",
        category=category,
        limit=50
    )

    requests_list = []
    for i, item in enumerate(data):
        prompt = f"""
        품목: {item.get("title")}
        가격: {item.get("price")}
        위치: {item.get("location")}
        날짜: {item.get("created_at")}

        이 게시글의 대여가가 시장 평균 대비 합리적인지,
        0~1 신뢰도 점수와 간단한 이유를 출력해줘.
        """
        requests_list.append({
            "custom_id": f"{category}_{i}",
            "params": {
                "model": "claude-3-5-sonnet-20240620",
                "max_tokens": 300,
                "messages": [{"role": "user", "content": prompt}]
            }
        })

    res = requests.post(
        "https://api.anthropic.com/v1/messages/batches",
        headers={
            "x-api-key": API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={"requests": requests_list}
    )
    batch = res.json()

    result_summary = {
        "category": category,
        "avg_rent_price": None,  # 나중에 계산 가능
        "batch_id": batch["id"],
        "confidence": 0.8,
        "updated_at": "auto",
    }

    set_cached(category, result_summary)
    print(f"✅ Redis 저장 완료: {category}")
    return result_summary
