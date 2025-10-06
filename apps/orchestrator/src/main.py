import asyncio
import json
from fastmcp import Client

def extract_payload(result):
    """
    MCP 응답을 안전하게 파싱:
    - JSONContent: item.data
    - TextContent: json.loads(item.text) 시도, 실패하면 그냥 text
    """
    if not result.content:
        return None
    item = result.content[0]
    # JSONContent인 경우
    if hasattr(item, "data"):
        return item.data
    # TextContent인 경우
    if hasattr(item, "text"):
        try:
            return json.loads(item.text)
        except Exception:
            return item.text
    # 그 외 예외 케이스
    return getattr(item, "json", None) or str(item)

async def main():
    async with Client("http://127.0.0.1:8787/mcp") as stats, Client("http://127.0.0.1:8765/mcp") as pricing:
        res1 = await stats.call_tool("stats_iqr", {"values": [10000,20000,30000]})
        res2 = await pricing.call_tool("pricing_engine", {"day_price": 30000, "duration_days":3})
        print("📊 통계:", res1.content[0].text)
        print("💰 정산:", res2.content[0].text)

def run():
    asyncio.run(main())
if __name__ == "__main__":
    run()
