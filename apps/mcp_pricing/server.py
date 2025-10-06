from fastmcp import FastMCP
from fastmcp import server

mcp = FastMCP("Pricing Engine")

@mcp.tool
def pricing_engine(day_price: float, duration_days: int) -> float:
    """일단가 * 대여 일수 계산"""
    return day_price * duration_days

def main():
    # 127.0.0.1:8787/mcp 로 서비스 노출
    mcp.run(transport="http",host="127.0.0.1", port=8765)
if __name__ == "__main__":
    main()
