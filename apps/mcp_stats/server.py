from fastmcp import FastMCP
import statistics as stats
from typing import List, Dict, Any

mcp = FastMCP("Stats Engine")

@mcp.tool
def stats_iqr(values: List[float]) -> Dict[str, Any]:
    """IQR로 이상치 제거 + 중앙값"""
    if not values:
        return {"q1": None, "q3": None, "iqr": None, "filtered": [], "median": None}
    q = stats.quantiles(values, n=4)  # [Q1, Q2, Q3]
    q1, q3 = q[0], q[2]
    iqr = q3 - q1
    lo, hi = q1 - 1.5*iqr, q3 + 1.5*iqr
    filtered = [x for x in values if lo <= x <= hi]
    median = stats.median(filtered) if filtered else None
    return {"q1": q1, "q3": q3, "iqr": iqr, "filtered": filtered, "median": median}

def main():
    # 127.0.0.1:8787/mcp 로 서비스 노출
    mcp.run(transport="http",host="127.0.0.1", port=8787)
if __name__ == "__main__":
    main()