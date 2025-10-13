"""가격 데이터 통계 유틸"""
def _percentile(sorted_vals: list[float], q: float) -> float:
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * q
    f = int(k)
    c = min(f + 1, len(sorted_vals) - 1)
    if f == c:
        return sorted_vals[f]
    d0 = sorted_vals[f] * (c - k)
    d1 = sorted_vals[c] * (k - f)
    return d0 + d1


def iqr_filter(prices: list[float]) -> tuple[list[float], dict]:
    """IQR로 이상치 제거하고 (남은값 리스트, 경계정보) 반환"""
    vals = sorted([float(v) for v in prices])
    if not vals:
        return [], {"q1": 0.0, "q3": 0.0, "iqr": 0.0, "lb": 0.0, "ub": 0.0}
    q1 = _percentile(vals, 0.25)
    q3 = _percentile(vals, 0.75)
    iqr = q3 - q1
    lb, ub = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    kept = [v for v in vals if lb <= v <= ub]
    return kept, {"q1": q1, "q3": q3, "iqr": iqr, "lb": lb, "ub": ub}


def summarize(prices: list[float]) -> dict:
    """간단 통계 요약"""
    vals = sorted([float(v) for v in prices])
    if not vals:
        return {"count": 0, "median": 0.0, "mean": 0.0, "min": 0.0, "max": 0.0}
    n = len(vals)
    median = vals[n // 2] if n % 2 else (vals[n // 2 - 1] + vals[n // 2]) / 2
    mean = sum(vals) / n
    return {"count": n, "median": median, "mean": mean, "min": vals[0], "max": vals[-1]}


__all__ = ["iqr_filter", "summarize"]
