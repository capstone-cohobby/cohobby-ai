from __future__ import annotations

import numpy as np


def iqr_filter(values: list[float], k: float = 1.5) -> list[float]:
    if not values:
        return values
    q1, q3 = np.percentile(values, [25, 75])
    iqr = q3 - q1
    lo, hi = q1 - k * iqr, q3 + k * iqr
    return [v for v in values if lo <= v <= hi]

def summarize(values: list[float]) -> dict[str, float]:
    if not values:
        return {
            "n": 0, "median": float("nan"), "iqr": float("nan"),
            "p10": float("nan"), "p50": float("nan"), "p90": float("nan"),
        }
    arr = np.array(values)
    p10, p50, p90 = np.percentile(arr, [10, 50, 90])
    q1, q3 = np.percentile(arr, [25, 75])
    return {
        "n": int(arr.size),
        "median": float(p50),
        "iqr": float(q3 - q1),
        "p10": float(p10),
        "p50": float(p50),
        "p90": float(p90),
    }

def quantile_position(x: float, p10: float, p50: float, p90: float) -> tuple[float, str]:
    if p10 == p90:
        q = 0.5
    else:
        q = (x - p10) / (p90 - p10)
        q = max(0.0, min(1.0, q))
    label = "low" if q < 0.33 else ("mid" if q <= 0.66 else "high")
    return q, label
