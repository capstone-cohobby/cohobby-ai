import re

def parse_krw(text: str) -> int | None:
    s = (text or "").replace(",", "")
    m = re.search(r"(\d{1,3}(?:,\d{3})+|\d+)\s*(만원|천원|원)?", text or "")
    if not m:
        return None
    n = int(m.group(1).replace(",", ""))
    unit = m.group(2) or "원"
    if unit == "만원":
        n *= 10_000
    elif unit == "천원":
        n *= 1_000
    return n

def extract_rental_price(title: str, body: str | None) -> int | None:
    text = f"{title}\n{body or ''}"
    # “하루/일” 단서 주변의 숫자 우선
    around = re.findall(r"(\d[\d,]*\s*(?:만|천)?원).{0,8}(?:/일|하루|1일|24\s*시간)", text)
    if around:
        return parse_krw(around[0])
    # 미발견 시 첫 가격(보증금 등과 혼동 가능) → signals로 경고
    m = re.search(r"(\d[\d,]*\s*(?:만|천)?원)", text)
    return parse_krw(m.group(1)) if m else None