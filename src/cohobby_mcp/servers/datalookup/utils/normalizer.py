import re
from typing import Optional

def parse_price_krw(value: object) -> Optional[int]:
    """
    문자열/숫자 가격을 '원' 단위 정수로 변환.
    지원 예:
      - '100000.0' -> 100000
      - '15,000원' -> 15000
      - '2만 5천' / '2만5천' -> 25000
      - '2.5만' -> 25000
      - '5천' -> 5000
      - '2만5' (천 생략) -> 25000
      - '2.5' (단위 없고 <10의 실수) -> 2.5만으로 간주 -> 25000
    읽지 못하면 None 반환
    """
    if value is None:
        return None

    s = str(value).strip().lower()
    if not s:
        return None
    s = s.replace(",", "")

    # 1) 'x만 y천'
    m = re.search(r"(\d+(?:\.\d+)?)\s*만\s*(\d+(?:\.\d+)?)?\s*천?", s)
    if m:
        man = float(m.group(1))
        cheon = float(m.group(2) or 0)
        v = int(man * 10000 + cheon * 1000)
        return v if v > 0 else None

    # 2) 'x.x만' 또는 'x만'
    m = re.search(r"(\d+(?:\.\d+)?)\s*만", s)
    if m:
        v = int(float(m.group(1)) * 10000)
        return v if v > 0 else None

    # 3) 'x천'
    m = re.search(r"(\d+(?:\.\d+)?)\s*천", s)
    if m:
        v = int(float(m.group(1)) * 1000)
        return v if v > 0 else None

    # 4) 숫자 + '원' (또는 숫자만)
    m = re.fullmatch(r"\d+(?:\.\d+)?(?:원)?", s)
    if m:
        num = m.group(0).replace("원", "")
        try:
            val = float(num)
        except ValueError:
            return None
        # '작은 실수'(단위 없음 & <10)는 '만' 단위로 간주
        if "." in num and 0 < val < 10:
            v = int(val * 10000)
        else:
            v = int(round(val))
        return v if v > 0 else None

    # 5) 'x만y' (천 생략 케이스 보정: '2만5' -> 25000)
    m = re.search(r"(\d+(?:\.\d+)?)\s*만\s*(\d+)\b", s)
    if m:
        man = float(m.group(1))
        rest = float(m.group(2))
        v = int(man * 10000 + rest * 1000)
        return v if v > 0 else None

    return None

def days_from_duration(text: Optional[str]) -> float:
    """
    자연어 기간을 '일 수'로 환산.
    지원 예:
      - '2박3일' -> 3.0
      - '2일' / '2~3일' -> 2.0 (앞 숫자 채택)
      - '하루' / '당일' -> 1.0
      - '1주' / '2주일' -> 7.0 / 14.0
      - '6시간' -> max(1.0, 6/24) = 1.0  (최소 1일 보정)
      - '한달' / '1개월' / '30일' -> 30.0
    읽지 못하면 기본 1.0
    """
    if not text:
        return 1.0
    t = re.sub(r"\s+", "", str(text))

    # 2박3일 → 3일
    m = re.search(r"(\d+)\s*박\s*(\d+)\s*일", t)
    if m:
        try:
            return float(m.group(2))
        except Exception:
            return 1.0

    # 1일 / 2~3일 → 앞 숫자
    m = re.search(r"(\d+)(?:\s*[-~]\s*\d+)?\s*일", t)
    if m:
        try:
            return float(m.group(1))
        except Exception:
            return 1.0

    # 하루/당일
    if re.search(r"(하루|당일)", t):
        return 1.0

    # 1주 / 2주일
    m = re.search(r"(\d+)\s*주(?:일)?", t)
    if m:
        try:
            return float(int(m.group(1)) * 7)
        except Exception:
            return 7.0

    # 6시간 -> 최소 1일
    m = re.search(r"(\d+)\s*시간", t)
    if m:
        try:
            hours = int(m.group(1))
            return max(1.0, hours / 24.0)
        except Exception:
            return 1.0

    # 한달/1개월/30일
    if re.search(r"(한달|1개월|30일)", t):
        return 30.0

    return 1.0

def extract_rental_price(title: str, body: Optional[str]) -> Optional[int]:
    """
    제목/본문의 자연어에서 가격을 찾아 정수 KRW로 반환.
    - '2만5천', '2.5만', '15,000원', '100000.0', '2만5' 등 지원
    - 없으면 None
    """
    blob = " ".join(x for x in [(title or ""), (body or "")] if x)
    return parse_price_krw(blob)