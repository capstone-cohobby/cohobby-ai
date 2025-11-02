import json
import pathlib
import re

# 1) 카테고리 맵은 선택(Optional)
CATEGORY_MAP = {}
try:
    _rules_dir = pathlib.Path(__file__).with_name("rules").with_suffix("").parent
    _map_path = _rules_dir.joinpath("category_map_ko.json")
    if _map_path.exists():
        CATEGORY_MAP = json.loads(_map_path.read_text(encoding="utf-8"))
except Exception:
    CATEGORY_MAP = {}  # 없거나 깨져도 절대 크래시 금지

# 2) 아주 약한 힌트(맵 없이도 돌아가도록)
HINT_WORDS = {
    "캠핑": ["텐트", "랜턴", "버너", "의자", "캠핑"],
    "전자기기": ["카메라", "드론", "노트북", "휴대폰", "탭", "태블릿"],
    "스포츠": ["축구", "농구", "배드민턴", "자전거"],
    "생활": ["청소기", "에어컨", "빨래", "건조기", "정수기"],
}

RE_MODEL = re.compile(r"\b([A-Z][A-Za-z0-9\-]+(?:\s?[A-Z0-9][A-Za-z0-9\-]+){0,3})\b")

def extract_product_and_category(title: str, body: str | None):
    """
    최종 카테고리 '결정'은 하지 않는다.
    - model: 모델/제품명 추정 (없을 수 있음)
    - category: 있으면 '힌트' 수준(맵/힌트워드에서 발견 시), 없으면 None
    """
    text = f"{title} {body or ''}"
    category = None

    # 2-1) CATEGORY_MAP 우선
    if CATEGORY_MAP:
        for cat, words in CATEGORY_MAP.items():
            for w in words:
                if re.search(rf"\b{re.escape(w)}\b", text, re.I):
                    category = cat
                    break
            if category:
                break
    # 2-2) 맵이 없거나 불발이면 HINT_WORDS로 약한 힌트
    if not category:
        for cat, words in HINT_WORDS.items():
            for w in words:
                if re.search(rf"\b{re.escape(w)}\b", text, re.I):
                    category = cat
                    break
            if category:
                break

    # 모델/제품명 후보 (간단 휴리스틱)
    m = re.search(r"([A-Za-z가-힣]+[^\n]*)", title)
    candidate = m.group(1) if m else title
    model = None
    for mm in RE_MODEL.finditer(candidate):
        token = mm.group(1).strip()
        if len(token) >= 3:
            model = token
            break

    # 주의: 여기서의 category는 'hint'일 뿐. 최종 판단은 Agent가 한다.
    return model, category