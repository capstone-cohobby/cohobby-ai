# /servers/mcp_datalookup/extractor.py
import json
import pathlib
import re

CATEGORY_MAP = json.loads(
    pathlib.Path(__file__)
    .with_name("rules")
    .with_suffix("")
    .parent.joinpath("category_map_ko.json")
    .read_text(encoding="utf-8")
)

RE_MODEL = re.compile(r"\b([A-Z][A-Za-z0-9\-]+(?:\s?[A-Z0-9][A-Za-z0-9\-]+){0,3})\b")


def extract_product_and_category(title: str, body: str | None):
    text = f"{title} {body or ''}"
    category = None
    for cat, words in CATEGORY_MAP.items():
        for w in words:
            if re.search(rf"\b{re.escape(w)}\b", text, re.I):
                category = cat
                break
        if category:
            break

    # 모델/제품명 후보 (Canon, Sony, Nikon 같은 브랜드 사전 있으면 우선 가중치)
    # 간단 샘플: 괄호 안/앞부분 우선
    m = re.search(r"([A-Za-z가-힣]+[^\n]*)", title)
    candidate = m.group(1) if m else title
    model = None
    for mm in RE_MODEL.finditer(candidate):
        token = mm.group(1).strip()
        if len(token) >= 3:
            model = token
            break

    return model, category
