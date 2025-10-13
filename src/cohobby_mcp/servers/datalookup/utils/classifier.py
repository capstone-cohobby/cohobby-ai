# /servers/mcp_datalookup/classifier.py
import re

RE_POS_RENT = re.compile(r"(대여|렌트|하루|일일|주말대여|보증금|반납|대여가능)", re.I)
RE_POS_SALE = re.compile(r"(판매|팝니다|급처|가격내림|택배가능)", re.I)
RE_NEG_RENT = re.compile(r"(대여\s*아님|렌탈\s*아님)", re.I)


def classify_listing(title: str, body: str | None) -> tuple[str, list[str]]:
    text = f"{title}\n{body or ''}"
    sig = []
    if RE_NEG_RENT.search(text):
        sig.append("neg_rent")
        # 부정이 있으면 우선 대여 아님 → sale 신호가 있으면 sale, 아니면 unknown
        if RE_POS_SALE.search(text):
            sig.append("pos_sale")
            return "sale", sig
        return "unknown", sig

    rent = bool(RE_POS_RENT.search(text))
    sale = bool(RE_POS_SALE.search(text))
    if rent and not sale:
        sig.append("pos_rent")
        return "rental", sig
    if sale and not rent:
        sig.append("pos_sale")
        return "sale", sig

    # 둘 다 있거나 애매하면 가격 단위 힌트로 보정
    if re.search(r"(/일|1일|하루|24\s*시간)", text):
        sig.append("unit_per_day")
        return "rental", sig

    return "unknown", sig
