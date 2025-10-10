import os
from anthropic import Anthropic

def classify_category(name: str, condition: str, bought_at: str):
    client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    prompt = f"""
    물품명: {name}
    상태: {condition}
    구입시기: {bought_at}

    위 정보를 보고 이 물품이 속하는 카테고리를 한 단어로 정리해줘.
    예: '캠핑용품', '전자기기', '가전제품', '의류', '스포츠장비' 등.
    """
    response = client.messages.create(
        model="claude-3-5-sonnet-20240620",
        max_tokens=30,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text.strip()
