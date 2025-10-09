import json
from pathlib import Path
from typing import List, Dict, Any


def read_jsonl(path: str | Path) -> List[Dict[str, Any]]:
    """
    임시 S3 reader.
    실제 AWS 대신 로컬 JSONL 파일을 읽어서 반환함.
    JSONL: {"key": "value"} 형식의 한 줄짜리 JSON들이 모인 파일

    Args:
        path (str | Path): 로컬 파일 경로 (예: ./data/daangn_sample.jsonl)

    Returns:
        list[dict]: JSON 객체 리스트
    """
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"⚠️ 파일을 찾을 수 없습니다: {file_path}")

    data = []
    with file_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                data.append(json.loads(line))
            except json.JSONDecodeError:
                print(f"⚠️ JSON 파싱 실패: {line[:100]}...")
                continue
    return data


if __name__ == "__main__":
    # ✅ 테스트 실행
    test_file = Path(__file__).parent / "sample_data.jsonl"
    try:
        items = read_jsonl(test_file)
        print(f"✅ {len(items)}개의 레코드를 읽었습니다.")
        print(json.dumps(items[:2], indent=2, ensure_ascii=False))  # 앞부분 미리보기
    except Exception as e:
        print(f"❌ 테스트 실패: {e}")
