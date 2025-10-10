import os
from anthropic import Anthropic, APIStatusError, APIConnectionError, RateLimitError
from dotenv import load_dotenv
load_dotenv()
def test_claude_connection():
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        print("❌ 환경 변수 ANTHROPIC_API_KEY가 설정되지 않았어요.")
        print("   예: export ANTHROPIC_API_KEY='sk-ant-...'\n")
        return

    client = Anthropic(api_key=api_key)

    print("🚀 Claude API 연결 테스트 중...\n")
    try:
        response = client.messages.create(
            model=os.getenv("ANTHROPIC_MODEL"),   # 🔧 최신 Sonnet 모델 사용
            max_tokens=128,
            messages=[
                {"role": "user", "content": "테스트: 연결이 잘 되나요?"}
            ]
        )

        print("✅ 연결 성공!")
        print("- 모델:", response.model)
        print("- 응답:", response.content[0].text)
        print("- 토큰 사용량:", response.usage)
        print("\n🎯 Anthropic API가 정상 작동 중입니다.\n")

    except APIStatusError as e:
        print(f"❌ API 오류: {e.status_code} - {e.message}")
    except RateLimitError:
        print("⚠️ 호출 한도를 초과했습니다. 잠시 후 다시 시도하세요.")
    except APIConnectionError:
        print("🌐 네트워크 연결 실패: 인터넷 또는 방화벽을 확인하세요.")
    except Exception as e:
        print("❌ 예기치 못한 오류:", e)

if __name__ == "__main__":
    test_claude_connection()
