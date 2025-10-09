# Claude Pricing Judge (LangChain + MCP)

LLM의 자유판단을 중심으로, 근거는 MCP/툴이 보강하는 구조입니다.

**구성**

agent.py: 1차 판단 → info_need 따라 MCP 툴 호출 → 2차 판단

llm.py: LangChain ChatAnthropic 설정(도구 호출 가능)

tools/mcp.py: MCP HTTP 클라이언트 + LangChain Tool 래퍼

tools/stats.py: IQR 필터/요약, 분위 위치 보조(필요시 사용)

schemas.py: pydantic 스키마(입력/출력)

prompts.py: System 프롬프트 + 규칙

**메모**

실제 MCP 서버는 FastAPI 등으로 별도 구현하세요. 여기서는 HTTP JSON 규약만 가정합니다.

Claude의 응답은 JSON만을 강제합니다(프롬프트로 제한 + 파서 검증).