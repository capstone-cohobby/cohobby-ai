# hosts/agent/llm.py
import os

if os.getenv("DISABLE_LLM", "0") in {"1", "true", "True"}:
    # 테스트/임포트 전용 더미 객체
    class _Dummy:
        def bind_tools(self, *args, **kwargs):
            return self
        async def ainvoke(self, *a, **k):
            raise RuntimeError("LLM disabled for import tests")

    chat_claude = _Dummy()

    def build_prompt(system_text: str):
        class _P:
            def partial(self, **kw): return self
            def __or__(self, other): return self
        return _P()
else:
    # 실제 LLM 연동(테스트에서는 꺼둠)
    from langchain_anthropic import ChatAnthropic

    from hosts.agent.config import settings
    chat_claude = ChatAnthropic(
        model=settings.model,
        anthropic_api_key=settings.anthropic_api_key,
        max_tokens=settings.max_output_tokens,
        temperature=0.2,
    )
    from langchain_core.prompts import ChatPromptTemplate, SystemMessagePromptTemplate
    def build_prompt(system_text: str):
        return ChatPromptTemplate.from_messages([
            SystemMessagePromptTemplate.from_template(system_text),
            ("human", "{user_json}")
        ])
