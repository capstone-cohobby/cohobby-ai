
# llm.py
from langchain_anthropic import ChatAnthropic
from anthropic import Anthropic
from .config import settings

chat_claude = ChatAnthropic(
    anthropic_api_key=settings.anthropic_api_key,
    model=settings.anthropic_model,
    temperature=settings.temperature,
    max_tokens=settings.max_output_tokens,
    timeout=settings.llm_request_timeout,
)

anthropic_client = Anthropic(
    api_key=settings.anthropic_api_key,
    timeout=settings.llm_request_timeout,
)

# ✅ Structured Chat agent로 직접 구성
from hosts.agent.tools.mcp import TOOLS
from langchain.agents import create_tool_calling_agent, AgentExecutor
from langchain.prompts import ChatPromptTemplate

def make_agent(verbose: bool = True) -> AgentExecutor:
    """
    최신 Tool Calling 방식의 Anthropic 에이전트를 생성합니다.
    """
    # 1. 최신 방식에 맞는 간단한 프롬프트 정의
    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", "You are a helpful assistant that uses tools to answer questions."),
            ("human", "{input}"),
            ("placeholder", "{agent_scratchpad}"),
        ]
    )
    
    # 2. create_tool_calling_agent 사용 (가장 중요한 변경점)
    agent = create_tool_calling_agent(chat_claude, TOOLS, prompt)
    
    # 3. AgentExecutor 생성 (이 부분은 동일)
    return AgentExecutor(agent=agent, tools=TOOLS, verbose=verbose)

__all__ = ["chat_claude", "anthropic_client", "make_agent"]

