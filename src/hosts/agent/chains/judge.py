# src/hosts/agent/chains/judge.py
from __future__ import annotations

import json

from langchain_core.output_parsers import PydanticOutputParser, StrOutputParser
from langchain_core.prompts import ChatPromptTemplate

from hosts.agent.llm import chat_claude
from hosts.agent.prompts.prompt import SYSTEM_PROMPT
from hosts.agent.schemas import AgentInput, DecisionOutput
from hosts.agent.tools.mcp import TOOLS

parser = PydanticOutputParser(pydantic_object=DecisionOutput)

prompt = ChatPromptTemplate.from_messages([
    ("system", SYSTEM_PROMPT),
    ("human", "{user_json}")
])

first_chain = (
    {"user_json": lambda x: json.dumps(AgentInput(**x).model_dump(), ensure_ascii=False)}
    | prompt
    | chat_claude.bind_tools(TOOLS)
    | StrOutputParser()
)

second_chain = (
    {"user_json": lambda x: json.dumps(AgentInput(**x).model_dump(), ensure_ascii=False)}
    | prompt
    | chat_claude.bind_tools(TOOLS)
    | StrOutputParser()
)

async def judge_once(payload: AgentInput) -> DecisionOutput:
    raw1 = await first_chain.ainvoke(payload.model_dump())
    try:
        first = DecisionOutput.model_validate_json(raw1)
    except Exception as e:
        raise RuntimeError(f"Claude JSON parse error: {e}\nRaw: {raw1[:400]}") from e

    if first.info_need in ("medium", "high"):
        raw2 = await second_chain.ainvoke(payload.model_dump())
        try:
            second = DecisionOutput.model_validate_json(raw2)
        except Exception as e:
            raise RuntimeError(f"Claude JSON parse error(2nd): {e}\nRaw: {raw2[:400]}") from e
        return second

    return first
