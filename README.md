# COHOBBY-AI 가격 산정 에이전트 

LangGraph 기반의 조건부 상태 기계(Conditional State Machine) 에이전트
사용자가 입력한 물품명, 상태, 구입 시기를 바탕으로 내부(Chroma)와 외부(Tavily) RAG를 병렬 수행하여 대여가, 보증금, 대여 규칙을 자동 산정한다.

단순 LLM 체인을 넘어 LangGraph 기반의 멀티 스테이지, 병렬 RAG 파이프라인으로 진화한 가격 판단 시스템

## 기술 스택
| 구성요소                    | 설명                                   |
| ----------------------- | ------------------------------------ |
| 🧩 **LangGraph**        | 노드/게이트 기반의 그래프 오케스트레이션               |
| ⚙️ **LangChain (LCEL)** | RunnableParallel 기반 체인 구성            |
| 💬 **LLM Provider**     | OpenAI GPT API                 |
| 🔍 **RAG Engine**       | 내부 ChromaHybridIndex + 외부 Tavily API |
| 💾 **Cache Layer**      | Redis 기반 결과 캐시 (확실한 결과만 저장)          |
| ⏱ **Async Framework**   | asyncio 기반 비동기 실행                    |

**구성**

graph_pipeline.py: LangGraph 그래프 정의 및 실행 진입점

judge.py: 가격, 보증금, 규칙 판단 체인 + JSON 파서 + 캐시 로직

rag_tools.py: 내부/외부 RAG 툴, 증거 병합/요약 유틸

llm.py: LangChain 설정

schemas.py: pydantic 스키마(입력/출력)

prompts.py: System 프롬프트 + 규칙

## 전체 파이프라인 흐름도 
    
    graph TD
    A[START: enrich_input] --> B(check_verdict_cache)

    B -- Cache HIT --> G[finalize_parallel]
    B -- Cache MISS --> C(retrieve_both)

    C --> D(merge_evidence)
    D --> E(summarize_rag_evidence)
    E --> G

    G --> H{gate_after_price}

    H -- reasonable --> Z[END]
    H -- uncertain --> I(retrieve_sale_price_node)
    I --> J(derive_rental_price_node)
    J --> Z


## 아키텍처 레벨 다이어그램
    
    flowchart LR
        subgraph Input
            U[User Input]
            S[Signature Generator]
        end

        subgraph Cache
            R[Redis Verdict Cache]
        end

        subgraph RAG
            I[Internal Chroma Index]
            W[External Tavily API]
            M[Merge Evidence]
            Y[Summarizer (LLM)]
        end

        subgraph Decision
            P[Parallel Finalizer]
            G2[Gate After Price]
            F1[Sale RAG Fallback]
            F2[Rental Deriver]
        end

        U --> S --> R
        R -- Miss --> I & W --> M --> Y
        Y --> P
        R -- Hit --> P
        P --> G2
        G2 -- Uncertain --> F1 --> F2


## 환경 변수
| 이름                               | 설명                 |
| -------------------------------- | ------------------ |
| `ANTHROPIC_API_KEY`              | LLM 호출 키           |
| `TAVILY_API_KEY`                 | 외부 검색 키            |
| `CHROMA_DB_DIR`                  | 내부 인덱스 저장 디렉토리     |
| `AWS_S3_BUCKET` / `S3_INPUT_KEY` | (선택) 과거 MCP/S3 통합용 |

## 실행 방법
**1. 의존성 설치**

poetry install

**2. 환경 변수 설정**

export ANTHROPIC_API_KEY=...
export TAVILY_API_KEY=...

**3. (선택) 내부 인덱스 구축**

python -m scripts.indexer_chroma

**4. 그래프 실행**
poetry run python -m hosts.agent.chains.graph_pipeline

### API I/O 예시
**요청(입력 JSON)**

    {
        "name": "라이즈 응원봉",
        "condition": "5회 미만 사용, 불 잘 들어옴",
        "bought_at": "2024년 5월"   
    }

**응답 (LLM 결과)**

    {
        "decision": "reasonable",
        "price": { "value": 5000, "unit": "원/일" },
        "deposit": { "value": 20000 },
        "rules": ["렌탈 후 바로 반납", "외관 손상 시 보증금 차감"],
        "confidence": 0.84,
        "evidence_source": "rag_internal_external"
    }
