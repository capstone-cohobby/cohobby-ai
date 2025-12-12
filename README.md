# COHOBBY-AI 가격 산정 에이전트 

단순 LLM 체인을 넘어 LangGraph 기반의 멀티 스테이지, 병렬 RAG 파이프라인으로 진화한 가격 판단 시스템

C2C 취미/장비 대여 플랫폼을 위한 **AI 기반 대여가 / 보증금 / 대여 규칙 추천 에이전트**입니다.  
LangGraph + LangChain + ChromaDB + Tavily 를 사용해, 실제 당근 대여 게시글·분쟁 사례·중고 시세를 근거로 **일관된 JSON 응답**을 생성합니다.


## 전체 파이프라인 흐름도 
    
    graph TD
    A[START: enrich_input<br/>- 입력 정규화<br/>- signature 생성] --> B[check_verdict_cache<br/>- 캐시 조회]

    %% Cache branch (cache는 구현미완. 실제로 아직 저장하지 않고 있습니다)
    B -- Cache HIT --> G[finalize_parallel<br/>- 기존 verdict 그대로 재구성]
    B -- Cache MISS --> C[retrieve_all_node<br/>- 내부/외부 RAG 병렬 검색]

    %% RAG branch
    C --> D[merge_evidence<br/>- internal/web/dispute/used 통합]
    D --> E[summarize_rag_evidence<br/>- Evidence 분석 리포트 생성]
    E --> G[finalize_parallel<br/>- Price/Deposit/Rules 병렬 결정]

    %% Gate after price
    G --> H{gate_after_price<br/>- 가격 신뢰도 판단}

    H -- reasonable --> Z[END<br/>- 결과 반환]

    %% Fallback: Deriver 경로
    H -- uncertain --> I[retrieve_sale_price_node<br/>- 판매·중고 시세 위주 RAG]
    I --> J[derive_rental_price_node<br/>- 시세→대여가 Deriver]
    J --> Z

## 아키텍처 레벨 다이어그램
    
    flowchart LR
    %% Input Layer
    subgraph Input[Input Layer]
        U[사용자 입력<br/>User / API Request]
        S[Signature Generator<br/>입력 정규화 & 서명]
    end

    %% Cache Layer
    subgraph Cache[Cache Layer]
        R[Redis Verdict Cache<br/>price/deposit/rules 결과 캐시]
    end

    %% RAG Layer
    subgraph RAG[RAG / Retrieval Layer]
        I[Internal Chroma Index<br/>대여 DB 벡터스토어]
        D[Dispute Chroma Index<br/>분쟁 사례 벡터스토어]
        W[External Tavily API<br/>웹/중고/판매 시세 검색]
        M[Evidence Merger<br/>점수 재조정 & 통합]
        Y[Summarizer LLM<br/>RAG Evidence 분석 리포트]
    end

    %% Decision Layer
    subgraph Decision[Decision Layer]
        P[Parallel Finalizer<br/>Price / Deposit / Rules<br/>병렬 LLM 호출]
        G2[Gate After Price<br/>신뢰도 판정]
        F1[Sale RAG Fallback<br/>판매/중고 위주 재검색]
        F2[Rental Deriver<br/>시세→대여가 산정 로직]
    end

    %% Monitoring / Logging (선택)
    subgraph Monitoring[Monitoring / Observability]
        L[LangSmith<br/>Trace / Log / Prompt Debug]
    end

    %% Edges
    U --> S --> R
    R -- Cache HIT --> P

    R -- Cache MISS --> I
    R -- Cache MISS --> D
    R -- Cache MISS --> W

    I & D & W --> M --> Y --> P

    P --> G2
    G2 -- Reasonable --> L
    G2 -- Uncertain --> F1 --> F2 --> L

---

## 1. 프로젝트 개요

- **목표**
  - 사용자가 입력한 `물품명 / 카테고리 / 상태 / 구입 시기`만으로
    - 대여 **일일 가격 범위**
    - **보증금** 권장 수준
    - **대여 규칙**(파손·분실·연체 대응 등)
    를 자동으로 산출하는 에이전트.

- **핵심 특징**
  - 단일 LLM 호출이 아니라,  
    **Probe → RAG Evidence 수집 → Evidence 분석(Analyst) → 최종 의사결정(Price/Deposit/Rules)**  
    로 나뉜 **멀티 스텝 LangGraph 파이프라인**.
  - 내부 대여 DB, 전자거래분쟁조정위원회 분쟁 사례 PDF, 웹 검색(중고/판매가) 등을 모두 합쳐서 판단.
  - 모든 출력은 **엄격한 JSON 스키마**를 따르고,  
    LLM의 Chain-of-Thought는 `<thinking>...</thinking>` 태그 안에 감추고 JSON만 파싱해서 사용.

- **기술 스택**
| 구성요소                    | 설명                                   |
| ----------------------- | ------------------------------------ |
| 🧩 **LangGraph**        | 노드/게이트 기반의 그래프 오케스트레이션               |
| ⚙️ **LangChain (LCEL)** | RunnableParallel 기반 체인 구성            |
| 💬 **LLM Provider**     | OpenAI GPT API                 |
| 🔍 **RAG Engine**       | 내부 ChromaHybridIndex + 외부 Tavily API |
| ⏱ **Async Framework**   | asyncio 기반 비동기 실행                    |

---

## 2. 주요 기능

- **대여 가격 추천**
  - 내부 대여 데이터에 **실제 대여가가 있는 경우**: 그 구간을 중심으로 추천가 산정
  - 내부에 없으면: 중고 시세/판매가 기반으로 **ROI 및 감가상각 로직**으로 대여가 추정

- **보증금 산정**
  - 물품 가격, 카테고리, 분쟁 사례에서 추출한 리스크를 종합해 **보증금 비율/금액** 추천

- **대여 규칙 추천**
  - 분쟁 사례 DB(RAG)를 기반으로
    - 파손·분실 시 처리
    - 연체, 하자, 취소/환불 조건
    등을 **규칙 리스트 형태**로 추천

- **결과 포맷**
  - `PriceDecision`, `DepositDecision`, `RulesDecision` 세 개의 JSON 오브젝트로 반환
  - 프론트/백엔드에서 바로 파싱해서 UI에 표출 가능

---

## 3. 전체 아키텍처

### 3.1 High-level Flow

1. **입력 수신**
   - FastAPI 등의 HTTP API에서 `AgentInput` (name, category, condition, bought_at …) 수신
2. **LangGraph 파이프라인 실행**
   - `enrich_input`에서 입력 정규화 + 캐시 키 생성
   - 캐시 히트 여부 확인 후, 필요 시 RAG 수행
3. **RAG Evidence 수집**
   - 내부 대여 DB(Chroma)
   - 웹 대여/렌탈 정보(Tavily)
   - 중고/판매가 정보(Tavily, 특정 도메인 필터)
   - 분쟁 사례 DB(Chroma, DisputeIndex)
4. **RAG Evidence 분석**
   - `chain_rag_summarizer`가 Evidence를 요약·충돌/아웃라이어 탐지·기준(baseline) 결정
5. **최종 의사결정**
   - `chain_parallel_finalize`에서
     - 가격
     - 보증금
     - 규칙
     을 **서로 독립적인 프롬프트**로 병렬 실행
6. **JSON 응답 반환**
   - FastAPI 레이어에서 `EstimationResponse`로 포장하여 클라이언트로 반환

**구성**

graph_pipeline.py: LangGraph 그래프 정의 및 실행 진입점

judge.py: 가격, 보증금, 규칙 판단 체인 + JSON 파서 + 캐시 로직

rag_tools.py: 내부/외부 RAG 툴, 증거 병합/요약 유틸

llm.py: LangChain 설정

schemas.py: pydantic 스키마(입력/출력)

prompts.py: System 프롬프트 + 규칙


---

## 4. 기술 스택 & 선택 이유

### 4.1 LLM & 프롬프트 엔지니어링

- **LangChain + LangGraph**
  - LangChain: 개별 LLM 체인, RAG 체인, 프롬프트 템플릿, Runnable 구성 등 **기본 블럭** 담당
  - LangGraph: 전체 플로우를 **상태 기반 그래프(StateGraph)**로 명시
    - 노드 간 데이터 이동, 캐시 분기, 에러 핸들링을 코드 구조에 녹여서 관리
    - “어떤 순서로 어떤 Evidence를 보고, 언제 Derivation으로 갈지”를 **눈에 보이게 설계**

- **LLM 제공자**
  - `llm.py`에서 `ChatOpenAI`를 감싸는 **팩토리 함수** 제공
  - 설정 파일(`config.settings`)에서
    - provider (예: openai)
    - model_name (결정용 / 요약용 분리)
    - temperature, max_tokens
    등을 주입해서 쉽게 교체 가능

- **JSON Mode**
  - OpenAI JSON 모드(`response_format={"type": "json_object"}`) + 프롬프트에서 **“JSON 밖의 텍스트 금지”**를 강하게 요구
  - judge 레이어에서 `_extract_json_from_content()`로
    - `<thinking>` 태그 제거
    - 코드 블록 제거
    - `tool_calls` / `response_metadata` 안에 들어간 args까지 검사
    해서 **최종적으로 순수 JSON만 추출**하도록 설계

### 4.2 Embedding & Vector Search

- **임베딩 모델**
  - `text-embedding-3-small (OpenAI)`
  - 이유:
    - 한/영 혼합 텍스트(당근 게시글, 분쟁 PDF 등)에 대해 **충분한 품질**
    - 가격이 저렴해서 실제 서비스에 적합
    - 속도가 빨라서 RAG 요청이 많은 상황에서 유리

- **벡터 스토어 – ChromaDB**
  - `emb_store.py`에서 Chroma 클라이언트 래퍼(`ChromaHybridIndex`, `DisputeIndex`) 구현
  - 장점:
    - 로컬 개발/테스트 용이
    - 간단한 설정으로 컬렉션 분리(내부 대여 DB vs 분쟁 사례 DB)
    - Embedding 함수(OpenAIEmbeddingFunction) 주입만으로 바로 사용 가능

- **Hybrid Search 전략**
  - 순수 벡터 거리 + `rapidfuzz` 를 사용한 **부분 문자열 유사도(partial_ratio)**를 합쳐 점수 계산
  - 점수 예:
    - `base = 1 - distance`
    - `score = base + 0.05 * title_similarity`
  - 이렇게 해서 “의미적으로 비슷하지만 제목도 어느 정도 매칭되는 글”에 가산점 부여

### 4.3 RAG & Evidence Policy

- **내부 대여 DB (ChromaHybridIndex)**
  - `title + snippet + "price:금액"` 형태로 문서를 구성해서 저장
  - 검색 시:
    - “대여/렌탈” 키워드 포함 정도
    - `price` 필드 존재 여부
    를 기준으로 **추가 가중치**를 곱해서 순위 조정  
    → “실제 대여가가 있는 문서”가, 단순 소개/판매 글보다 항상 더 위에 오도록 설계

- **분쟁 사례 DB (DisputeIndex)**
  - 전자거래분쟁조정위원회 PDF를 텍스트로 추출 후, `RecursiveCharacterTextSplitter`로 `chunk_size=2000` 기준으로 나눠서 저장
  - 카테고리별 필터(`where_clause["category"]`) 지원
  - 검색 결과는 `{"content": ..., "source": "dispute", "score": ...}` 형태의 dict로 반환

- **외부 웹 검색 – Tavily**
  - `TAVILY_API_KEY` 기반 Web Search
  - 목적별로 쿼리/도메인 분리:
    - 대여/렌탈 중심 검색
    - 중고/판매가 중심 검색 (중고나라, 번개장터, 당근 등 도메인 한정)
  - LangGraph에서 내부 DB와 **병렬로 호출**해서, 상황에 따라
    - 내부 데이터 > 웹 데이터
    - 내부 데이터가 없으면 → 웹/중고 데이터 기반으로 fallback
    가 가능하도록 구성

---

## 5. LangGraph 워크플로 상세

### 5.1 상태 정의

- `GraphState`
  - `inp`: 원본 입력 dict (name, category, condition, bought_at …)
  - `evidence`: RAG로 모인 모든 증거 리스트
  - `used_evidence`: 중고/판매가 관련 증거
  - `dispute_evidence`: 분쟁 사례 증거
  - `rag_analysis_report`: Evidence를 통합 분석한 JSON 리포트
  - `price_decision`, `deposit_decision`, `rules_decision`: 최종 출력
  - `signature`, `cache_hit`: 캐시 관련 필드
  - `error`: 에러 메시지

### 5.2 주요 노드

- `enrich_input`
  - 입력 정리 + 시그니처 생성 (`make_signature`)
  - 추후 캐시 키로 사용

- `check_verdict_cache`
  - `get_verdict(signature)`로 기존 결과 존재 여부 확인
  - 있으면:
    - `price_decision` 바로 세팅
    - `cache_hit=True`, `info_need="none"` → RAG 건너뜀
  - 없으면:
    - `cache_hit=False`, `info_need="high"`

- `retrieve_all_node`
  - 단일 노드에서 **4개 채널 Evidence 병렬 수집**
    1. 내부 대여 DB (`retrieve_internal`)
    2. 외부 대여/렌탈 Web (`retrieve_external_web`)
    3. 중고/판매 Web (`retrieve_used_price_web`)
    4. 분쟁 사례 DB (`retrieve_dispute_cases`)
  - 결과를 `evidence`, `used_evidence`, `dispute_evidence_raw` 등에 저장

- `summarize_rag_evidence`
  - `chain_rag_summarizer` 호출
  - Evidence들을 바탕으로:
    - `summary_text`
    - `analysis_reasoning`
    - `basis_of_summary` (internal_priority / web_priority / blended / no_data)
    - `conflict_detected`, `outlier_info`
    를 담은 JSON 리포트 생성 → `rag_analysis_report`에 저장

- `derive_rental_price_node`
  - 내부 대여 Evidence가 충분치 않을 때
  - 중고/판매 Evidence를 기반으로
    - 기본 단가(원가)
    - 감가/회수 기간(몇 번 대여 시 원금 회수?)
    를 가정해서 **가이드라인 대여가**를 산출

- `finalize_parallel`
  - 최종 단계
  - `AgentInput` 객체를 구성하여, 세 개의 체인을 **병렬 실행**
    - `chain_price`   → `PriceDecision`
    - `chain_deposit` → `DepositDecision`
    - `chain_rules`   → `RulesDecision`
  - 에러가 나면 각 체인별로 **안전한 기본값**을 반환하도록 보호 로직 포함
  - 결과를 `price_decision`, `deposit_decision`, `rules_decision`에 세팅 후 종료

- 마지막에 `graph.compile()` → `app_graph` 로 사용

---

## 6. 프롬프트 설계 & CoT 전략

프롬프트들은 `prompt.py` / `prompts.prompt` 모듈에 정의되어 있으며, 크게 4종류로 나뉩니다.

1. **Probe Prompt (`SYSTEM_PROMPT_PROBE`)**
   - 목적: 입력으로부터 **카테고리**와 **정보 충분도(info_need)**만 간단히 판정
   - 출력: `"category"`, `"info_need"` 를 가진 JSON
   - 제약:
     - `<thinking>` 블록 안에서만 내부 추론
     - 외부로는 **JSON 외 아무 텍스트도 허용하지 않음**

2. **RAG Summarizer Prompt (`SYSTEM_PROMPT_RAG_SUMMARIZER`)**
   - 역할: 다양한 출처(internal / web / dispute / used)의 Evidence를 비교·분석
   - 출력:
     - `summary_text`
     - `analysis_reasoning`: 다음 LLM이 근거를 이해할 수 있도록 **상세한 분석 설명**
     - `basis_of_summary`: 어떤 Evidence를 더 신뢰했는지
     - `conflict_detected`, `outlier_info`
   - 내부 CoT:
     - `<thinking>`에 실제 비교·충돌 분석 과정을 단계별로 서술
     - 외부 JSON에는 요약된 근거만 남겨, 추론 과정은 UI에는 직접 노출하지 않음

3. **Price / Deposit / Rules Prompt**
   - 각 의사결정마다 별도 System Prompt 사용
   - 공통 특징:
     - **통화 단위는 반드시 KRW(원)**, 숫자만 사용 (예: 50000)
     - Evidence와 `rag_analysis_report`를 바탕으로 JSON 스키마에 맞게 응답
     - `reasoning` 필드에 “어떤 Evidence를 근거로 어느 정도 margin을 줬는지”를 설명
   - 여기서 **사용자에게 보여줄 근거 설명**은 `reasoning` 필드에 담기고,
     `<thinking>` 블록 내의 세부 CoT는 서버에서 잘라내어 숨긴다는 점이 핵심.

4. **Deriver Prompt (`SYSTEM_PROMPT_DERIVER`)**
   - 내부 대여가가 없는 품목에 대해 중고/판매가 기반 anchor price를 산출
   - “몇 번 대여하면 원금을 회수할지”, “대여 시장에 거의 없는 품목인지” 등을 고려해  
     보수적인 대여가 가이드를 생성

---

## 7. 데이터 인덱싱 전략

### 7.1 내부 대여 DB (ChromaHybridIndex)

- 입력 데이터 예:
  - 실제 당근 대여 게시글
  - 크롤링/수집된 JSON (`id`, `title`, `snippet`, `price`, `category`, `region` …)
- 인덱싱 시:
  - `documents`: `"제목\n내용\nprice:{price}"` 형태로 구성
  - `metadatas`: 원본 JSON 대부분 유지 (id, listing_id, url 등)
- 검색 시 재점수 로직:
  - 벡터 유사도가 너무 낮으면 버림 (threshold)
  - 그 이상은:
    - 제목/내용에 “대여/렌탈” 단어 포함 → **1.5배**
    - `price` 필드 존재 → **2.0배**
  - 이렇게 하면:
    - “실제 대여가가 있는 internal 데이터”는 최대 점수가 2.0 이상
    - 웹 검색 Evidence 점수는 최대 1.0 근처로 유지  
      → **대여가가 있는 internal Evidence가 항상 우선**

### 7.2 분쟁 사례 DB (DisputeIndex)

- 매우 긴 판결문/사례 텍스트를 그냥 넣지 않고,
  - `chunk_size=2000`, `chunk_overlap=200` 기준으로 잘라서 저장
- 검색 결과는:
  - `{"content": chunk_text, "source": "dispute", "score": ..., "case_id": ..., "category": ...}` 형태로 반환
- Price/Deposit/Rules 체인은 이 Evidence를 기반으로,
  - “어떤 상황에서 분쟁이 났는지”
  - “보증금·배상 책임이 어떻게 판단됐는지”
  를 참고해서 규칙/보증금 산정 로직에 반영

---

## 8. 테스트 & 평가

### 8.1 정성 + 정량 평가용 테스트 케이스 (`pricing_cases.json`)

- 대표적인 테스트 케이스를 JSON 파일로 관리:
  - A 타입: 내부 대여가가 명확히 있는 품목
  - B 타입: 대여 시장에는 거의 없고, 중고/판매가로만 추정해야 하는 품목
  - C 타입: “축구공”, “텐트”, “유니폼”처럼 **품목명이 너무 일반적인 케이스**
- 각 케이스에 대해:
  - 입력: `name`, `condition`, `bought_at`, `category`
  - 메타: `case_type`, `notes`, `reference_price_per_day` (있다면)
- 이 JSON을 읽어서 `app_graph.ainvoke()`를 돌리고,
  - PriceDecision의 `price.point`, `low`, `high`가
  - 사람이 수동으로 정한 기준 범위 안에 들어오는지 체크할 수 있도록 설계

---

## 9. 설치 & 실행 방법

### 9.1 요구 사항

- Python 3.10+
- Poetry 또는 pip (프로젝트 환경에 따라)
- OpenAI / Tavily API Key
- 로컬 ChromaDB 폴더 (혹은 호스트된 Chroma 서버)

### 9.2 환경 변수 설정

`.env` 예시:

```bash
OPENAI_API_KEY=sk-...
TAVILY_API_KEY=tvly-...
CHROMA_DB_DIR=./chroma_db


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
poetry run uvicorn main:app --reload --host 0.0.0.0 --port 8080


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
