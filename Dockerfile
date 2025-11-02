# ===========================
# 1️⃣ 공통 베이스 이미지
# ===========================
FROM python:3.12-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# 시스템 도구 (curl은 healthcheck용)
RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

# ===========================
# 2️⃣ 의존성 설치 (Poetry)
# ===========================
FROM base AS deps
RUN pip install --no-cache-dir poetry==1.8.3

# pyproject/lock 복사
COPY pyproject.toml poetry.lock* /app/

# 🔧 pyproject와 lock 불일치면 여기서 맞춰줌
# --no-update로 시도 후 실패하면 전체 re-lock
RUN poetry config virtualenvs.create false \
 && (poetry lock --no-update || poetry lock) \
 && poetry install --no-interaction --no-ansi

# ===========================
# 3️⃣ MCP 서버용 빌드
# ===========================
FROM deps AS mcp
WORKDIR /app
COPY src /app/src
ENV PYTHONPATH=/app:/app/src
EXPOSE 8765
CMD ["python","-m", "cohobby_mcp.servers.datalookup.server"]

# ===========================
# 4️⃣ Agent / Judge 런타임 빌드
# ===========================
FROM deps AS agent
WORKDIR /app
COPY src /app/src
EXPOSE 8080
CMD ["python", "src/main.py"]
