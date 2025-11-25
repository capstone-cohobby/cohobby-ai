FROM python:3.12-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

FROM base AS deps
RUN pip install --no-cache-dir poetry==1.8.3

COPY pyproject.toml poetry.lock* /app/

RUN poetry config virtualenvs.create false \
 && (poetry lock --no-update || poetry lock) \
 && poetry install --no-interaction --no-ansi

FROM deps AS agent
WORKDIR /app
COPY src /app/src
EXPOSE 8080
CMD ["python", "-m", "src/main"]
