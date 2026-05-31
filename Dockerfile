FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:0.11.16 /uv /uvx /bin/

RUN groupadd --system app && useradd --system --gid app --home /app app

COPY pyproject.toml uv.lock README.md ./
COPY src ./src

RUN uv sync --locked --no-dev --no-cache && \
    chown -R app:app /app

ENV PATH="/app/.venv/bin:$PATH"

USER app
EXPOSE 8080

CMD ["uvicorn", "kafka_hooks.main:app", "--host", "0.0.0.0", "--port", "8080"]
