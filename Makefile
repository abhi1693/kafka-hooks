.PHONY: install dev test lint format

install:
	uv sync --extra dev

dev:
	uv run uvicorn kafka_hooks.main:app --reload --port 8080

test:
	uv run pytest

lint:
	uv sync --extra lint
	uv run ruff check .

format:
	uv run ruff format .
