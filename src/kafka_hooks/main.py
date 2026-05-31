from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from kafka_hooks import __version__
from kafka_hooks.api.routes import WebhookResponse, router, webhook_ingress
from kafka_hooks.core.config import Settings, get_settings
from kafka_hooks.core.logging import configure_logging
from kafka_hooks.providers import get_webhook_provider
from kafka_hooks.services.kafka import AIOKafkaEventPublisher, EventPublisher


def create_app(
    *,
    settings: Settings | None = None,
    publisher: EventPublisher | None = None,
) -> FastAPI:
    resolved_settings = settings or get_settings()
    configure_logging(resolved_settings.log_level)
    webhook_provider = get_webhook_provider(resolved_settings.webhook_provider)
    resolved_publisher = publisher or AIOKafkaEventPublisher(resolved_settings)
    owns_publisher = publisher is None

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if owns_publisher:
            await resolved_publisher.start()
        try:
            yield
        finally:
            if owns_publisher:
                await resolved_publisher.stop()

    app = FastAPI(
        title=resolved_settings.app_name,
        version=__version__,
        lifespan=lifespan,
    )
    app.state.settings = resolved_settings
    app.state.publisher = resolved_publisher
    app.state.webhook_provider = webhook_provider
    app.include_router(router)
    app.add_api_route(
        resolved_settings.resolved_webhook_path,
        webhook_ingress,
        methods=["POST"],
        response_model=WebhookResponse,
        operation_id="webhook_ingress",
        tags=["webhooks"],
    )
    return app


app = create_app()
