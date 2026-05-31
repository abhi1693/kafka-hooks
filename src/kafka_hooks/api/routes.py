from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from kafka_hooks.core.config import Settings
from kafka_hooks.providers.base import (
    WebhookPayloadError,
    WebhookProvider,
    WebhookSignatureError,
)
from kafka_hooks.services.kafka import EventPublisher, KafkaPublishError
from kafka_hooks.services.routing import (
    build_kafka_key,
    choose_topic,
    is_event_allowed,
)

logger = logging.getLogger(__name__)
router = APIRouter()


class ServiceInfo(BaseModel):
    name: str
    environment: str
    ok: bool = True


class HealthResponse(BaseModel):
    ok: bool
    checks: dict[str, bool]


class WebhookResponse(BaseModel):
    ok: bool
    status: str
    provider: str | None = None
    event_type: str | None = None
    action: str | None = None
    delivery_id: str | None = None
    topic: str | None = None
    key: str | None = None


def _get_settings(request: Request) -> Settings:
    return request.app.state.settings


def _get_publisher(request: Request) -> EventPublisher:
    return request.app.state.publisher


def _get_provider(request: Request) -> WebhookProvider:
    return request.app.state.webhook_provider


def _content_length_exceeds_limit(request: Request, limit: int) -> bool:
    header = request.headers.get("content-length")
    if not header:
        return False
    try:
        return int(header) > limit
    except ValueError:
        return False


@router.get("/", response_model=ServiceInfo)
async def root(request: Request) -> ServiceInfo:
    settings = _get_settings(request)
    return ServiceInfo(name=settings.app_name, environment=settings.app_env)


@router.get("/health/live", response_model=HealthResponse)
async def live() -> HealthResponse:
    return HealthResponse(ok=True, checks={"process": True})


@router.get("/health/ready", response_model=HealthResponse)
async def ready(request: Request) -> JSONResponse:
    settings = _get_settings(request)
    publisher = _get_publisher(request)
    provider = _get_provider(request)
    checks = {
        "webhook_provider": provider.name == settings.webhook_provider,
        "webhook_secret": bool(provider.get_secrets(settings)),
        "kafka_producer": publisher.is_ready,
    }
    ok = all(checks.values())
    return JSONResponse(
        status_code=status.HTTP_200_OK if ok else status.HTTP_503_SERVICE_UNAVAILABLE,
        content=HealthResponse(ok=ok, checks=checks).model_dump(),
    )


async def webhook_ingress(request: Request) -> JSONResponse:
    settings = _get_settings(request)
    publisher = _get_publisher(request)
    provider = _get_provider(request)

    if not provider.get_secrets(settings):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"{provider.name} webhook secret is not configured.",
        )

    if _content_length_exceeds_limit(request, settings.webhook_max_body_bytes):
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="Webhook payload is too large.",
        )

    body = await request.body()
    if len(body) > settings.webhook_max_body_bytes:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="Webhook payload is too large.",
        )

    try:
        event = provider.parse_event(
            body=body,
            headers=request.headers,
            settings=settings,
        )
    except WebhookSignatureError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(exc),
        ) from exc
    except WebhookPayloadError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc

    if not is_event_allowed(
        event_type=event.event_type,
        allowlist=settings.resolved_event_allowlist,
        denylist=settings.resolved_event_denylist,
    ):
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=WebhookResponse(
                ok=True,
                status="ignored",
                provider=provider.name,
                event_type=event.event_type,
                action=event.action,
                delivery_id=event.delivery_id,
            ).model_dump(),
        )

    ack = provider.ack_without_publish(event=event, settings=settings)
    if ack:
        return JSONResponse(
            status_code=ack.status_code,
            content=WebhookResponse(
                ok=True,
                status=ack.status,
                provider=provider.name,
                event_type=event.event_type,
                action=event.action,
                delivery_id=event.delivery_id,
            ).model_dump(),
        )

    topic = choose_topic(
        envelope=event.envelope,
        default_topic=settings.kafka_topic_default,
        topic_routes=settings.kafka_topic_routes,
        topic_template=settings.kafka_topic_template,
    )
    key = build_kafka_key(event.envelope, strategy=settings.kafka_key_strategy)
    headers = provider.build_kafka_headers(event=event, settings=settings)

    try:
        await publisher.publish(
            topic=topic,
            key=key,
            value=event.envelope,
            headers=headers,
        )
    except KafkaPublishError as exc:
        logger.exception(
            "webhook publish failed",
            extra={
                "provider": provider.name,
                "event_type": event.event_type,
                "delivery_id": event.delivery_id,
                "topic": topic,
                "key": key,
            },
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Kafka publish failed.",
        ) from exc

    logger.info(
        "webhook published",
        extra={
            "provider": provider.name,
            "event_type": event.event_type,
            "action": event.action,
            "delivery_id": event.delivery_id,
            "topic": topic,
            "key": key,
        },
    )
    content: dict[str, Any] = WebhookResponse(
        ok=True,
        status="published",
        provider=provider.name,
        event_type=event.event_type,
        action=event.action,
        delivery_id=event.delivery_id,
        topic=topic,
        key=key,
    ).model_dump()
    return JSONResponse(
        status_code=settings.webhook_success_status_code,
        content=content,
    )
