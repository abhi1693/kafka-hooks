from __future__ import annotations

from typing import Any, Mapping

from kafka_hooks.providers.base import ProviderAck, WebhookEvent
from kafka_hooks.services.github import (
    GitHubPayloadError,
    GitHubSignatureError,
    build_github_event_envelope,
    build_github_kafka_headers,
    get_payload_action,
    parse_json_payload,
    verify_github_signature_with_any_secret,
)


class GitHubWebhookProvider:
    name = "github"
    default_path = "/webhooks/github"

    def get_secrets(self, settings: Any) -> list[str]:
        return settings.resolved_webhook_secrets

    def parse_event(
        self,
        *,
        body: bytes,
        headers: Mapping[str, str],
        settings: Any,
    ) -> WebhookEvent:
        event_type = headers.get("x-github-event")
        delivery_id = headers.get("x-github-delivery")
        hook_id = headers.get("x-github-hook-id")
        if not event_type:
            raise GitHubPayloadError("Missing X-GitHub-Event header.")
        if not delivery_id:
            raise GitHubPayloadError("Missing X-GitHub-Delivery header.")

        verify_github_signature_with_any_secret(
            body,
            headers.get("x-hub-signature-256"),
            self.get_secrets(settings),
        )
        payload = parse_json_payload(body)
        action = get_payload_action(payload)
        envelope = build_github_event_envelope(
            payload=payload,
            event_type=event_type,
            delivery_id=delivery_id,
            hook_id=hook_id,
            source=settings.resolved_webhook_source,
            include_payload=settings.resolved_envelope_include_payload,
            include_headers=settings.resolved_envelope_include_headers,
            headers=dict(headers),
        )
        return WebhookEvent(
            event_type=event_type,
            action=action,
            delivery_id=delivery_id,
            hook_id=hook_id,
            envelope=envelope,
        )

    def ack_without_publish(
        self,
        *,
        event: WebhookEvent,
        settings: Any,
    ) -> ProviderAck | None:
        if event.event_type == "ping" and not settings.webhook_publish_ping:
            return ProviderAck(status="pong")
        return None

    def build_kafka_headers(
        self,
        *,
        event: WebhookEvent,
        settings: Any,
    ) -> dict[str, str]:
        return build_github_kafka_headers(
            event_type=event.event_type,
            delivery_id=event.delivery_id,
            hook_id=event.hook_id,
            source=settings.resolved_webhook_source,
            action=event.action,
            include_github_headers=settings.resolved_kafka_include_provider_headers,
            static_headers=settings.kafka_static_headers,
        )


__all__ = [
    "GitHubPayloadError",
    "GitHubSignatureError",
    "GitHubWebhookProvider",
]
