from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Protocol


class WebhookPayloadError(ValueError):
    pass


class WebhookSignatureError(ValueError):
    pass


@dataclass(frozen=True)
class ProviderAck:
    status: str
    status_code: int = 200


@dataclass(frozen=True)
class WebhookEvent:
    event_type: str
    action: str | None
    delivery_id: str
    hook_id: str | None
    envelope: dict[str, Any]


class WebhookProvider(Protocol):
    name: str
    default_path: str

    def get_secrets(self, settings: Any) -> list[str]: ...

    def parse_event(
        self,
        *,
        body: bytes,
        headers: Mapping[str, str],
        settings: Any,
    ) -> WebhookEvent: ...

    def ack_without_publish(
        self,
        *,
        event: WebhookEvent,
        settings: Any,
    ) -> ProviderAck | None: ...

    def build_kafka_headers(
        self,
        *,
        event: WebhookEvent,
        settings: Any,
    ) -> dict[str, str]: ...
