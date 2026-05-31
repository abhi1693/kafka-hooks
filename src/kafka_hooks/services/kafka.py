from __future__ import annotations

import json
from typing import Any, Protocol

from aiokafka import AIOKafkaProducer

from kafka_hooks.core.config import Settings


class KafkaPublishError(RuntimeError):
    pass


class EventPublisher(Protocol):
    @property
    def is_ready(self) -> bool: ...

    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def publish(
        self,
        *,
        topic: str,
        key: str | None,
        value: dict[str, Any],
        headers: dict[str, str],
    ) -> None: ...


class AIOKafkaEventPublisher:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._producer: AIOKafkaProducer | None = None
        self._started = False

    @property
    def is_ready(self) -> bool:
        return self._started and self._producer is not None

    async def start(self) -> None:
        if self._started:
            return

        kwargs: dict[str, Any] = {
            "bootstrap_servers": self._settings.kafka_bootstrap_servers,
            "client_id": self._settings.kafka_client_id,
            "acks": self._settings.kafka_acks,
            "request_timeout_ms": self._settings.kafka_request_timeout_ms,
            "linger_ms": self._settings.kafka_linger_ms,
            "max_request_size": self._settings.kafka_max_request_size,
            "max_batch_size": self._settings.kafka_max_batch_size,
            "enable_idempotence": self._settings.kafka_enable_idempotence,
            "security_protocol": self._settings.kafka_security_protocol,
        }
        if self._settings.kafka_compression_type:
            kwargs["compression_type"] = self._settings.kafka_compression_type
        if self._settings.kafka_sasl_mechanism:
            kwargs["sasl_mechanism"] = self._settings.kafka_sasl_mechanism
        if self._settings.kafka_sasl_username:
            kwargs["sasl_plain_username"] = self._settings.kafka_sasl_username
        if self._settings.kafka_sasl_password:
            kwargs["sasl_plain_password"] = (
                self._settings.kafka_sasl_password.get_secret_value()
            )

        producer = AIOKafkaProducer(**kwargs)
        await producer.start()
        self._producer = producer
        self._started = True

    async def stop(self) -> None:
        producer = self._producer
        self._producer = None
        self._started = False
        if producer:
            await producer.stop()

    async def publish(
        self,
        *,
        topic: str,
        key: str | None,
        value: dict[str, Any],
        headers: dict[str, str],
    ) -> None:
        if not self._producer:
            raise KafkaPublishError("Kafka producer is not started.")

        encoded_headers = [
            (name, header_value.encode("utf-8"))
            for name, header_value in headers.items()
        ]
        try:
            await self._producer.send_and_wait(
                topic,
                json.dumps(
                    value,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode("utf-8"),
                key=key.encode("utf-8") if key else None,
                headers=encoded_headers,
            )
        except Exception as exc:
            raise KafkaPublishError("Kafka publish failed.") from exc
