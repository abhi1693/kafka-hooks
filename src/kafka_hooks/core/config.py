from __future__ import annotations

import json
from functools import lru_cache
from typing import Annotated, Any

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


DEFAULT_TOPIC_ROUTES = {
    "installation": "github.webhooks.installations.v1",
    "installation_repositories": "github.webhooks.installations.v1",
}
DEFAULT_KAFKA_KEY_STRATEGY = "installation_repository"
KAFKA_KEY_STRATEGIES = {
    "installation_repository",
    "installation",
    "repository",
    "delivery",
    "event",
    "none",
}


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "kafka-hooks"
    app_env: str = "development"
    log_level: str = "INFO"

    webhook_provider: str = "github"
    webhook_source: str | None = None
    webhook_path: str | None = None
    webhook_secret: SecretStr | None = None
    webhook_secrets: Annotated[list[SecretStr], NoDecode] = []
    envelope_include_payload: bool = True
    envelope_include_headers: bool = False
    webhook_event_allowlist: Annotated[list[str], NoDecode] = []
    webhook_event_denylist: Annotated[list[str], NoDecode] = []
    webhook_success_status_code: int = 202
    webhook_publish_ping: bool = False

    webhook_max_body_bytes: int = 26 * 1024 * 1024

    kafka_bootstrap_servers: Annotated[list[str], NoDecode] = Field(
        default_factory=lambda: ["localhost:9092"]
    )
    kafka_topic_default: str = "github.webhooks.events.v1"
    kafka_topic_routes: Annotated[dict[str, str], NoDecode] = Field(
        default_factory=lambda: dict(DEFAULT_TOPIC_ROUTES)
    )
    kafka_topic_template: str | None = None
    kafka_key_strategy: str = DEFAULT_KAFKA_KEY_STRATEGY
    kafka_static_headers: Annotated[dict[str, str], NoDecode] = {}
    kafka_include_provider_headers: bool = True
    kafka_client_id: str = "kafka-hooks"
    kafka_acks: str = "all"
    kafka_request_timeout_ms: int = 10_000
    kafka_linger_ms: int = 5
    kafka_max_request_size: int = 28 * 1024 * 1024
    kafka_max_batch_size: int = 28 * 1024 * 1024
    kafka_enable_idempotence: bool = True
    kafka_compression_type: str | None = None
    kafka_security_protocol: str = "PLAINTEXT"
    kafka_sasl_mechanism: str | None = None
    kafka_sasl_username: str | None = None
    kafka_sasl_password: SecretStr | None = None

    @field_validator("webhook_provider")
    @classmethod
    def normalize_provider(cls, value: str) -> str:
        return value.strip().lower()

    @field_validator("webhook_path", mode="before")
    @classmethod
    def normalize_path(cls, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return None
            return stripped if stripped.startswith("/") else f"/{stripped}"
        return value

    @field_validator("kafka_bootstrap_servers", mode="before")
    @classmethod
    def parse_bootstrap_servers(cls, value: Any) -> Any:
        if value is None or value == "":
            return ["localhost:9092"]
        if isinstance(value, str):
            items = [item.strip() for item in value.split(",")]
            return [item for item in items if item]
        return value

    @field_validator(
        "webhook_event_allowlist",
        "webhook_event_denylist",
        mode="before",
    )
    @classmethod
    def parse_csv_list(cls, value: Any) -> Any:
        if value is None or value == "":
            return []
        if isinstance(value, str):
            items = [item.strip() for item in value.split(",")]
            return [item for item in items if item]
        return value

    @field_validator("webhook_secrets", mode="before")
    @classmethod
    def parse_secret_list(cls, value: Any) -> Any:
        if value is None or value == "":
            return []
        if isinstance(value, str):
            items = [item.strip() for item in value.split(",")]
            return [item for item in items if item]
        return value

    @field_validator("kafka_topic_routes", "kafka_static_headers", mode="before")
    @classmethod
    def parse_json_dict(cls, value: Any) -> Any:
        if value is None or value == "":
            return {}
        if isinstance(value, str):
            parsed = json.loads(value)
            if not isinstance(parsed, dict):
                raise ValueError("must be a JSON object")
            return {str(key): str(item) for key, item in parsed.items()}
        return value

    @field_validator(
        "kafka_compression_type",
        "kafka_sasl_mechanism",
        "kafka_sasl_username",
        "kafka_topic_template",
        mode="before",
    )
    @classmethod
    def empty_string_to_none(cls, value: Any) -> Any:
        if isinstance(value, str):
            stripped = value.strip()
            return stripped or None
        return value

    @field_validator(
        "webhook_event_allowlist",
        "webhook_event_denylist",
    )
    @classmethod
    def normalize_event_lists(cls, value: list[str]) -> list[str]:
        return [item.strip().lower() for item in value if item.strip()]

    @field_validator("kafka_topic_routes")
    @classmethod
    def normalize_topic_routes(cls, value: dict[str, str]) -> dict[str, str]:
        normalized: dict[str, str] = {}
        for key, topic in value.items():
            route_key = str(key).strip().lower()
            route_topic = str(topic).strip()
            if route_key and route_topic:
                normalized[route_key] = route_topic
        return normalized

    @field_validator("kafka_static_headers")
    @classmethod
    def normalize_static_headers(cls, value: dict[str, str]) -> dict[str, str]:
        normalized: dict[str, str] = {}
        for key, header_value in value.items():
            header_name = str(key).strip()
            if header_name:
                normalized[header_name] = str(header_value)
        return normalized

    @field_validator("kafka_key_strategy")
    @classmethod
    def validate_key_strategy(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in KAFKA_KEY_STRATEGIES:
            allowed = ", ".join(sorted(KAFKA_KEY_STRATEGIES))
            raise ValueError(f"kafka_key_strategy must be one of: {allowed}")
        return normalized

    @field_validator(
        "webhook_max_body_bytes",
        "kafka_max_request_size",
        "kafka_max_batch_size",
    )
    @classmethod
    def positive_int(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("must be greater than zero")
        return value

    @field_validator("webhook_success_status_code")
    @classmethod
    def validate_success_status_code(cls, value: int) -> int:
        if value < 200 or value >= 300:
            raise ValueError("webhook_success_status_code must be a 2xx status")
        return value

    @property
    def resolved_webhook_path(self) -> str:
        return self.webhook_path or f"/webhooks/{self.webhook_provider}"

    @property
    def resolved_webhook_source(self) -> str:
        return (self.webhook_source or "").strip() or self.webhook_provider

    @property
    def resolved_webhook_secrets(self) -> list[str]:
        secrets: list[str] = []
        if self.webhook_secret is not None:
            secrets.append(self.webhook_secret.get_secret_value())
        secrets.extend(secret.get_secret_value() for secret in self.webhook_secrets)
        return secrets

    @property
    def resolved_event_allowlist(self) -> list[str]:
        return self.webhook_event_allowlist

    @property
    def resolved_event_denylist(self) -> list[str]:
        return self.webhook_event_denylist

    @property
    def resolved_envelope_include_payload(self) -> bool:
        return self.envelope_include_payload

    @property
    def resolved_envelope_include_headers(self) -> bool:
        return self.envelope_include_headers

    @property
    def resolved_kafka_include_provider_headers(self) -> bool:
        return self.kafka_include_provider_headers


@lru_cache
def get_settings() -> Settings:
    return Settings()
