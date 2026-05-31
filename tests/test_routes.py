from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any

from fastapi.testclient import TestClient

from kafka_hooks.core.config import Settings
from kafka_hooks.main import create_app


def test_settings_accept_comma_separated_kafka_bootstrap_env(monkeypatch) -> None:
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092,kafka:19092")

    settings = Settings()

    assert settings.kafka_bootstrap_servers == ["localhost:9092", "kafka:19092"]


def test_settings_resolves_generic_webhook_config() -> None:
    settings = Settings(
        webhook_provider="github",
        webhook_secret="secret",
        webhook_path="hooks/acme",
        webhook_source="acme",
    )

    assert settings.resolved_webhook_path == "/hooks/acme"
    assert settings.resolved_webhook_source == "acme"
    assert settings.resolved_webhook_secrets == ["secret"]


class FakePublisher:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.started = False

    @property
    def is_ready(self) -> bool:
        return True

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.started = False

    async def publish(
        self,
        *,
        topic: str,
        key: str | None,
        value: dict[str, Any],
        headers: dict[str, str],
    ) -> None:
        self.calls.append(
            {
                "topic": topic,
                "key": key,
                "value": value,
                "headers": headers,
            }
        )


def _signature(secret: str, body: bytes) -> str:
    digest = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def _client() -> tuple[TestClient, FakePublisher, Settings]:
    settings = Settings(
        webhook_secret="secret",
        kafka_bootstrap_servers="localhost:9092",
    )
    publisher = FakePublisher()
    app = create_app(settings=settings, publisher=publisher)
    return TestClient(app), publisher, settings


def test_github_webhook_publishes_event() -> None:
    client, publisher, settings = _client()
    payload = {
        "ref": "refs/heads/main",
        "installation": {"id": 123},
        "repository": {"id": 456, "full_name": "acme/widgets"},
        "sender": {"id": 1, "login": "octocat"},
    }
    body = json.dumps(payload).encode("utf-8")

    response = client.post(
        "/webhooks/github",
        content=body,
        headers={
            "x-github-event": "push",
            "x-github-delivery": "delivery-1",
            "x-github-hook-id": "hook-1",
            "x-hub-signature-256": _signature("secret", body),
            "content-type": "application/json",
        },
    )

    assert response.status_code == 202
    assert response.json()["status"] == "published"
    assert response.json()["provider"] == "github"
    assert publisher.calls[0]["topic"] == settings.kafka_topic_default
    assert publisher.calls[0]["key"] == "123:456"
    assert publisher.calls[0]["headers"]["x-github-delivery"] == "delivery-1"
    assert publisher.calls[0]["headers"]["webhook-source"] == "github"
    assert publisher.calls[0]["value"]["payload"] == payload


def test_github_webhook_routes_installation_event_to_installations_topic() -> None:
    client, publisher, settings = _client()
    payload = {
        "action": "created",
        "installation": {"id": 123},
        "sender": {"id": 1, "login": "octocat"},
    }
    body = json.dumps(payload).encode("utf-8")

    response = client.post(
        "/webhooks/github",
        content=body,
        headers={
            "x-github-event": "installation",
            "x-github-delivery": "delivery-1",
            "x-hub-signature-256": _signature("secret", body),
        },
    )

    assert response.status_code == 202
    assert publisher.calls[0]["topic"] == settings.kafka_topic_routes["installation"]
    assert publisher.calls[0]["key"] == "123"


def test_github_webhook_supports_custom_route_and_topic_template() -> None:
    settings = Settings(
        webhook_secret="secret",
        webhook_path="/hooks/org-a/github",
        webhook_source="org-a",
        kafka_bootstrap_servers="localhost:9092",
        kafka_topic_routes={},
        kafka_topic_template="{source}.{event_type}",
    )
    publisher = FakePublisher()
    client = TestClient(create_app(settings=settings, publisher=publisher))
    payload = {
        "installation": {"id": 123},
        "repository": {"id": 456, "full_name": "acme/widgets"},
    }
    body = json.dumps(payload).encode("utf-8")

    response = client.post(
        "/hooks/org-a/github",
        content=body,
        headers={
            "x-github-event": "push",
            "x-github-delivery": "delivery-1",
            "x-hub-signature-256": _signature("secret", body),
        },
    )

    assert response.status_code == 202
    assert publisher.calls[0]["topic"] == "org-a.push"
    assert publisher.calls[0]["value"]["source"] == "org-a"


def test_github_webhook_supports_multiple_secrets() -> None:
    settings = Settings(
        webhook_secrets=["old-secret", "new-secret"],
        kafka_bootstrap_servers="localhost:9092",
    )
    publisher = FakePublisher()
    client = TestClient(create_app(settings=settings, publisher=publisher))
    body = b'{"repository":{"full_name":"acme/widgets"}}'

    response = client.post(
        "/webhooks/github",
        content=body,
        headers={
            "x-github-event": "push",
            "x-github-delivery": "delivery-1",
            "x-hub-signature-256": _signature("new-secret", body),
        },
    )

    assert response.status_code == 202
    assert publisher.calls


def test_github_webhook_can_filter_events() -> None:
    settings = Settings(
        webhook_secret="secret",
        webhook_event_denylist=["push"],
        kafka_bootstrap_servers="localhost:9092",
    )
    publisher = FakePublisher()
    client = TestClient(create_app(settings=settings, publisher=publisher))
    body = b'{"repository":{"full_name":"acme/widgets"}}'

    response = client.post(
        "/webhooks/github",
        content=body,
        headers={
            "x-github-event": "push",
            "x-github-delivery": "delivery-1",
            "x-hub-signature-256": _signature("secret", body),
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "ignored"
    assert publisher.calls == []


def test_github_webhook_can_disable_payload_and_key() -> None:
    settings = Settings(
        webhook_secret="secret",
        envelope_include_payload=False,
        kafka_key_strategy="none",
        kafka_static_headers={"consumer": "analytics"},
        kafka_bootstrap_servers="localhost:9092",
    )
    publisher = FakePublisher()
    client = TestClient(create_app(settings=settings, publisher=publisher))
    payload = {"repository": {"id": 456, "full_name": "acme/widgets"}}
    body = json.dumps(payload).encode("utf-8")

    response = client.post(
        "/webhooks/github",
        content=body,
        headers={
            "x-github-event": "push",
            "x-github-delivery": "delivery-1",
            "x-hub-signature-256": _signature("secret", body),
        },
    )

    assert response.status_code == 202
    assert publisher.calls[0]["key"] is None
    assert "payload" not in publisher.calls[0]["value"]
    assert publisher.calls[0]["headers"]["consumer"] == "analytics"


def test_ping_is_not_published_by_default() -> None:
    client, publisher, _settings = _client()
    body = b'{"zen":"Approachable is better than simple."}'

    response = client.post(
        "/webhooks/github",
        content=body,
        headers={
            "x-github-event": "ping",
            "x-github-delivery": "delivery-1",
            "x-hub-signature-256": _signature("secret", body),
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "pong"
    assert publisher.calls == []


def test_invalid_signature_returns_401() -> None:
    client, publisher, _settings = _client()

    response = client.post(
        "/webhooks/github",
        content=b"{}",
        headers={
            "x-github-event": "push",
            "x-github-delivery": "delivery-1",
            "x-hub-signature-256": "sha256=bad",
        },
    )

    assert response.status_code == 401
    assert publisher.calls == []


def test_unsupported_provider_fails_at_app_creation() -> None:
    settings = Settings(webhook_provider="unknown", webhook_secret="secret")

    try:
        create_app(settings=settings, publisher=FakePublisher())
    except ValueError as exc:
        assert "Unsupported webhook provider" in str(exc)
    else:
        raise AssertionError("Expected unsupported provider to fail.")
