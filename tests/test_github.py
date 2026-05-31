from __future__ import annotations

import hashlib
import hmac
import json
from datetime import datetime, timezone

import pytest

from kafka_hooks.services.github import (
    GitHubSignatureError,
    build_github_event_envelope,
    parse_json_payload,
    verify_github_signature,
)
from kafka_hooks.services.routing import build_kafka_key, choose_topic


def _signature(secret: str, body: bytes) -> str:
    digest = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def test_verify_github_signature_accepts_valid_signature() -> None:
    body = b'{"zen":"Approachable is better than simple."}'
    verify_github_signature(body, _signature("secret", body), "secret")


def test_verify_github_signature_rejects_invalid_signature() -> None:
    with pytest.raises(GitHubSignatureError):
        verify_github_signature(b"{}", "sha256=bad", "secret")


def test_parse_json_payload_requires_object() -> None:
    assert parse_json_payload(b'{"ok": true}') == {"ok": True}
    with pytest.raises(ValueError):
        parse_json_payload(b"[]")


def test_build_envelope_and_key_prefers_installation_repository() -> None:
    payload = {
        "action": "opened",
        "installation": {"id": 123, "account": {"id": 456, "login": "acme"}},
        "repository": {"id": 789, "full_name": "acme/widgets", "private": True},
        "sender": {"id": 1, "login": "octocat"},
    }

    envelope = build_github_event_envelope(
        payload=payload,
        event_type="pull_request",
        delivery_id="delivery-1",
        hook_id="hook-1",
        source="github",
        include_payload=True,
        include_headers=False,
        received_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    assert envelope["schema_version"] == 1
    assert envelope["action"] == "opened"
    assert envelope["repository"] == {
        "id": 789,
        "node_id": None,
        "name": None,
        "full_name": "acme/widgets",
        "private": True,
        "visibility": None,
        "html_url": None,
        "default_branch": None,
    }
    assert build_kafka_key(envelope, strategy="installation_repository") == "123:789"


def test_choose_topic_uses_routes_then_default() -> None:
    envelope = build_github_event_envelope(
        payload={"action": "opened"},
        event_type="pull_request",
        delivery_id="delivery-1",
        hook_id=None,
        source="github",
        include_payload=True,
        include_headers=False,
    )

    assert (
        choose_topic(
            envelope=envelope,
            default_topic="events",
            topic_routes={"pull_request:opened": "pull-request-opened"},
            topic_template=None,
        )
        == "pull-request-opened"
    )
    assert (
        choose_topic(
            envelope=envelope,
            default_topic="events",
            topic_routes={},
            topic_template=None,
        )
        == "events"
    )


def test_choose_topic_can_use_template() -> None:
    envelope = build_github_event_envelope(
        payload={"repository": {"full_name": "Acme/widgets"}},
        event_type="push",
        delivery_id="delivery-1",
        hook_id=None,
        source="my-app",
        include_payload=True,
        include_headers=False,
    )

    assert (
        choose_topic(
            envelope=envelope,
            default_topic="events",
            topic_routes={},
            topic_template="{source}.{event_type}.{repository_owner}",
        )
        == "my-app.push.acme"
    )


def test_payload_fixture_is_json_serializable() -> None:
    payload = {"repository": {"full_name": "acme/widgets"}}
    envelope = build_github_event_envelope(
        payload=payload,
        event_type="push",
        delivery_id="delivery-1",
        hook_id=None,
        source="github",
        include_payload=True,
        include_headers=False,
    )
    json.dumps(envelope)
