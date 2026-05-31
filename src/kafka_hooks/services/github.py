from __future__ import annotations

import hashlib
import hmac
import json
from datetime import datetime, timezone
from typing import Any

from kafka_hooks.providers.base import WebhookPayloadError, WebhookSignatureError


class GitHubPayloadError(WebhookPayloadError):
    pass


class GitHubSignatureError(WebhookSignatureError):
    pass


def parse_json_payload(payload: bytes) -> dict[str, Any]:
    try:
        value = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise GitHubPayloadError("Invalid JSON payload.") from exc
    if not isinstance(value, dict):
        raise GitHubPayloadError("Payload must be a JSON object.")
    return value


def verify_github_signature(
    payload: bytes,
    signature_header: str | None,
    secret: str,
) -> None:
    if not signature_header:
        raise GitHubSignatureError("Missing GitHub signature.")
    if not signature_header.startswith("sha256="):
        raise GitHubSignatureError("Invalid GitHub signature format.")

    actual = signature_header.split("=", 1)[1]
    expected = hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, actual):
        raise GitHubSignatureError("Invalid GitHub signature.")


def verify_github_signature_with_any_secret(
    payload: bytes,
    signature_header: str | None,
    secrets: list[str],
) -> None:
    if not secrets:
        raise GitHubSignatureError("GitHub webhook secret is not configured.")
    errors: list[GitHubSignatureError] = []
    for secret in secrets:
        try:
            verify_github_signature(payload, signature_header, secret)
            return
        except GitHubSignatureError as exc:
            errors.append(exc)
    raise errors[-1] if errors else GitHubSignatureError("Invalid GitHub signature.")


def _object_or_none(value: Any) -> dict[str, Any] | None:
    return value if isinstance(value, dict) else None


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _compact_repository(payload: dict[str, Any]) -> dict[str, Any] | None:
    repository = _object_or_none(payload.get("repository"))
    if not repository:
        return None
    return {
        "id": repository.get("id"),
        "node_id": repository.get("node_id"),
        "name": repository.get("name"),
        "full_name": repository.get("full_name"),
        "private": repository.get("private"),
        "visibility": repository.get("visibility"),
        "html_url": repository.get("html_url"),
        "default_branch": repository.get("default_branch"),
    }


def _compact_installation(payload: dict[str, Any]) -> dict[str, Any] | None:
    installation = _object_or_none(payload.get("installation"))
    if not installation:
        return None
    account = _object_or_none(installation.get("account"))
    return {
        "id": installation.get("id"),
        "node_id": installation.get("node_id"),
        "account": {
            "id": account.get("id"),
            "login": account.get("login"),
            "type": account.get("type"),
        }
        if account
        else None,
    }


def _compact_sender(payload: dict[str, Any]) -> dict[str, Any] | None:
    sender = _object_or_none(payload.get("sender"))
    if not sender:
        return None
    return {
        "id": sender.get("id"),
        "login": sender.get("login"),
        "type": sender.get("type"),
        "avatar_url": sender.get("avatar_url"),
        "html_url": sender.get("html_url"),
    }


def get_payload_action(payload: dict[str, Any]) -> str | None:
    action = payload.get("action")
    return action if isinstance(action, str) else None


def _selected_headers(headers: dict[str, str]) -> dict[str, str]:
    allowed = {
        "x-github-delivery",
        "x-github-event",
        "x-github-hook-id",
        "x-github-hook-installation-target-id",
        "x-github-hook-installation-target-type",
    }
    return {
        key: value
        for key, value in headers.items()
        if key.lower() in allowed and value is not None
    }


def build_github_event_envelope(
    *,
    payload: dict[str, Any],
    event_type: str,
    delivery_id: str,
    hook_id: str | None,
    source: str,
    include_payload: bool,
    include_headers: bool,
    headers: dict[str, str] | None = None,
    received_at: datetime | None = None,
) -> dict[str, Any]:
    timestamp = received_at or datetime.now(timezone.utc)
    envelope = {
        "schema_version": 1,
        "provider": "github",
        "source": source,
        "received_at": timestamp.isoformat(),
        "delivery_id": delivery_id,
        "event_type": event_type,
        "action": get_payload_action(payload),
        "hook_id": _string_or_none(hook_id),
        "installation": _compact_installation(payload),
        "repository": _compact_repository(payload),
        "sender": _compact_sender(payload),
    }
    if include_payload:
        envelope["payload"] = payload
    if include_headers and headers:
        envelope["headers"] = _selected_headers(headers)
    return envelope


def build_github_kafka_headers(
    *,
    event_type: str,
    delivery_id: str,
    hook_id: str | None,
    source: str,
    action: str | None,
    include_github_headers: bool,
    static_headers: dict[str, str],
) -> dict[str, str]:
    headers = dict(static_headers)
    headers["webhook-source"] = source
    headers["webhook-event"] = event_type
    headers["webhook-delivery"] = delivery_id
    if action:
        headers["webhook-action"] = action
    if include_github_headers:
        headers["x-github-event"] = event_type
        headers["x-github-delivery"] = delivery_id
        if hook_id:
            headers["x-github-hook-id"] = hook_id
    return headers

