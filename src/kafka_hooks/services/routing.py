from __future__ import annotations

import re
from typing import Any


_TOPIC_VALUE_PATTERN = re.compile(r"[^A-Za-z0-9._-]+")


def _object_or_none(value: Any) -> dict[str, Any] | None:
    return value if isinstance(value, dict) else None


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _topic_value(value: Any) -> str:
    text = _string_or_none(value)
    if not text:
        return "none"
    return _TOPIC_VALUE_PATTERN.sub("_", text.strip().lower()).strip("_") or "none"


def _topic_context(envelope: dict[str, Any]) -> dict[str, str]:
    repository = _object_or_none(envelope.get("repository")) or {}
    installation = _object_or_none(envelope.get("installation")) or {}
    sender = _object_or_none(envelope.get("sender")) or {}
    repository_full_name = _string_or_none(repository.get("full_name")) or ""
    owner, _, repo = repository_full_name.partition("/")
    return {
        "source": _topic_value(envelope.get("source")),
        "provider": _topic_value(envelope.get("provider")),
        "event_type": _topic_value(envelope.get("event_type")),
        "action": _topic_value(envelope.get("action")),
        "installation_id": _topic_value(installation.get("id")),
        "repository_id": _topic_value(repository.get("id")),
        "repository_owner": _topic_value(owner),
        "repository_name": _topic_value(repo or repository.get("name")),
        "sender_login": _topic_value(sender.get("login")),
    }


def choose_topic(
    *,
    envelope: dict[str, Any],
    default_topic: str,
    topic_routes: dict[str, str],
    topic_template: str | None,
) -> str:
    event_type = _topic_value(envelope.get("event_type"))
    action = _topic_value(envelope.get("action"))
    route_candidates = [f"{event_type}:{action}", event_type, "*"]
    for candidate in route_candidates:
        topic = topic_routes.get(candidate)
        if topic:
            return topic
    if topic_template:
        return topic_template.format_map(_topic_context(envelope))
    return default_topic


def build_kafka_key(envelope: dict[str, Any], *, strategy: str) -> str | None:
    installation = _object_or_none(envelope.get("installation"))
    repository = _object_or_none(envelope.get("repository"))

    installation_id = _string_or_none(installation.get("id") if installation else None)
    repository_id = _string_or_none(repository.get("id") if repository else None)
    repository_full_name = _string_or_none(
        repository.get("full_name") if repository else None
    )
    delivery_id = _string_or_none(envelope.get("delivery_id"))
    event_type = _string_or_none(envelope.get("event_type"))

    if strategy == "none":
        return None
    if strategy == "installation_repository":
        if installation_id and repository_id:
            return f"{installation_id}:{repository_id}"
        return installation_id or repository_id or repository_full_name or delivery_id
    if strategy == "installation":
        return installation_id or delivery_id
    if strategy == "repository":
        return repository_id or repository_full_name or delivery_id
    if strategy == "delivery":
        return delivery_id
    if strategy == "event":
        return event_type or delivery_id
    return delivery_id


def is_event_allowed(
    *,
    event_type: str,
    allowlist: list[str],
    denylist: list[str],
) -> bool:
    normalized = event_type.strip().lower()
    if normalized in denylist:
        return False
    return not allowlist or normalized in allowlist
