from __future__ import annotations

from kafka_hooks.providers.base import WebhookProvider


_PROVIDERS: dict[str, WebhookProvider] | None = None


def _providers() -> dict[str, WebhookProvider]:
    global _PROVIDERS
    if _PROVIDERS is None:
        from kafka_hooks.providers.github import GitHubWebhookProvider

        _PROVIDERS = {
            GitHubWebhookProvider.name: GitHubWebhookProvider(),
        }
    return _PROVIDERS


def get_webhook_provider(name: str) -> WebhookProvider:
    normalized = name.strip().lower()
    providers = _providers()
    provider = providers.get(normalized)
    if provider is None:
        supported = ", ".join(sorted(providers))
        raise ValueError(
            f"Unsupported webhook provider {name!r}. Supported providers: {supported}."
        )
    return provider


def list_webhook_providers() -> list[str]:
    return sorted(_providers())
