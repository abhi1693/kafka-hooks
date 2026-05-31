# kafka-hooks

Small FastAPI ingress service for webhook-to-Kafka ingestion.

It runs one active webhook provider per deployment, verifies that provider's
signature, builds a stable envelope, and publishes events to Kafka. The app
intentionally does not process provider-specific business logic; consumers
should handle idempotency with `delivery_id`.

The service is intentionally reusable. Provider, route path, source name, event
filters, topic routing, topic templates, key strategy, envelope payload/header
inclusion, and static Kafka headers are all configured from environment
variables.

Currently supported providers:

- `github`

## Endpoints

- `POST /webhooks/github` - webhook ingress when `WEBHOOK_PROVIDER=github`.
  Override with `WEBHOOK_PATH`.
- `GET /health/live` - process liveness.
- `GET /health/ready` - checks Kafka producer startup and webhook secret config.

## Kafka Topics

By default, installation lifecycle events go to:

```text
github.webhooks.installations.v1
```

All other GitHub events go to:

```text
github.webhooks.events.v1
```

Message keys use `KAFKA_KEY_STRATEGY=installation_repository` by default, which
prefers `installation_id:repository_id`, then installation/repository/delivery
fallbacks. Other strategies are `installation`, `repository`, `delivery`,
`event`, and `none`.

Webhook payloads can be large. This service raises the Kafka producer request
and batch limits by default; production brokers and topics should also allow the
same message size.

## Configuration

Copy `.env.example` to `.env` for local development.

```text
WEBHOOK_PROVIDER=github
WEBHOOK_SECRET=...
WEBHOOK_SOURCE=github
WEBHOOK_PATH=/webhooks/github
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_DEFAULT=github.webhooks.events.v1
KAFKA_TOPIC_ROUTES={"installation":"github.webhooks.installations.v1","installation_repositories":"github.webhooks.installations.v1"}
KAFKA_MAX_REQUEST_SIZE=29360128
KAFKA_MAX_BATCH_SIZE=29360128
```

Topic route keys are matched in this order:

```text
event_type:action
event_type
*
```

If no route matches and `KAFKA_TOPIC_TEMPLATE` is set, the template is used.
Supported template fields:

```text
{source}
{provider}
{event_type}
{action}
{installation_id}
{repository_id}
{repository_owner}
{repository_name}
{sender_login}
```

Example:

```text
WEBHOOK_PROVIDER=github
WEBHOOK_SOURCE=analytics
KAFKA_TOPIC_ROUTES={"pull_request:opened":"analytics.github.pr.opened"}
KAFKA_TOPIC_TEMPLATE={source}.{provider}.{event_type}
KAFKA_KEY_STRATEGY=repository
WEBHOOK_EVENT_DENYLIST=ping
```

Envelope controls:

```text
ENVELOPE_INCLUDE_PAYLOAD=true
ENVELOPE_INCLUDE_HEADERS=false
KAFKA_INCLUDE_PROVIDER_HEADERS=true
KAFKA_STATIC_HEADERS={"tenant":"acme","consumer":"activity-pipeline"}
```

Provider ping behavior:

```text
WEBHOOK_PUBLISH_PING=false
```

The app returns a non-2xx response when Kafka publishing fails so GitHub can
retry the delivery.

## Development

```bash
uv sync --extra dev
uv run uvicorn kafka_hooks.main:app --reload --port 8080
uv run pytest
uv sync --extra lint && uv run ruff check .
```

## Docker

```bash
docker build -t kafka-hooks .
docker run --rm -p 8080:8080 \
  -e WEBHOOK_PROVIDER=github \
  -e WEBHOOK_SECRET=change-me \
  -e KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:9092 \
  kafka-hooks
```

Run a Kafka broker separately and set `KAFKA_BOOTSTRAP_SERVERS` to its address.
Then configure the GitHub App webhook URL as:

```text
http://localhost:8080/webhooks/github
```

## Image Publishing

GitHub Actions builds and publishes the image to GHCR as:

```text
ghcr.io/<owner>/<repo>
```

The workflow runs tests first and publishes only when a GitHub release is
published. It pushes only the release tag, for example `v0.0.1`, plus `latest`
for non-prerelease releases.
