# Facebook Page Management System

A microservice-based system for managing Facebook Pages. The webhook service receives Facebook events and publishes them to Kafka. The core service analyzes comments and creates action commands. The backend API executes reply/hide actions through the Facebook Graph API. The retry service handles transient failures and routes terminal failures to a dead-letter topic.

## Architecture

```text
Facebook Page
    |
    v
webhook-service :3001
    |
    | raw_events
    v
core-service / core-worker :3002
    |
    | reply_commands
    v
backend-api / backend-worker :3000
    |
    | send_failed
    v
retry-service / retry-worker :3003
    |
    | send_retry / dead_letter
    v
backend-worker / ops
```

All internal service communication goes through Kafka. Services do not call each other directly over HTTP.

## Services

| Service         |         Directory | Port | Responsibility                                                      |
| --------------- | ----------------: | ---: | ------------------------------------------------------------------- |
| backend-api     | `ucode_page_api/` | 3000 | REST API and Facebook Graph API command execution                   |
| webhook-service |  `ucode_webhook/` | 3001 | Webhook verification, payload validation, `raw_events` publishing   |
| core-service    |   `core_service/` | 3002 | Comment analysis, AI/rule classification, `reply_commands` creation |
| retry-service   |  `retry_service/` | 3003 | Exponential backoff retry handling and `dead_letter` routing        |

## Kafka Topics

| Topic            | Producer        | Consumer       |
| ---------------- | --------------- | -------------- |
| `raw_events`     | webhook-service | core-worker    |
| `reply_commands` | core-worker     | backend-worker |
| `send_failed`    | backend-worker  | retry-worker   |
| `send_retry`     | retry-worker    | backend-worker |
| `dead_letter`    | retry-worker    | ops/monitoring |

## Dead Letter Queue Monitoring

`dead_letter` is a Kafka topic, not an application service. The retry worker publishes terminal failures there after `MAX_RETRIES` is exhausted, and no service consumes from it.

Docker Compose includes the operational tooling for this flow:

| Tool         | URL                   | Purpose                                      |
| ------------ | --------------------- | -------------------------------------------- |
| Kafka UI     | http://localhost:8080 | Inspect `dead_letter` messages manually      |
| Prometheus   | http://localhost:9090 | Scrape Kafka offsets and evaluate DLQ alerts |
| Alertmanager | http://localhost:9093 | Route critical alerts immediately            |
| Gmail SMTP   | —                     | Sends DLQ alert emails                       |

Prometheus fires `DeadLetterQueueReceived` when the `dead_letter` topic offset increases within 1 minute:

```promql
increase(kafka_topic_partition_current_offset{topic="dead_letter"}[1m]) > 0
```

Alertmanager routes `severity="critical"` alerts with `group_wait: 0s`, so a new DLQ message sends an immediate email through Gmail SMTP. Configure `ALERTMANAGER_SMTP_FROM`, `ALERTMANAGER_SMTP_USERNAME`, `ALERTMANAGER_SMTP_PASSWORD`, and `ALERTMANAGER_EMAIL_TO` in `.env`. For Gmail, use a Google App Password, not the normal account password.

## Requirements

- Docker and Docker Compose
- Python 3.13 for local test runs outside Docker
- Facebook App/Page credentials for real webhook integration

## Environment Configuration

Create a local `.env` file from the sample file:

```bash
cp .env.example .env
```

Important variables:

```env
DJANGO_SECRET_KEY=change-me
DJANGO_DEBUG=1

FACEBOOK_APP_SECRET=
FACEBOOK_VERIFY_TOKEN=local-verify-token
FACEBOOK_PAGE_ACCESS_TOKEN=
FACEBOOK_ACTIONS_ENABLED=0
PAGE_ID=

DIFY_API_KEY=
ADMIN_API_KEY=
MAX_RETRIES=5
```

By default, `FACEBOOK_ACTIONS_ENABLED=0`, so the backend does not execute real Facebook actions. Set it to `1` only after configuring valid credentials in a staging or production-like environment.

## Run With Docker Compose

Start the full stack:

```bash
docker compose up --build
```

Stop the stack:

```bash
docker compose down
```

## Local Tests

Install dependencies:

```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
pip install -r core_service/requirements.txt
pip install -r ucode_page_api/requirements.txt
```

Run tests per service:

```bash
cd core_service && python manage.py test
cd ../ucode_page_api && python manage.py test
cd ../ucode_webhook && python manage.py test
cd ../retry_service && python manage.py test
```

Run Django system checks:

```bash
cd core_service && python manage.py check
cd ../ucode_page_api && python manage.py check
cd ../ucode_webhook && python manage.py check
cd ../retry_service && python manage.py check
```

## CI

The GitHub Actions workflow is located at:

```text
.github/workflows/ci.yml
```

CI runs a matrix across the four Django services:

- `python manage.py check`
- `python manage.py test`

Add CI secrets in GitHub:

```text
Settings -> Secrets and variables -> Actions -> New repository secret
```

Recommended secrets:

- `DJANGO_SECRET_KEY`
- `FACEBOOK_APP_SECRET`
- `FACEBOOK_VERIFY_TOKEN`
- `FACEBOOK_PAGE_ACCESS_TOKEN`
- `PAGE_ID`
- `ADMIN_API_KEY`
- `DIFY_API_KEY`

Use dedicated CI or staging credentials where possible. Avoid using production tokens for test-only workflows.
