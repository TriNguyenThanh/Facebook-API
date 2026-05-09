# Ucode Webhook Agent Guide

This guide applies to work in this folder.

## Build and Test

- Install dependencies: pip install -r requirements.txt
- Apply database migrations: python manage.py migrate
- Run tests: python manage.py test
- Run development server: python manage.py runserver
- Optional production run check: gunicorn core.wsgi:application

## Architecture Map

- Project settings and env loading: [core/settings.py](core/settings.py)
- Root routing and Swagger setup: [core/urls.py](core/urls.py)
- Webhook route definitions: [webhook/urls.py](webhook/urls.py)
- Webhook handlers: [webhook/views.py](webhook/views.py)

## Facebook Webhook Contracts

- GET /api/webhook/facebook/ verifies subscription and must return raw hub.challenge as text/plain when verification succeeds.
- POST /api/webhook/ receives payload data through DRF request.data and returns JSON.
- Webhook endpoints are intentionally public unless the task explicitly changes auth behavior.

## Editing Rules That Matter

- Prefer centralized env-backed settings in [core/settings.py](core/settings.py) and access them via django.conf.settings.
- Use DRF Response for JSON API responses.
- Use HttpResponse only when Facebook requires a plain text challenge body.
- Keep Swagger routes in [core/urls.py](core/urls.py) intact when adding or changing endpoints.
- If models are added in [webhook/models.py](webhook/models.py), include migrations and related tests in the same change.
- Add or update endpoint tests in [webhook/tests.py](webhook/tests.py) for behavior changes.

## Current State Notes

- Kafka dependencies are present, but webhook payload publishing is still TODO in [webhook/views.py](webhook/views.py).
- The webhook app is scaffold-light; avoid unrelated architecture expansion during webhook tasks.