from __future__ import annotations

import time
from datetime import timedelta

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from app.models import EventStatus, FailedAction, FailedActionStatus
from app.services.facebook_client import FacebookClient, FacebookConfig
from app.services.rules import reply_template


class Command(BaseCommand):
    help = "Retry failed Facebook actions stored in DB (FailedAction)."

    def add_arguments(self, parser):
        parser.add_argument("--sleep", type=float, default=2.0)
        parser.add_argument("--limit", type=int, default=20)

    def handle(self, *args, **opts):
        sleep_s: float = opts["sleep"]
        limit: int = opts["limit"]

        fb_client = FacebookClient(
            FacebookConfig(
                graph_api_version=settings.FACEBOOK_GRAPH_API_VERSION,
                page_access_token=settings.FACEBOOK_PAGE_ACCESS_TOKEN,
            )
        )

        self.stdout.write(self.style.SUCCESS("Retry worker started"))

        while True:
            now = timezone.now()
            due = (
                FailedAction.objects.select_related("incoming_event")
                .filter(status=FailedActionStatus.PENDING, next_retry_at__lte=now)
                .order_by("next_retry_at")[:limit]
            )

            if not due:
                time.sleep(sleep_s)
                continue

            for fa in due:
                self._retry_one(fa, fb_client)

    def _retry_one(self, fa: FailedAction, fb_client: FacebookClient) -> None:
        incoming = fa.incoming_event

        if not settings.FACEBOOK_ACTIONS_ENABLED:
            # If actions are disabled, abandon retries to prevent infinite loop.
            fa.status = FailedActionStatus.ABANDONED
            fa.last_error = "actions_disabled"
            fa.save(update_fields=["status", "last_error", "updated_at"])
            if incoming.status == EventStatus.FAILED:
                incoming.status = EventStatus.REVIEW_PENDING
                incoming.error_message = "actions_disabled: moved_to_review"
                incoming.save(update_fields=["status", "error_message", "updated_at"])
            return

        try:
            with transaction.atomic():
                fa.attempts += 1

                comment_id = fa.action_payload.get("comment_id")
                if not comment_id:
                    raise RuntimeError("Missing comment_id for retry")

                # Derive what to do from stored payload
                if fa.action_payload.get("is_spam"):
                    fb_client.hide_comment(comment_id)
                    incoming.status = EventStatus.PROCESSED
                else:
                    reply = reply_template(fa.action_payload.get("intent"))
                    if reply:
                        fb_client.reply_to_comment(comment_id, reply)
                        incoming.status = EventStatus.REPLIED
                    else:
                        incoming.status = EventStatus.PROCESSED

                fa.status = FailedActionStatus.SUCCEEDED
                fa.last_error = ""
                fa.save(update_fields=["attempts", "status", "last_error", "updated_at"])
                incoming.error_message = ""
                incoming.save(update_fields=["status", "error_message", "updated_at"])

        except Exception as exc:  # noqa: BLE001
            if fa.attempts >= fa.max_attempts:
                fa.status = FailedActionStatus.ABANDONED
                incoming.status = EventStatus.REVIEW_PENDING
            else:
                fa.status = FailedActionStatus.PENDING

            fa.last_error = str(exc)
            # Exponential backoff (30s, 60s, 120s...)
            delay = min(30 * (2 ** max(fa.attempts - 1, 0)), 3600)
            fa.next_retry_at = timezone.now() + timedelta(seconds=delay)
            fa.save(
                update_fields=[
                    "attempts",
                    "status",
                    "last_error",
                    "next_retry_at",
                    "updated_at",
                ]
            )
            incoming.error_message = str(exc)
            incoming.save(update_fields=["status", "error_message", "updated_at"])
