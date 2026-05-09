from __future__ import annotations

import json
import time
from datetime import timedelta

from confluent_kafka import Consumer
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import IntegrityError, transaction
from django.utils import timezone

from app.models import EventStatus, FailedAction, FailedActionStatus, IncomingEvent, SocialProfile
from app.services.ai_classifier import DifyClassifier
from app.services.event_parser import extract_comment_events
from app.services.facebook_client import FacebookClient, FacebookConfig
from app.services.kafka_utils import KafkaJsonProducer
from app.services.rules import classify_rule_based, content_hash, reply_template

class Command(BaseCommand):
    help = "Consume raw Facebook webhook events from Kafka topic and process comment events."

    def add_arguments(self, parser):
        parser.add_argument("--topic", default=settings.KAFKA_RAW_EVENTS_TOPIC)
        parser.add_argument("--group", default=settings.KAFKA_CONSUMER_GROUP)
        parser.add_argument("--poll", type=float, default=1.0)
        parser.add_argument("--max-batch", type=int, default=50)

    def handle(self, *args, **opts):
        topic: str = opts["topic"]
        group: str = opts["group"]
        poll_timeout: float = opts["poll"]

        consumer = Consumer(
            {
                "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                "group.id": group,
                "auto.offset.reset": settings.KAFKA_AUTO_OFFSET_RESET,
                "enable.auto.commit": False,
            }
        )
        consumer.subscribe([topic])

        failed_producer = KafkaJsonProducer(settings.KAFKA_BOOTSTRAP_SERVERS)
        fb_client = FacebookClient(
            FacebookConfig(
                graph_api_version=settings.FACEBOOK_GRAPH_API_VERSION,
                page_access_token=settings.FACEBOOK_PAGE_ACCESS_TOKEN,
            )
        )

        # Initialise Dify classifier if API key is configured
        ai_classifier: DifyClassifier | None = None
        if getattr(settings, "DIFY_API_KEY", ""):
            try:
                ai_classifier = DifyClassifier()
                self.stdout.write(self.style.SUCCESS("Dify AI classifier enabled"))
            except Exception as exc:  # noqa: BLE001
                self.stderr.write(f"Dify init failed, falling back to rule-based: {exc}")

        self.stdout.write(
            self.style.SUCCESS(
                f"Consuming topic={topic} group={group} bootstrap={settings.KAFKA_BOOTSTRAP_SERVERS}"
            )
        )

        try:
            while True:
                msg = consumer.poll(poll_timeout)
                if msg is None:
                    continue

                if msg.error():
                    self.stderr.write(str(msg.error()))
                    continue

                raw = msg.value()
                if not raw:
                    consumer.commit(message=msg, asynchronous=False)
                    continue

                try:
                    payload = json.loads(raw.decode("utf-8"))
                except Exception as exc:  # noqa: BLE001
                    self.stderr.write(f"Invalid JSON payload: {exc}")
                    consumer.commit(message=msg, asynchronous=False)
                    continue

                parsed_events = extract_comment_events(payload)
                if not parsed_events:
                    consumer.commit(message=msg, asynchronous=False)
                    continue

                for evt in parsed_events:
                    self._process_comment_event(
                        evt,
                        payload=payload,
                        fb_client=fb_client,
                        failed_producer=failed_producer,
                        ai_classifier=ai_classifier,
                    )

                consumer.commit(message=msg, asynchronous=False)
        except KeyboardInterrupt:
            self.stdout.write("Stopping consumer...")
        finally:
            try:
                failed_producer.flush()
            except Exception:
                pass
            consumer.close()

    def _process_comment_event(self, evt, payload, fb_client, failed_producer, ai_classifier=None):
        now = timezone.now()

        # Prevent infinite loops: ignore comments sent by our own page
        if getattr(settings, "FACEBOOK_PAGE_ID", "") and str(evt.sender_id) == str(settings.FACEBOOK_PAGE_ID):
            self.stdout.write(self.style.NOTICE(f"Ignoring our own comment: {evt.comment_id}"))
            return

        # Ignore nested replies (parent_id is the comment it replies to, not the post)
        if evt.parent_id and evt.post_id and str(evt.parent_id) != str(evt.post_id):
            self.stdout.write(self.style.NOTICE(f"Ignoring nested reply: {evt.comment_id}"))
            return

        profile, _ = SocialProfile.objects.get_or_create(social_id=evt.sender_id)

        self.stdout.write(self.style.WARNING(f"--- Processing New Comment ---"))
        self.stdout.write(f"Event ID: {evt.event_id}")
        self.stdout.write(f"Sender ID: {evt.sender_id}")
        self.stdout.write(f"Comment ID: {evt.comment_id}")
        self.stdout.write(f"Message: {evt.message}")

        msg_hash = content_hash(evt.message)
        defaults = {
            "sender": profile,
            "content": evt.message,
            "content_hash": msg_hash,
            "raw_payload": payload,
            "status": EventStatus.RECEIVED,
        }

        try:
            with transaction.atomic():
                incoming, created = IncomingEvent.objects.get_or_create(
                    event_id=evt.event_id, defaults=defaults
                )
        except IntegrityError:
            incoming = IncomingEvent.objects.get(event_id=evt.event_id)
            created = False

        if not created and incoming.status in (EventStatus.PROCESSED, EventStatus.REPLIED, EventStatus.REVIEW_PENDING):
            return

        incoming.content = evt.message
        incoming.content_hash = msg_hash

        # --- Step 1: Rule-based spam detection (fast, no API cost) ---
        classification = classify_rule_based(evt.message)
        incoming.is_spam = classification.is_spam
        classification_reason = classification.spam_reason

        # Spam repetition heuristic: same content_hash from same sender within 24h
        if not incoming.is_spam:
            window_start = now - timedelta(hours=24)
            same_count = IncomingEvent.objects.filter(
                sender=profile, content_hash=msg_hash, created_at__gte=window_start
            ).exclude(pk=incoming.pk).count()
            if same_count >= 1:
                incoming.is_spam = True
                classification_reason = "repeated_content"

        # --- Step 2: AI classification (intent, sentiment, natural reply) ---
        # Only call Gemini when message is not already flagged as spam.
        ai_reply: str | None = None
        if not incoming.is_spam and ai_classifier is not None:
            ai_result = ai_classifier.classify(evt.message)
            incoming.intent = ai_result.intent
            incoming.sentiment = ai_result.sentiment
            ai_reply = ai_result.reply
        else:
            # Fallback: rule-based intent/sentiment
            incoming.intent = classification.intent
            incoming.sentiment = classification.sentiment

        # Update profile spam counters
        if incoming.is_spam:
            profile.last_spam_at = now
            profile.spam_count_24h = IncomingEvent.objects.filter(
                sender=profile, is_spam=True, created_at__gte=now - timedelta(hours=24)
            ).count() + 1

        # Decision
        try:
            if incoming.is_spam:
                # For comment spam: hide it; if no comment_id, push to review
                if evt.comment_id and settings.FACEBOOK_ACTIONS_ENABLED:
                    fb_client.hide_comment(evt.comment_id)
                    incoming.status = EventStatus.PROCESSED
                elif evt.comment_id:
                    incoming.status = EventStatus.REVIEW_PENDING
                    incoming.error_message = (
                        f"actions_disabled: would_hide_comment reason={classification_reason}"
                    )
                else:
                    incoming.status = EventStatus.REVIEW_PENDING
                    incoming.error_message = f"missing_comment_id reason={classification_reason}"

                # Blacklist if repeated spam threshold
                if profile.spam_count_24h >= 3:
                    profile.is_blacklisted = True

            else:
                # Auto reply: prefer AI-generated reply, fall back to rule-based template
                reply = ai_reply or reply_template(incoming.intent)
                if reply and evt.comment_id and (not profile.is_blacklisted):
                    if settings.FACEBOOK_ACTIONS_ENABLED:
                        fb_client.reply_to_comment(evt.comment_id, reply)
                        incoming.status = EventStatus.REPLIED
                    else:
                        incoming.status = EventStatus.PROCESSED
                        incoming.error_message = "actions_disabled: would_reply"
                else:
                    incoming.status = EventStatus.PROCESSED

            profile.save(update_fields=["is_blacklisted", "spam_count_24h", "last_spam_at", "updated_at"])
            incoming.save(
                update_fields=[
                    "content",
                    "content_hash",
                    "raw_payload",
                    "intent",
                    "sentiment",
                    "is_spam",
                    "status",
                    "error_message",
                    "updated_at",
                ]
            )

        except Exception as exc:  # noqa: BLE001
            incoming.status = EventStatus.FAILED
            incoming.error_message = str(exc)
            incoming.save(update_fields=["status", "error_message", "updated_at"])

            # Persist retry
            fa = FailedAction.objects.create(
                incoming_event=incoming,
                action_type="facebook_action",
                action_payload={
                    "comment_id": evt.comment_id,
                    "intent": incoming.intent,
                    "is_spam": incoming.is_spam,
                },
                attempts=0,
                max_attempts=5,
                next_retry_at=timezone.now() + timedelta(seconds=30),
                status=FailedActionStatus.PENDING,
                last_error=str(exc),
            )

            # Also publish send_failed for observability
            try:
                failed_producer.produce(
                    settings.KAFKA_SEND_FAILED_TOPIC,
                    {
                        "event_id": incoming.event_id,
                        "failed_action_id": fa.id,
                        "error": str(exc),
                        "ts": int(time.time()),
                    },
                    key=incoming.event_id,
                )
            except Exception:
                pass
