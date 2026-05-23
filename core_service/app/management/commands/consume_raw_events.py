from __future__ import annotations

import json
from collections import defaultdict
from datetime import timedelta
from uuid import uuid4

from confluent_kafka import Consumer
from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone

from app.services.ai_classifier import DifyClassifier
from app.services.event_parser import ParsedCommentEvent, extract_comment_events, is_direct_post_comment
from app.services.kafka_utils import KafkaJsonProducer
from app.services.rules import classify_rule_based, content_hash, reply_template


class Command(BaseCommand):
    help = "Consume normalized raw_events and publish reply_commands."

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.processed_events: set[str] = set()
        self.blacklisted_senders: set[str] = set()
        self.sender_events: dict[str, list] = defaultdict(list)
        self.sender_spam_events: dict[str, list] = defaultdict(list)
        self.sender_content_events: dict[tuple[str, str], list] = defaultdict(list)

    def add_arguments(self, parser):
        parser.add_argument("--topic", default=settings.KAFKA_RAW_EVENTS_TOPIC)
        parser.add_argument("--group", default=settings.KAFKA_CONSUMER_GROUP)
        parser.add_argument("--poll", type=float, default=1.0)

    def handle(self, *args, **opts):
        consumer = Consumer(
            {
                "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                "group.id": opts["group"],
                "auto.offset.reset": settings.KAFKA_AUTO_OFFSET_RESET,
                "enable.auto.commit": False,
            }
        )
        consumer.subscribe([opts["topic"]])

        producer = KafkaJsonProducer(settings.KAFKA_BOOTSTRAP_SERVERS)
        ai_classifier: DifyClassifier | None = None
        if getattr(settings, "DIFY_API_KEY", ""):
            try:
                ai_classifier = DifyClassifier()
                self.stdout.write(self.style.SUCCESS("Dify AI classifier enabled"))
            except Exception as exc:  # noqa: BLE001
                self.stderr.write(f"Dify init failed, using rule fallback: {exc}")

        self.stdout.write(
            self.style.SUCCESS(
                f"Consuming topic={opts['topic']} group={opts['group']} bootstrap={settings.KAFKA_BOOTSTRAP_SERVERS}"
            )
        )

        try:
            while True:
                msg = consumer.poll(opts["poll"])
                if msg is None:
                    continue
                if msg.error():
                    self.stderr.write(str(msg.error()))
                    continue

                try:
                    payload = json.loads((msg.value() or b"{}").decode("utf-8"))
                    for evt in extract_comment_events(payload):
                        self._process_event(evt, producer, ai_classifier)
                except Exception as exc:  # noqa: BLE001
                    self.stderr.write(f"raw_events processing failed: {exc}")
                finally:
                    consumer.commit(message=msg, asynchronous=False)
        except KeyboardInterrupt:
            self.stdout.write("Stopping consumer...")
        finally:
            producer.flush()
            consumer.close()

    def _process_event(
        self,
        evt: ParsedCommentEvent,
        producer: KafkaJsonProducer,
        ai_classifier: DifyClassifier | None,
    ) -> None:
        if evt.event_id in self.processed_events:
            return
        if not is_direct_post_comment(evt):
            self.processed_events.add(evt.event_id)
            return

        command = self._build_reply_command(evt, timezone.now(), ai_classifier)
        producer.produce(
            settings.KAFKA_REPLY_COMMANDS_TOPIC,
            command,
            key=command["command_id"],
        )
        producer.flush()
        self.processed_events.add(evt.event_id)

    def _build_reply_command(
        self,
        evt: ParsedCommentEvent,
        now,
        ai_classifier: DifyClassifier | None,
    ) -> dict:
        self._trim_old(now)
        self.sender_events[evt.sender_id].append(now)

        recent_count = len(self.sender_events[evt.sender_id])
        if recent_count > getattr(settings, "RATE_LIMIT_EVENTS_PER_MINUTE", 20):
            return self._command(evt, "queue_for_review", None, None, None, "pending_review")

        if evt.sender_id in self.blacklisted_senders:
            return self._command(evt, "no_action", None, None, None, "processed")

        msg_hash = content_hash(evt.message)
        classification = classify_rule_based(evt.message)
        is_spam = classification.is_spam

        if not is_spam and self.sender_content_events[(evt.sender_id, msg_hash)]:
            is_spam = True

        ai_reply: str | None = None
        intent = classification.intent
        if not is_spam and ai_classifier is not None:
            ai_result = ai_classifier.classify(evt.message)
            intent = ai_result.intent
            ai_reply = ai_result.reply

        self.sender_content_events[(evt.sender_id, msg_hash)].append(now)

        if is_spam:
            self.sender_spam_events[evt.sender_id].append(now)
            if len(self.sender_spam_events[evt.sender_id]) >= 3:
                self.blacklisted_senders.add(evt.sender_id)
            return self._command(evt, "hide_comment", None, intent, classification.sentiment, "processed")

        reply = ai_reply or reply_template(intent)
        if reply:
            return self._command(evt, "reply", reply, intent, classification.sentiment, "processed")

        return self._command(evt, "no_action", None, intent, classification.sentiment, "processed")

    def _trim_old(self, now) -> None:
        one_minute_ago = now - timedelta(minutes=1)
        one_day_ago = now - timedelta(hours=24)

        for sender_id, events in list(self.sender_events.items()):
            self.sender_events[sender_id] = [ts for ts in events if ts >= one_minute_ago]

        for sender_id, events in list(self.sender_spam_events.items()):
            self.sender_spam_events[sender_id] = [ts for ts in events if ts >= one_day_ago]

        for key, events in list(self.sender_content_events.items()):
            recent = [ts for ts in events if ts >= one_day_ago]
            if recent:
                self.sender_content_events[key] = recent
            else:
                del self.sender_content_events[key]

    @staticmethod
    def _command(
        evt: ParsedCommentEvent,
        action: str,
        reply_text: str | None,
        intent: str | None,
        sentiment: str | None,
        status: str,
    ) -> dict:
        target_id = evt.comment_id or evt.event_id
        return {
            "command_id": str(uuid4()),
            "event_id": evt.event_id,
            "action": action,
            "target_id": target_id,
            "post_id": evt.post_id or "",
            "message": evt.message,
            "intent": intent,
            "sentiment": sentiment,
            "status": status,
            "reply_text": reply_text,
            "retry_count": 0,
            "created_at": timezone.now().isoformat(),
        }
