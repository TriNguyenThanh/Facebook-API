from __future__ import annotations

import json
import time
from typing import Any

from confluent_kafka import Consumer, Producer
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import IntegrityError, transaction
from django.utils import timezone

from api.models import Comment, IdempotencyKey
from api.services import FacebookGraphError, execute_reply_command


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


class Command(BaseCommand):
    help = "Consume reply_commands/send_retry and execute Facebook actions idempotently."

    def add_arguments(self, parser):
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
        consumer.subscribe([settings.KAFKA_REPLY_COMMANDS_TOPIC, settings.KAFKA_SEND_RETRY_TOPIC])
        producer = Producer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})

        self.stdout.write(self.style.SUCCESS("Backend command consumer started"))
        try:
            while True:
                msg = consumer.poll(opts["poll"])
                if msg is None:
                    continue
                if msg.error():
                    self.stderr.write(str(msg.error()))
                    continue

                try:
                    command = json.loads((msg.value() or b"{}").decode("utf-8"))
                    self._handle_command(command, producer)
                except Exception as exc:  # noqa: BLE001
                    self.stderr.write(f"command processing failed: {exc}")
                finally:
                    consumer.commit(message=msg, asynchronous=False)
        except KeyboardInterrupt:
            self.stdout.write("Stopping backend command consumer...")
        finally:
            producer.flush()
            consumer.close()

    def _handle_command(self, command: dict[str, Any], producer: Producer) -> None:
        command_id = str(command.get("command_id") or "")
        if not command_id:
            return
        if IdempotencyKey.objects.filter(command_id=command_id).exists():
            return

        self._record_comment(command, "received")

        try:
            execute_reply_command(command)
        except FacebookGraphError as exc:
            if exc.retryable:
                failed = dict(command)
                failed["retry_count"] = int(failed.get("retry_count") or 0)
                failed["error"] = str(exc)
                failed["failed_at"] = timezone.now().isoformat()
                producer.produce(
                    settings.KAFKA_SEND_FAILED_TOPIC,
                    value=_json_bytes(failed),
                    key=command_id.encode("utf-8"),
                )
                producer.poll(0)
            else:
                self.stderr.write(f"Non-retryable command failure {command_id}: {exc}")
                self._record_comment(command, "failed")
            return

        try:
            with transaction.atomic():
                IdempotencyKey.objects.create(
                    command_id=command_id,
                    status="success",
                )
                self._record_comment(command, str(command.get("status") or "processed"))
        except IntegrityError:
            return

    @staticmethod
    def _record_comment(command: dict[str, Any], status: str) -> None:
        comment_id = str(command.get("target_id") or command.get("event_id") or "")
        if not comment_id:
            return

        Comment.objects.update_or_create(
            comment_id=comment_id[:100],
            defaults={
                "post_id": str(command.get("post_id") or "")[:100],
                "message": command.get("message") or "",
                "intent": command.get("intent") or None,
                "sentiment": command.get("sentiment") or None,
                "status": status[:20],
            },
        )
