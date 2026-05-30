from __future__ import annotations

import json
import time
from typing import Any

from confluent_kafka import Consumer, Producer
from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def next_retry_message(message: dict[str, Any], max_retries: int) -> tuple[str, dict[str, Any], float]:
    retry_count = int(message.get("retry_count") or 0)
    if retry_count >= max_retries:
        dead = dict(message)
        dead["dead_lettered_at"] = timezone.now().isoformat()
        return "dead_letter", dead, 0.0

    retry = dict(message)
    retry["retry_count"] = retry_count + 1
    retry["retry_scheduled_at"] = timezone.now().isoformat()
    return "send_retry", retry, float(2**retry_count)


def retry_decision_log(command_id: str, retry_count: int, destination: str) -> str:
    return json.dumps(
        {
            "timestamp": timezone.now().isoformat(),
            "service": "retry-service",
            "level": "info",
            "message": "retry decision",
            "command_id": command_id,
            "retry_count": retry_count,
            "destination": destination,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )


class Command(BaseCommand):
    help = "Consume send_failed and republish to send_retry or dead_letter."

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.processed_retries: set[str] = set()

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
        consumer.subscribe([settings.KAFKA_SEND_FAILED_TOPIC])
        producer = Producer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})

        self.stdout.write(self.style.SUCCESS("Retry service consumer started"))
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
                    self._handle_failed(payload, producer)
                except Exception as exc:  # noqa: BLE001
                    self.stderr.write(f"retry processing failed: {exc}")
                finally:
                    consumer.commit(message=msg, asynchronous=False)
        except KeyboardInterrupt:
            self.stdout.write("Stopping retry service...")
        finally:
            producer.flush()
            consumer.close()

    def _handle_failed(self, payload: dict[str, Any], producer: Producer) -> None:
        command_id = str(payload.get("command_id") or "")
        retry_count = int(payload.get("retry_count") or 0)
        if not command_id:
            return

        destination, outgoing, delay = next_retry_message(payload, settings.MAX_RETRIES)
        idempotency_key = f"{command_id}:{retry_count}:{destination}"

        if idempotency_key in self.processed_retries:
            return

        if delay > 0:
            time.sleep(delay)

        topic = (
            settings.KAFKA_SEND_RETRY_TOPIC
            if destination == "send_retry"
            else settings.KAFKA_DEAD_LETTER_TOPIC
        )
        producer.produce(topic, value=_json_bytes(outgoing), key=command_id.encode("utf-8"))
        producer.poll(0)
        self.processed_retries.add(idempotency_key)
        self.stdout.write(retry_decision_log(command_id, retry_count, topic))
