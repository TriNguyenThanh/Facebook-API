from __future__ import annotations

import json
from typing import Any

from confluent_kafka import Producer


def json_dumps(data: Any) -> bytes:
    return json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


class KafkaJsonProducer:
    def __init__(self, bootstrap_servers: str):
        self._producer = Producer({"bootstrap.servers": bootstrap_servers})

    def produce(self, topic: str, value: Any, key: str | None = None) -> None:
        self._producer.produce(topic, value=json_dumps(value), key=key.encode("utf-8") if key else None)

    def flush(self, timeout: float = 5.0) -> None:
        self._producer.flush(timeout)
