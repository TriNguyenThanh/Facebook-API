import hmac
import hashlib
import json
import logging
import requests
from django.conf import settings
from confluent_kafka import Producer
from typing import Any, cast
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

class FacebookSubscriptionError(Exception):
    pass

class FacebookSubscriptionService:
    """Register page webhook subscriptions on Facebook Graph API."""

    def __init__(self) -> None:
        self.version = settings.FACEBOOK_GRAPH_API_VERSION
        self.base_url = f"https://graph.facebook.com/{self.version}"
        self.access_token = settings.FACEBOOK_PAGE_ACCESS_TOKEN

    def subscribe_page_comment_events(self, page_id: str) -> dict[str, Any]:
        if not self.access_token:
            raise FacebookSubscriptionError("Missing FACEBOOK_PAGE_ACCESS_TOKEN in environment")

        params = {
            "access_token": self.access_token,
        }
        data = {
            "subscribed_fields": "feed",
        }

        query = urlencode(params)
        encoded_body = urlencode(data).encode("utf-8")
        url = f"{self.base_url}/{page_id}/subscribed_apps?{query}"
        request = Request(url=url, data=encoded_body, method="POST")

        try:
            with urlopen(request, timeout=30) as response:
                payload = response.read().decode("utf-8")
                return json.loads(payload) if payload else {"success": True}
        except HTTPError as exc:
            raw = exc.read().decode("utf-8")
            details = cast(dict[str, Any], json.loads(raw))
            message = details.get("error", {}).get("message", "Facebook subscription error")
            raise FacebookSubscriptionError(message) from exc
        except URLError as exc:
            raise FacebookSubscriptionError(f"Connection error: {exc.reason}") from exc


class FacebookWebhookService:
    def __init__(self):
        self.verify_token = getattr(settings, 'FACEBOOK_VERIFY_TOKEN', '')
        self.app_secret = getattr(settings, 'FACEBOOK_APP_SECRET', '')
        self.kafka_topic = getattr(settings, 'KAFKA_WEBHOOK_TOPIC', 'webhooks')
        self.raw_events_topic = getattr(settings, 'KAFKA_RAW_EVENTS_TOPIC', 'raw_events')
        self.publish_legacy = getattr(settings, 'KAFKA_PUBLISH_WEBHOOKS_TOPIC', False)
        
        # Initialize Kafka Producer
        bootstrap_servers = getattr(settings, 'KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers
        })

    def verify_webhook(self, mode, token, challenge):
        """Verifies the webhook subscription with Facebook."""
        if mode == 'subscribe' and token == self.verify_token:
            return True, challenge
        return False, None

    def verify_signature(self, payload_body: bytes, signature_header: str | None) -> bool:
        """Verifies the payload signature using the App Secret (X-Hub-Signature-256).

        Facebook format: sha256=<hex_digest>
        """
        if not self.app_secret:
            logger.warning("FACEBOOK_APP_SECRET is not set. Skipping signature verification.")
            return True

        if not signature_header:
            return False

        # Use partition to safely split on the FIRST '=' only.
        # split('=') would break if the digest itself ever contained '='.
        hash_algorithm, sep, signature = signature_header.partition('=')

        if sep != '=' or hash_algorithm != 'sha256' or not signature:
            return False

        expected_signature = hmac.new(
            self.app_secret.encode('utf-8'),
            payload_body,
            hashlib.sha256,
        ).hexdigest()

        return hmac.compare_digest(expected_signature, signature)

    def publish_event(self, payload):
        """Publishes the standardized payload to Kafka."""
        event_data = json.dumps(payload) if isinstance(payload, dict) else payload

        key = None
        if isinstance(payload, dict):
            try:
                entries = payload.get("entry") or []
                if entries and isinstance(entries, list) and isinstance(entries[0], dict):
                    key = str(entries[0].get("id") or "") or None
            except Exception:
                key = None
        
        try:
            topics = [self.raw_events_topic]
            if self.publish_legacy and self.kafka_topic not in topics:
                topics.append(self.kafka_topic)

            for topic in topics:
                self.producer.produce(
                    topic,
                    value=event_data.encode('utf-8'),
                    key=key.encode('utf-8') if key else None,
                )
            self.producer.poll(0) # Trigger delivery reports
            return True
        except Exception as e:
            logger.error(f"Error publishing to Kafka: {e}")
            return False

    def flush(self):
        """Wait for any outstanding messages to be delivered and delivery report callbacks to be triggered."""
        self.producer.flush()
