from django.test import override_settings
from rest_framework.test import APITestCase
from unittest.mock import patch
import hashlib
import hmac

from webhook.normalizers import normalize_facebook_webhook_events


class FacebookWebhookViewTests(APITestCase):
    @override_settings(FACEBOOK_APP_SECRET="")
    @patch("webhook.views.FacebookWebhookService.publish_event", return_value=True)
    @patch("webhook.views.FacebookWebhookService.flush", return_value=True)
    def test_webhook_post_valid_payload_returns_200(self, mock_flush, mock_publish):
        payload = {
            "object": "page",
            "entry": [
                {
                    "id": "1",
                    "time": 123,
                    "messaging": [
                        {
                            "sender": {"id": "user-1"},
                            "message": {"text": "hello"},
                        }
                    ],
                }
            ],
        }

        response = self.client.post("/api/webhook", payload, format="json")

        self.assertEqual(response.status_code, 200)
        mock_publish.assert_called_once()
        self.assertEqual(mock_publish.call_args.args[0]["event_type"], "message")
        self.assertEqual(mock_publish.call_args.args[0]["sender_id"], "user-1")
        mock_flush.assert_called_once()

    @override_settings(FACEBOOK_APP_SECRET="")
    def test_webhook_post_invalid_payload_returns_400(self):
        payload = {"object": "page"}

        response = self.client.post("/api/webhook", payload, format="json")

        self.assertEqual(response.status_code, 400)

    @override_settings(FACEBOOK_APP_SECRET="secret")
    def test_webhook_post_invalid_signature_returns_403(self):
        payload = b'{"object":"page","entry":[]}'
        response = self.client.post(
            "/api/webhook/",
            payload,
            content_type="application/json",
            HTTP_X_HUB_SIGNATURE_256="sha256=bad",
        )
        self.assertEqual(response.status_code, 403)

    @override_settings(FACEBOOK_APP_SECRET="secret")
    @patch("webhook.views.FacebookWebhookService.publish_event", return_value=True)
    @patch("webhook.views.FacebookWebhookService.flush", return_value=True)
    def test_webhook_post_valid_signature_publishes_normalized_comment(self, _mock_flush, mock_publish):
        payload = b'{"object":"page","entry":[{"id":"page-1","time":1710000000,"changes":[{"field":"feed","value":{"item":"comment","comment_id":"c-1","post_id":"p-1","from":{"id":"u-1","name":"User"},"message":"Gia bao nhieu?"}}]}]}'
        digest = hmac.new(b"secret", payload, hashlib.sha256).hexdigest()

        response = self.client.post(
            "/api/webhook/",
            payload,
            content_type="application/json",
            HTTP_X_HUB_SIGNATURE_256=f"sha256={digest}",
        )

        self.assertEqual(response.status_code, 200)
        event = mock_publish.call_args.args[0]
        self.assertEqual(event["event_id"], "c-1")
        self.assertEqual(event["event_type"], "comment")


class NormalizerTests(APITestCase):
    def test_normalize_comment_minimum_schema(self):
        events = normalize_facebook_webhook_events(
            {
                "object": "page",
                "entry": [
                    {
                        "id": "page-1",
                        "time": 1710000000,
                        "changes": [
                            {
                                "field": "feed",
                                "value": {
                                    "item": "comment",
                                    "comment_id": "c-1",
                                    "post_id": "p-1",
                                    "from": {"id": "u-1"},
                                    "message": "hello",
                                },
                            }
                        ],
                    }
                ],
            }
        )

        self.assertEqual(events[0]["event_id"], "c-1")
        self.assertEqual(events[0]["sender_id"], "u-1")
        self.assertEqual(events[0]["target_id"] if "target_id" in events[0] else events[0]["comment_id"], "c-1")

    def test_normalize_comments_field_without_item(self):
        events = normalize_facebook_webhook_events(
            {
                "object": "page",
                "entry": [
                    {
                        "id": "page-1",
                        "time": 1710000000,
                        "changes": [
                            {
                                "field": "comments",
                                "value": {
                                    "id": "c-2",
                                    "post_id": "p-1",
                                    "sender": {"id": "u-2", "name": "User 2"},
                                    "text": "hello from comments field",
                                },
                            }
                        ],
                    }
                ],
            }
        )

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["event_id"], "c-2")
        self.assertEqual(events[0]["event_type"], "comment")
        self.assertEqual(events[0]["sender_id"], "u-2")
        self.assertEqual(events[0]["message"], "hello from comments field")
