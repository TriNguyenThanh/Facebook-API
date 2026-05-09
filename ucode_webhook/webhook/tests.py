from django.test import override_settings
from rest_framework.test import APITestCase
from unittest.mock import patch


class FacebookWebhookViewTests(APITestCase):
    @override_settings(FACEBOOK_APP_SECRET="")
    @patch("webhook.views.FacebookWebhookService.publish_event", return_value=True)
    @patch("webhook.views.FacebookWebhookService.flush")
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
        mock_flush.assert_called_once()

    @override_settings(FACEBOOK_APP_SECRET="")
    def test_webhook_post_invalid_payload_returns_400(self):
        payload = {"object": "page"}

        response = self.client.post("/api/webhook", payload, format="json")

        self.assertEqual(response.status_code, 400)
