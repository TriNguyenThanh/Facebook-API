from django.test import TestCase
from unittest.mock import Mock, patch

from retry.management.commands.consume_send_failed import Command, next_retry_message


class RetryDecisionTests(TestCase):
    def test_republishes_when_under_max_retries(self):
        destination, outgoing, delay = next_retry_message({"command_id": "cmd-1", "retry_count": 1}, 5)

        self.assertEqual(destination, "send_retry")
        self.assertEqual(outgoing["retry_count"], 2)
        self.assertEqual(delay, 2.0)

    def test_dead_letters_when_exhausted(self):
        destination, outgoing, delay = next_retry_message({"command_id": "cmd-1", "retry_count": 5}, 5)

        self.assertEqual(destination, "dead_letter")
        self.assertEqual(outgoing["retry_count"], 5)
        self.assertEqual(delay, 0.0)

    @patch("retry.management.commands.consume_send_failed.time.sleep")
    def test_handle_failed_publishes_retry_with_backoff(self, mock_sleep):
        worker = Command()
        producer = Mock()

        worker._handle_failed({"command_id": "cmd-1", "retry_count": 1}, producer)

        mock_sleep.assert_called_once_with(2.0)
        producer.produce.assert_called_once()
        topic, = producer.produce.call_args.args
        payload = producer.produce.call_args.kwargs["value"]

        self.assertEqual(topic, "send_retry")
        self.assertIn(b'"retry_count":2', payload)

    @patch("retry.management.commands.consume_send_failed.time.sleep")
    def test_handle_failed_dead_letters_when_exhausted(self, mock_sleep):
        worker = Command()
        producer = Mock()

        worker._handle_failed({"command_id": "cmd-1", "retry_count": 5}, producer)

        mock_sleep.assert_not_called()
        producer.produce.assert_called_once()
        topic, = producer.produce.call_args.args

        self.assertEqual(topic, "dead_letter")

    @patch("retry.management.commands.consume_send_failed.time.sleep")
    def test_duplicate_failed_message_is_not_republished(self, _mock_sleep):
        worker = Command()
        producer = Mock()
        payload = {"command_id": "cmd-1", "retry_count": 1}

        worker._handle_failed(payload, producer)
        worker._handle_failed(payload, producer)

        producer.produce.assert_called_once()
