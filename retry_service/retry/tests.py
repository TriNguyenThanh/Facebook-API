from django.test import TestCase

from retry.management.commands.consume_send_failed import next_retry_message


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
