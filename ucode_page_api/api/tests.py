from django.test import TestCase, override_settings
from unittest.mock import Mock, patch

from api.management.commands.consume_reply_commands import Command
from api.models import Comment, IdempotencyKey
from api.services import FacebookGraphError, execute_reply_command


class BackendCommandTests(TestCase):
    @override_settings(FACEBOOK_ACTIONS_ENABLED=True)
    def test_execute_reply_command_calls_reply(self):
        service = Mock()
        execute_reply_command(
            {
                "action": "reply",
                "target_id": "comment-1",
                "reply_text": "hello",
            },
            service=service,
        )
        service.reply_to_comment.assert_called_once_with("comment-1", "hello")

    @patch("api.management.commands.consume_reply_commands.execute_reply_command")
    def test_idempotency_skips_duplicate_command(self, mock_execute):
        command = {
            "command_id": "cmd-1",
            "action": "no_action",
            "target_id": "comment-1",
        }
        worker = Command()
        producer = Mock()

        worker._handle_command(command, producer)
        worker._handle_command(command, producer)

        self.assertEqual(mock_execute.call_count, 1)
        self.assertTrue(IdempotencyKey.objects.filter(command_id="cmd-1").exists())

    @patch("api.management.commands.consume_reply_commands.execute_reply_command")
    def test_retryable_error_publishes_send_failed(self, mock_execute):
        mock_execute.side_effect = FacebookGraphError("timeout", retryable=True)
        worker = Command()
        producer = Mock()

        worker._handle_command({"command_id": "cmd-2", "action": "reply"}, producer)

        producer.produce.assert_called_once()
        self.assertFalse(IdempotencyKey.objects.filter(command_id="cmd-2").exists())

    @patch("api.management.commands.consume_reply_commands.execute_reply_command")
    def test_comment_history_is_recorded(self, _mock_execute):
        worker = Command()
        producer = Mock()

        worker._handle_command(
            {
                "command_id": "cmd-3",
                "event_id": "evt-3",
                "action": "reply",
                "target_id": "comment-3",
                "post_id": "post-3",
                "message": "Shop oi gia bao nhieu?",
                "intent": "ask_service",
                "sentiment": "neutral",
                "status": "processed",
            },
            producer,
        )

        comment = Comment.objects.get(comment_id="comment-3")
        self.assertEqual(comment.post_id, "post-3")
        self.assertEqual(comment.intent, "ask_service")
        self.assertEqual(comment.status, "processed")
