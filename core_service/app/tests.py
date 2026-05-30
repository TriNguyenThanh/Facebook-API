from django.test import TestCase
from django.utils import timezone

from app.management.commands.consume_raw_events import Command
from app.services.event_parser import ParsedCommentEvent, extract_comment_events
from app.services.rules import classify_rule_based


class EventParserTests(TestCase):
	def test_extract_comment_event(self):
		payload = {
			"object": "page",
			"entry": [
				{
					"id": "123",
					"time": 1710000000,
					"changes": [
						{
							"field": "feed",
							"value": {
								"item": "comment",
								"verb": "add",
								"comment_id": "c_1",
								"post_id": "p_1",
								"from": {"id": "u_1", "name": "Test"},
								"message": "Shop ơi giá bao nhiêu?",
								"created_time": 1710000000,
							},
						}
					],
				}
			],
		}

		events = extract_comment_events(payload)
		self.assertEqual(len(events), 1)
		self.assertEqual(events[0].event_id, "c_1")
		self.assertEqual(events[0].sender_id, "u_1")
		self.assertEqual(events[0].sender_name, "Test")
		self.assertEqual(events[0].page_id, "123")
		self.assertIsNone(events[0].permalink_url)

	def test_extract_permalink_from_post_object(self):
		payload = {
			"object": "page",
			"entry": [
				{
					"id": "1104315202765579",
					"time": 1779499232,
					"changes": [
						{
							"field": "feed",
							"value": {
								"from": {"id": "26719519847714238", "name": "Trí Nguyễn"},
								"post": {
									"permalink_url": "https://www.facebook.com/photo.php?fbid=122094631076602100&set=a.122094631088602100&type=3",
									"id": "1104315202765579_122094631076602100",
								},
								"message": "Ăn sáng chưa",
								"post_id": "1104315202765579_122094631076602100",
								"comment_id": "122094631076602100_891720030624206",
								"created_time": 1779499228,
								"item": "comment",
								"parent_id": "27216919881246213_122094631076602100",
								"verb": "add",
							},
						}
					],
				}
			],
		}

		events = extract_comment_events(payload)
		self.assertEqual(len(events), 1)
		evt = events[0]
		self.assertEqual(evt.event_id, "122094631076602100_891720030624206")
		self.assertEqual(evt.sender_id, "26719519847714238")
		self.assertEqual(evt.sender_name, "Trí Nguyễn")
		self.assertEqual(evt.page_id, "1104315202765579")
		self.assertEqual(
			evt.permalink_url,
			"https://www.facebook.com/photo.php?fbid=122094631076602100&set=a.122094631088602100&type=3",
		)

	def test_ignore_non_comment(self):
		payload = {
			"object": "page",
			"entry": [{"id": "123", "time": 1, "changes": [{"field": "feed", "value": {"item": "post"}}]}],
		}
		self.assertEqual(extract_comment_events(payload), [])

	def test_extract_normalized_event(self):
		events = extract_comment_events(
			{
				"event_id": "evt-1",
				"event_type": "comment",
				"sender_id": "u-1",
				"comment_id": "c-1",
				"message": "hello",
				"timestamp": "2026-05-23T00:00:00+00:00",
			}
		)
		self.assertEqual(len(events), 1)
		self.assertEqual(events[0].event_id, "evt-1")
		self.assertEqual(events[0].event_type, "comment")

	def test_ignore_nested_comment_reply(self):
		events = extract_comment_events(
			{
				"event_id": "evt-reply",
				"event_type": "comment",
				"page_id": "page-1",
				"sender_id": "u-1",
				"comment_id": "post-1_reply-1",
				"post_id": "page-1_post-1",
				"parent_id": "post-1_comment-1",
				"message": "reply to another comment",
			}
		)

		self.assertEqual(events, [])

	def test_ignore_page_self_comment(self):
		events = extract_comment_events(
			{
				"event_id": "evt-self",
				"event_type": "comment",
				"page_id": "page-1",
				"sender_id": "page-1",
				"comment_id": "post-1_comment-1",
				"post_id": "page-1_post-1",
				"parent_id": "page-1_post-1",
				"message": "thanks",
			}
		)

		self.assertEqual(events, [])


class RuleClassifierTests(TestCase):
	def test_classify_link_as_spam(self):
		c = classify_rule_based("click https://example.com")
		self.assertTrue(c.is_spam)

	def test_intent_ask_price(self):
		c = classify_rule_based("Shop ơi giá bao nhiêu?")
		self.assertFalse(c.is_spam)
		self.assertEqual(c.intent, "ask_service")


class CoreCommandTests(TestCase):
	class ProducerStub:
		def __init__(self):
			self.messages = []
			self.flush_count = 0

		def produce(self, topic, value, key=None):
			self.messages.append({"topic": topic, "value": value, "key": key})

		def flush(self):
			self.flush_count += 1

	def test_build_reply_command_for_price_question(self):
		evt = extract_comment_events(
			{
				"event_id": "evt-1",
				"event_type": "comment",
				"sender_id": "u-1",
				"comment_id": "c-1",
				"message": "Shop ơi giá bao nhiêu?",
			}
		)[0]

		command = Command()._build_reply_command(evt, timezone.now(), None)

		self.assertEqual(command["action"], "reply")
		self.assertEqual(command["target_id"], "c-1")
		self.assertEqual(command["event_id"], "evt-1")

	def test_build_reply_command_for_spam(self):
		evt = extract_comment_events(
			{
				"event_id": "evt-2",
				"event_type": "comment",
				"sender_id": "u-2",
				"comment_id": "c-2",
				"message": "click https://example.com",
			}
		)[0]

		command = Command()._build_reply_command(evt, timezone.now(), None)

		self.assertEqual(command["action"], "hide_comment")

	def test_process_event_skips_nested_comment_reply(self):
		evt = ParsedCommentEvent(
			event_id="evt-3",
			event_type="comment",
			sender_id="u-3",
			sender_name=None,
			comment_id="post-1_reply-1",
			page_id="page-1",
			post_id="page-1_post-1",
			parent_id="post-1_comment-1",
			message="nested reply",
			created_time=None,
		)
		producer = self.ProducerStub()

		Command()._process_event(evt, producer, None)

		self.assertEqual(producer.messages, [])

	def test_process_event_skips_page_self_comment(self):
		evt = ParsedCommentEvent(
			event_id="evt-4",
			event_type="comment",
			page_id="page-1",
			sender_id="page-1",
			sender_name=None,
			comment_id="post-1_comment-1",
			post_id="page-1_post-1",
			parent_id="page-1_post-1",
			message="self comment",
			created_time=None,
		)
		producer = self.ProducerStub()

		Command()._process_event(evt, producer, None)

		self.assertEqual(producer.messages, [])
