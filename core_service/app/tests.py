from django.test import TestCase

from app.services.event_parser import extract_comment_events
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
		self.assertEqual(events[0].page_id, "123")

	def test_ignore_non_comment(self):
		payload = {
			"object": "page",
			"entry": [{"id": "123", "time": 1, "changes": [{"field": "feed", "value": {"item": "post"}}]}],
		}
		self.assertEqual(extract_comment_events(payload), [])


class RuleClassifierTests(TestCase):
	def test_classify_link_as_spam(self):
		c = classify_rule_based("click https://example.com")
		self.assertTrue(c.is_spam)

	def test_intent_ask_price(self):
		c = classify_rule_based("Shop ơi giá bao nhiêu?")
		self.assertFalse(c.is_spam)
		self.assertEqual(c.intent, "ask_price")
