from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4


_SCHEMA = "fb.webhook.page.feed.min.v1"
_COMMENT_FIELDS = {"feed", "comment", "comments"}


def _iso_from_timestamp(value: Any) -> str:
    if isinstance(value, int | float):
        if value > 10_000_000_000:
            value = value / 1000
        return datetime.fromtimestamp(value, tz=UTC).isoformat()
    if isinstance(value, str) and value:
        return value
    return datetime.now(tz=UTC).isoformat()


def _stable_event_id(*parts: str) -> str:
    raw = "|".join(part for part in parts if part)
    if not raw:
        return str(uuid4())
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _first_text(*values: Any) -> str:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _dict_or_empty(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _is_comment_change(field: Any, value: dict[str, Any]) -> bool:
    field_name = str(field or "")
    item = str(value.get("item") or "")
    return field_name in _COMMENT_FIELDS and (not item or item == "comment" or bool(value.get("comment_id")))


def normalize_facebook_webhook_events(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Convert Facebook webhook payloads into the internal Kafka event schema."""

    events: list[dict[str, Any]] = []

    for entry in payload.get("entry", []) or []:
        if not isinstance(entry, dict):
            continue

        page_id = str(entry.get("id")) if entry.get("id") is not None else ""
        entry_time = entry.get("time")

        for change in entry.get("changes", []) or []:
            if not isinstance(change, dict):
                continue

            value = change.get("value") or {}
            if not isinstance(value, dict) or not _is_comment_change(change.get("field"), value):
                continue

            message_obj = _dict_or_empty(value.get("message"))
            message = _first_text(
                value.get("message"),
                value.get("text"),
                value.get("comment"),
                message_obj.get("text"),
            )
            if not message:
                continue

            from_obj = _dict_or_empty(value.get("from"))
            sender_obj = _dict_or_empty(value.get("sender"))
            author_obj = _dict_or_empty(value.get("author"))

            sender_id = (
                from_obj.get("id")
                or sender_obj.get("id")
                or author_obj.get("id")
                or value.get("sender_id")
                or value.get("user_id")
            )
            if sender_id is None:
                continue
            sender_id = str(sender_id)

            post_obj = _dict_or_empty(value.get("post"))

            comment_id = value.get("comment_id") or value.get("commentId") or value.get("id")
            post_id = value.get("post_id") or post_obj.get("id")
            created_time = value.get("created_time") or entry_time

            event_id = str(comment_id) if comment_id else _stable_event_id(
                page_id,
                str(created_time or ""),
                sender_id,
                message,
            )

            events.append(
                {
                    "event_id": event_id,
                    "event_type": "comment",
                    "sender_id": sender_id,
                    "sender_name": str(from_obj.get("name") or sender_obj.get("name") or author_obj.get("name") or ""),
                    "page_id": page_id,
                    "post_id": str(post_id or ""),
                    "comment_id": str(comment_id or ""),
                    "parent_id": str(value.get("parent_id") or ""),
                    "message": message,
                    "timestamp": _iso_from_timestamp(created_time),
                    "fb_created_time": created_time if isinstance(created_time, int) else None,
                    "permalink_url": str(value.get("permalink_url") or post_obj.get("permalink_url") or ""),
                }
            )

        for message_event in entry.get("messaging", []) or []:
            if not isinstance(message_event, dict):
                continue

            sender = message_event.get("sender") or {}
            recipient = message_event.get("recipient") or {}
            message_obj = message_event.get("message") or {}
            if not isinstance(sender, dict) or not isinstance(message_obj, dict):
                continue

            text = str(message_obj.get("text") or "").strip()
            sender_id = sender.get("id")
            if not text or sender_id is None:
                continue

            mid = message_obj.get("mid")
            timestamp = message_event.get("timestamp") or entry_time
            event_id = str(mid) if mid else _stable_event_id(
                page_id,
                str(timestamp or ""),
                str(sender_id),
                text,
            )

            events.append(
                {
                    "event_id": event_id,
                    "event_type": "message",
                    "sender_id": str(sender_id),
                    "sender_name": "",
                    "page_id": str(recipient.get("id") or page_id),
                    "post_id": "",
                    "comment_id": str(mid or ""),
                    "parent_id": "",
                    "message": text,
                    "timestamp": _iso_from_timestamp(timestamp),
                    "fb_created_time": timestamp if isinstance(timestamp, int) else None,
                    "permalink_url": "",
                }
            )

    return events


def minimize_facebook_page_webhook_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Minimize a Facebook Page webhook payload.

    Keeps only the subset of fields required by the downstream pipeline:
    - entry.id, entry.time
    - changes[].field == 'feed'
    - changes[].value for item == 'comment'

    The output keeps the original envelope shape (object/entry/changes) to remain
    compatible with existing consumers, while dropping large/unneeded fields.
    """

    entries_out: list[dict[str, Any]] = []

    for entry in payload.get("entry", []) or []:
        if not isinstance(entry, dict):
            continue

        entry_id = entry.get("id")
        entry_time = entry.get("time")

        changes_out: list[dict[str, Any]] = []
        for change in entry.get("changes", []) or []:
            if not isinstance(change, dict):
                continue

            if change.get("field") != "feed":
                continue

            value = change.get("value") or {}
            if not isinstance(value, dict):
                continue

            if value.get("item") != "comment":
                continue

            from_obj = value.get("from") or {}
            if not isinstance(from_obj, dict):
                from_obj = {}

            post_obj = value.get("post") or {}
            if not isinstance(post_obj, dict):
                post_obj = {}

            permalink_url = value.get("permalink_url") or post_obj.get("permalink_url")

            # Facebook sometimes provides post id inside value.post.id.
            post_id = value.get("post_id") or post_obj.get("id")

            minimal_value: dict[str, Any] = {
                "item": value.get("item"),
                "verb": value.get("verb"),
                "message": value.get("message"),
                "post_id": post_id,
                "comment_id": value.get("comment_id"),
                "parent_id": value.get("parent_id"),
                "created_time": value.get("created_time"),
                "from": {
                    "id": from_obj.get("id"),
                    "name": from_obj.get("name"),
                },
            }

            if permalink_url:
                minimal_value["permalink_url"] = permalink_url

            changes_out.append({"field": "feed", "value": minimal_value})

        entries_out.append({"id": entry_id, "time": entry_time, "changes": changes_out})

    return {
        "schema": _SCHEMA,
        "object": payload.get("object"),
        "entry": entries_out,
    }
