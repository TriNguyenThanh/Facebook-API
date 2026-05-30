from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ParsedCommentEvent:
    event_id: str
    event_type: str
    comment_id: str | None
    page_id: str | None
    sender_id: str
    sender_name: str | None
    message: str
    created_time: int | None
    permalink_url: str | None = None
    parent_id: str | None = None
    post_id: str | None = None
    timestamp: str | None = None


def _stable_event_id(*parts: str) -> str:
    raw = "|".join(p for p in parts if p)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _object_tail(value: str | None) -> str | None:
    if not value:
        return None
    return value.rsplit("_", 1)[-1]


def is_direct_post_comment(event: ParsedCommentEvent) -> bool:
    if event.page_id and event.sender_id == event.page_id:
        return False

    if not event.parent_id:
        return True

    if not event.post_id:
        return False

    return event.parent_id == event.post_id or _object_tail(event.parent_id) == _object_tail(event.post_id)


def extract_comment_events(payload: dict[str, Any]) -> list[ParsedCommentEvent]:
    """Extract feed comment events from a Facebook webhook payload.

    Scope MVP: entry[].changes[] where field == 'feed' and value.item == 'comment'.
    """

    if payload.get("event_id") and payload.get("sender_id") and payload.get("message"):
        event = ParsedCommentEvent(
            event_id=str(payload["event_id"]),
            event_type=str(payload.get("event_type") or "comment"),
            comment_id=str(payload.get("comment_id") or "") or None,
            page_id=str(payload.get("page_id") or "") or None,
            sender_id=str(payload["sender_id"]),
            sender_name=str(payload.get("sender_name") or "") or None,
            message=str(payload["message"]),
            created_time=payload.get("fb_created_time") if isinstance(payload.get("fb_created_time"), int) else None,
            permalink_url=str(payload.get("permalink_url") or "") or None,
            parent_id=str(payload.get("parent_id") or "") or None,
            post_id=str(payload.get("post_id") or "") or None,
            timestamp=str(payload.get("timestamp") or "") or None,
        )
        return [event] if is_direct_post_comment(event) else []

    events: list[ParsedCommentEvent] = []

    for entry in payload.get("entry", []) or []:
        page_id = str(entry.get("id")) if entry.get("id") is not None else None
        entry_time = entry.get("time")

        for change in entry.get("changes", []) or []:
            if change.get("field") != "feed":
                continue

            value = change.get("value") or {}
            if value.get("item") != "comment":
                continue

            message = (value.get("message") or "").strip()
            if not message:
                continue

            comment_id = value.get("comment_id") or value.get("post_id")
            if comment_id is not None:
                comment_id = str(comment_id)

            parent_id = value.get("parent_id")
            if parent_id is not None:
                parent_id = str(parent_id)

            post_obj = value.get("post") or {}
            if not isinstance(post_obj, dict):
                post_obj = {}

            post_id = value.get("post_id") or post_obj.get("id")
            if post_id is not None:
                post_id = str(post_id)

            from_obj = value.get("from") or {}
            if not isinstance(from_obj, dict):
                from_obj = {}

            sender_id = from_obj.get("id") or value.get("sender_id")
            if sender_id is None:
                # Can't process without a stable sender id
                continue
            sender_id = str(sender_id)

            sender_name = None
            if isinstance(from_obj, dict):
                name_val = from_obj.get("name")
                if name_val is not None:
                    sender_name = str(name_val)

            permalink_url = value.get("permalink_url") or post_obj.get("permalink_url")
            if permalink_url is not None:
                permalink_url = str(permalink_url)

            created_time = value.get("created_time")
            if created_time is None and isinstance(entry_time, int):
                created_time = entry_time

            if comment_id:
                event_id = comment_id
            else:
                event_id = _stable_event_id(
                    page_id or "",
                    str(entry_time or ""),
                    sender_id,
                    message,
                )

            event = ParsedCommentEvent(
                event_id=event_id,
                event_type="comment",
                comment_id=comment_id,
                page_id=page_id,
                sender_id=sender_id,
                sender_name=sender_name,
                message=message,
                created_time=created_time if isinstance(created_time, int) else None,
                permalink_url=permalink_url,
                parent_id=parent_id,
                post_id=post_id,
            )
            if is_direct_post_comment(event):
                events.append(event)

    return events
