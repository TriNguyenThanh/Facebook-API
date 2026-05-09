from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ParsedCommentEvent:
    event_id: str
    comment_id: str | None
    page_id: str | None
    sender_id: str
    message: str
    created_time: int | None
    parent_id: str | None = None
    post_id: str | None = None


def _stable_event_id(*parts: str) -> str:
    raw = "|".join(p for p in parts if p)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def extract_comment_events(payload: dict[str, Any]) -> list[ParsedCommentEvent]:
    """Extract feed comment events from a Facebook webhook payload.

    Scope MVP: entry[].changes[] where field == 'feed' and value.item == 'comment'.
    """

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

            post_id = value.get("post_id")
            if post_id is not None:
                post_id = str(post_id)

            from_obj = value.get("from") or {}
            sender_id = from_obj.get("id") or value.get("sender_id")
            if sender_id is None:
                # Can't process without a stable sender id
                continue
            sender_id = str(sender_id)

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

            events.append(
                ParsedCommentEvent(
                    event_id=event_id,
                    comment_id=comment_id,
                    page_id=page_id,
                    sender_id=sender_id,
                    message=message,
                    created_time=created_time if isinstance(created_time, int) else None,
                    parent_id=parent_id,
                    post_id=post_id,
                )
            )

    return events
