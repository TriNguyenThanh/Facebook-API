from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Any

import requests
from django.conf import settings

logger = logging.getLogger(__name__)


class FacebookGraphError(RuntimeError):
    def __init__(self, message: str, *, retryable: bool, status_code: int | None = None):
        super().__init__(message)
        self.retryable = retryable
        self.status_code = status_code


class CircuitBreaker:
    def __init__(self, threshold: int = 5, cooldown_seconds: int = 30):
        self.threshold = threshold
        self.cooldown_seconds = cooldown_seconds
        self.failures = 0
        self.opened_at: float | None = None

    def allow(self) -> bool:
        if self.opened_at is None:
            return True
        if time.time() - self.opened_at >= self.cooldown_seconds:
            self.failures = 0
            self.opened_at = None
            return True
        return False

    def success(self) -> None:
        self.failures = 0
        self.opened_at = None

    def failure(self) -> None:
        self.failures += 1
        if self.failures >= self.threshold:
            self.opened_at = time.time()


_breaker = CircuitBreaker()


@dataclass(frozen=True)
class FacebookConfig:
    graph_api_version: str
    page_access_token: str


class FacebookService:
    def __init__(self, config: FacebookConfig | None = None):
        self.config = config or FacebookConfig(
            graph_api_version=settings.FACEBOOK_GRAPH_API_VERSION,
            page_access_token=settings.FACEBOOK_PAGE_ACCESS_TOKEN,
        )
        self.base_url = f"https://graph.facebook.com/{self.config.graph_api_version}"

    def _request(self, method: str, endpoint: str, **kwargs) -> dict[str, Any]:
        if not self.config.page_access_token:
            raise FacebookGraphError("FACEBOOK_PAGE_ACCESS_TOKEN is not configured", retryable=False)
        if not _breaker.allow():
            raise FacebookGraphError("facebook circuit breaker is open", retryable=True)

        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        params = kwargs.pop("params", {}) or {}
        data = kwargs.pop("data", {}) or {}
        params.setdefault("access_token", self.config.page_access_token)

        try:
            response = requests.request(method, url, params=params, data=data, timeout=10, **kwargs)
        except requests.Timeout as exc:
            _breaker.failure()
            raise FacebookGraphError(f"Facebook timeout: {exc}", retryable=True) from exc
        except requests.RequestException as exc:
            _breaker.failure()
            raise FacebookGraphError(f"Facebook request failed: {exc}", retryable=True) from exc

        logger.info(
            "facebook_graph_request",
            extra={
                "service": "backend-api",
                "endpoint": endpoint,
                "method": method,
                "status_code": response.status_code,
                "payload_summary": json.dumps(data, ensure_ascii=False)[:300],
            },
        )

        if response.status_code >= 400:
            _breaker.failure()
            retryable = response.status_code >= 500 or response.status_code == 429
            raise FacebookGraphError(
                f"Facebook error {response.status_code}: {response.text}",
                retryable=retryable,
                status_code=response.status_code,
            )

        _breaker.success()
        return response.json() if response.content else {"success": True}

    def get_page_info(self, page_id: str, fields: str | None = None):
        fields = fields or "id,name,about,category,fan_count,followers_count,picture,cover,link,website"
        return self._request("GET", page_id, params={"fields": fields})

    def get_posts(self, page_id: str):
        return self._request("GET", f"{page_id}/feed")

    def create_post(self, page_id: str, message: str | None = None, **kwargs):
        payload = dict(kwargs)
        if message:
            payload["message"] = message
        return self._request("POST", f"{page_id}/feed", data=payload)

    def delete_post(self, post_id: str):
        return self._request("DELETE", post_id)

    def get_comments(self, post_id: str):
        return self._request("GET", f"{post_id}/comments")

    def get_likes(self, post_id: str):
        return self._request("GET", f"{post_id}/likes", params={"summary": "true"})

    def get_insights(self, page_id: str, metric: str | None = None):
        metric = metric or "page_media_view,post_media_view,post_total_media_view_unique"
        return self._request("GET", f"{page_id}/insights", params={"metric": metric})

    def hide_comment(self, comment_id: str) -> None:
        if not settings.FACEBOOK_ACTIONS_ENABLED:
            return
        self._request("POST", comment_id, data={"is_hidden": "true"})

    def reply_to_comment(self, comment_id: str, message: str) -> None:
        if not settings.FACEBOOK_ACTIONS_ENABLED:
            return
        self._request("POST", f"{comment_id}/comments", data={"message": message})


def execute_reply_command(command: dict[str, Any], service: FacebookService | None = None) -> None:
    action = command.get("action")
    target_id = command.get("target_id")
    fb = service or FacebookService()

    if action in {"no_action", "queue_for_review"}:
        return
    if not target_id:
        raise FacebookGraphError("missing target_id", retryable=False)
    if action == "hide_comment":
        fb.hide_comment(str(target_id))
        return
    if action == "reply":
        reply_text = command.get("reply_text")
        if not reply_text:
            raise FacebookGraphError("missing reply_text", retryable=False)
        fb.reply_to_comment(str(target_id), str(reply_text))
        return

    raise FacebookGraphError(f"unsupported action: {action}", retryable=False)
