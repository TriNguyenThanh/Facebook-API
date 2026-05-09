from __future__ import annotations

from dataclasses import dataclass

import requests


@dataclass(frozen=True)
class FacebookConfig:
    graph_api_version: str
    page_access_token: str


class FacebookClient:
    def __init__(self, config: FacebookConfig):
        self._config = config

    def _url(self, path: str) -> str:
        path = path.lstrip("/")
        return f"https://graph.facebook.com/{self._config.graph_api_version}/{path}"

    def hide_comment(self, comment_id: str) -> None:
        if not self._config.page_access_token:
            raise RuntimeError("FACEBOOK_PAGE_ACCESS_TOKEN is not set")

        resp = requests.post(
            self._url(comment_id),
            data={"is_hidden": "true", "access_token": self._config.page_access_token},
            timeout=10,
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"hide_comment failed: {resp.status_code} {resp.text}")

    def reply_to_comment(self, comment_id: str, message: str) -> None:
        if not self._config.page_access_token:
            raise RuntimeError("FACEBOOK_PAGE_ACCESS_TOKEN is not set")

        resp = requests.post(
            self._url(f"{comment_id}/comments"),
            data={"message": message, "access_token": self._config.page_access_token},
            timeout=10,
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"reply_to_comment failed: {resp.status_code} {resp.text}")
