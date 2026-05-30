"""AI-powered comment classifier using Google Gemini.

Design goals:
- Prompt returns structured JSON to make parsing reliable and retries easy.
- Rule-based spam detection still runs first (cheap, no API cost).
- On any Gemini failure we gracefully fall back to rule-based intent + template reply.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass

from django.conf import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data contract
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AIClassification:
    intent: str | None
    sentiment: str | None
    reply: str | None          # AI-generated natural reply (None if no reply needed)
    raw_response: str | None   # Raw JSON string from Gemini, for debugging


# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """\
You are a comment analysis assistant for a Vietnamese Information Technology (IT) Facebook page.

Task: Read the user's comment and return a strict JSON object with the exact structure below.

JSON STRUCTURE:
{
  "intent": "<value>",
  "sentiment": "<value>",
  "reply": "<value or null>"
}

RULES:
- "intent": Choose EXACTLY ONE of: "ask_service", "tech_support", "praise", "inquiry", "other", null
    ask_service  = asking for prices, IT services, consulting, hiring, or courses
    tech_support = reporting bugs, asking for technical help, configuration issues
    praise       = praising, thanking, expressing satisfaction
    inquiry      = general tech questions, IT news discussion, non-pricing questions
    other        = none of the above
    null         = not enough information to classify
- "sentiment": Choose EXACTLY ONE of: "positive", "negative", "neutral"
- "reply": A natural, humorous, and witty reply FROM THE PAGE IN VIETNAMESE.
    Use IT/developer humor and jokes where appropriate (e.g., mentioning bugs, features, coding, coffee). Keep it short (1-3 sentences).
    Always start with the word "Dạ" and consider calling the customer "sếp" or "bạn" in a friendly, funny way.
    Return null if the comment requires no reply (e.g., meaningless text, just emojis).

RETURN ONLY JSON. No additional explanations.
"""


# ---------------------------------------------------------------------------
# Classifier
# ---------------------------------------------------------------------------

class DifyClassifier:
    """Calls Dify to classify intent/sentiment and generate a natural reply.

    Args:
        api_key: Dify Application API key. Falls back to settings.DIFY_API_KEY.
        base_url: Dify API Base URL. Falls back to settings.DIFY_BASE_URL.
        max_retries: Number of retry attempts on transient errors.
        retry_delay: Base delay in seconds between retries (doubles each time).
    """

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ) -> None:
        import requests
        self._requests = requests

        self._api_key = api_key or getattr(settings, "DIFY_API_KEY", "")
        if not self._api_key:
            raise RuntimeError("DIFY_API_KEY is not configured")

        self._base_url = base_url or getattr(settings, "DIFY_BASE_URL", "https://api.dify.ai/v1")
        self._endpoint = f"{self._base_url.rstrip('/')}/chat-messages"

        self._max_retries = max_retries
        self._retry_delay = retry_delay

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def classify(self, message: str) -> AIClassification:
        """Classify a comment message using Dify.

        Returns AIClassification. On failure after all retries, returns a
        safe default so the caller can fall back to rule-based logic.
        """
        # Inject the system prompt into the query to ensure Dify returns JSON
        # Even if the Dify app has its own prompt, this forces it to adhere
        # to our strict output requirements.
        query = f"{_SYSTEM_PROMPT}\n\nComment: {message}"
        raw: str | None = None

        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json"
        }

        payload = {
            "inputs": {},
            "query": query,
            "response_mode": "blocking",
            "conversation_id": "",
            "user": "core-service"
        }

        for attempt in range(1, self._max_retries + 1):
            try:
                response = self._requests.post(
                    self._endpoint,
                    json=payload,
                    headers=headers,
                    timeout=30
                )
                response.raise_for_status()
                response_json = response.json()
                
                # Dify blocking response contains the text in 'answer'
                raw = response_json.get("answer", "").strip()
                return self._parse(raw)
                
            except self._requests.RequestException as exc:
                logger.warning(
                    "DifyClassifier: Network error (attempt %d/%d): %s",
                    attempt, self._max_retries, exc
                )
            except json.JSONDecodeError as exc:
                logger.warning(
                    "DifyClassifier: JSON parse error (attempt %d/%d): %s | raw=%r",
                    attempt, self._max_retries, exc, raw,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "DifyClassifier: API error (attempt %d/%d): %s",
                    attempt, self._max_retries, exc,
                )

            if attempt < self._max_retries:
                time.sleep(self._retry_delay * (2 ** (attempt - 1)))

        # All retries exhausted — return safe fallback
        logger.error("DifyClassifier: all %d retries failed for message=%r", self._max_retries, message)
        return AIClassification(intent=None, sentiment="neutral", reply=None, raw_response=raw)

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _parse(raw: str) -> AIClassification:
        """Parse Dify JSON response into AIClassification."""
        # Extract json object from possible markdown or preamble text
        start_idx = raw.find('{')
        end_idx = raw.rfind('}')
        
        if start_idx != -1 and end_idx != -1 and end_idx >= start_idx:
            json_str = raw[start_idx:end_idx + 1]
        else:
            json_str = raw

        data = json.loads(json_str)

        intent = data.get("intent") or None
        sentiment = data.get("sentiment") or "neutral"
        reply = data.get("reply") or None

        # Normalise: strip whitespace, enforce allowed values
        _valid_intents = {"ask_service", "tech_support", "praise", "inquiry", "other"}
        if intent and intent not in _valid_intents:
            logger.warning("DifyClassifier: unknown intent %r — treating as None", intent)
            intent = None

        _valid_sentiments = {"positive", "negative", "neutral"}
        if sentiment not in _valid_sentiments:
            sentiment = "neutral"

        return AIClassification(
            intent=intent,
            sentiment=sentiment,
            reply=reply,
            raw_response=raw,
        )
