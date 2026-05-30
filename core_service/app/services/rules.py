from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass


_LINK_RE = re.compile(r"(https?://|www\.)", re.IGNORECASE)


def normalize_text(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def content_hash(text: str) -> str:
    norm = normalize_text(text)
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class Classification:
    is_spam: bool
    spam_reason: str
    intent: str | None
    sentiment: str | None


def classify_rule_based(message: str) -> Classification:
    msg = message.strip()
    norm = normalize_text(msg)

    # Spam rules (MVP)
    if _LINK_RE.search(msg):
        return Classification(True, "contains_link", None, None)

    scam_keywords = ["kiếm tiền", "đầu tư", "nhận thưởng", "click", "telegram", "zalo.me", "bit.ly"]
    if any(k in norm for k in scam_keywords):
        return Classification(True, "scam_keywords", None, None)

    # Intent rules (IT Page)
    intent: str | None = None
    if any(k in norm for k in ["code", "bug", "lỗi", "cài", "cấu hình", "fix", "support", "hỗ trợ"]):
        intent = "tech_support"
    elif any(k in norm for k in ["giá", "khóa học", "dịch vụ", "bao nhiêu", "thuê", "tư vấn", "price"]):
        intent = "ask_service"
    elif any(k in norm for k in ["cảm ơn", "thanks", "tốt", "hay", "xịn", "tuyệt", "đỉnh", "ok"]):
        intent = "praise"

    # Sentiment rules (MVP)
    sentiment: str | None = None
    if any(k in norm for k in ["tệ", "bực", "lỗi", "khiếu nại", "không", "chưa nhận", "fail"]):
        sentiment = "negative"
    elif any(k in norm for k in ["tốt", "hay", "tuyệt", "cảm ơn", "love", "xịn"]):
        sentiment = "positive"
    else:
        sentiment = "neutral"

    return Classification(False, "", intent, sentiment)


def reply_template(intent: str | None) -> str | None:
    if intent == "ask_service":
        return "Dạ sếp cho em xin thêm thông tin yêu cầu cụ thể để team báo giá chuẩn đét nhé!"
    if intent == "tech_support":
        return "Dạ em nghe thấy mùi bug đâu đây. Sếp quăng cho em cái log hoặc mô tả kỹ hơn để em debug nhé!"
    if intent == "praise":
        return "Dạ cảm ơn sếp! Team nghe khen xong mà code mượt hẳn lên, bug tự dưng rụng hết!"
    return None
