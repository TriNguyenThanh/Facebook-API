from django.db import models


class EventStatus(models.TextChoices):
    RECEIVED = 'received', 'Received'
    PROCESSED = 'processed', 'Processed'
    REPLIED = 'replied', 'Replied'
    FAILED = 'failed', 'Failed'
    REVIEW_PENDING = 'review_pending', 'Review Pending'

class SocialProfile(models.Model):
    """Lưu trữ thông tin người dùng Facebook/Zalo tương tác"""
    social_id = models.CharField(max_length=100, unique=True, db_index=True)
    is_blacklisted = models.BooleanField(default=False)
    spam_count_24h = models.IntegerField(default=0)
    last_spam_at = models.DateTimeField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

class IncomingEvent(models.Model):
    """Tracking từng sự kiện (comment/message)"""
    event_id = models.CharField(max_length=255, unique=True, db_index=True)
    sender = models.ForeignKey(SocialProfile, on_delete=models.CASCADE)
    content = models.TextField()

    # For dedupe / spam heuristics
    content_hash = models.CharField(max_length=64, db_index=True, blank=True, default="")

    # Raw payload for traceability
    raw_payload = models.JSONField(null=True, blank=True)
    
    # Kết quả phân tích AI
    intent = models.CharField(max_length=50, null=True, blank=True)
    sentiment = models.CharField(max_length=50, null=True, blank=True)
    is_spam = models.BooleanField(default=False)
    
    # Tracking trạng thái
    status = models.CharField(
        max_length=20, 
        choices=EventStatus.choices, 
        default=EventStatus.RECEIVED
    )
    error_message = models.TextField(null=True, blank=True) # Ghi nhận lý do fail
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)


class FailedActionStatus(models.TextChoices):
    PENDING = "pending", "Pending"
    SUCCEEDED = "succeeded", "Succeeded"
    ABANDONED = "abandoned", "Abandoned"


class FailedAction(models.Model):
    """Retry queue persisted in DB (avoids hot-looping Kafka for delayed retries)."""

    incoming_event = models.ForeignKey(
        IncomingEvent, on_delete=models.CASCADE, related_name="failed_actions"
    )

    action_type = models.CharField(max_length=50)
    action_payload = models.JSONField(default=dict)

    attempts = models.IntegerField(default=0)
    max_attempts = models.IntegerField(default=5)

    next_retry_at = models.DateTimeField(db_index=True)
    status = models.CharField(
        max_length=20, choices=FailedActionStatus.choices, default=FailedActionStatus.PENDING
    )
    last_error = models.TextField(blank=True, default="")

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)