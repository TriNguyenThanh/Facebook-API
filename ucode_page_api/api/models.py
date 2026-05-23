from django.db import models


class IdempotencyKey(models.Model):
    command_id = models.CharField(max_length=100, primary_key=True)
    processed_at = models.DateTimeField(auto_now_add=True)
    status = models.CharField(max_length=20)

    class Meta:
        db_table = "idempotency_keys"


class Comment(models.Model):
    comment_id = models.CharField(max_length=100, unique=True)
    post_id = models.CharField(max_length=100)
    message = models.TextField(blank=True, null=True)
    intent = models.CharField(max_length=50, blank=True, null=True)
    sentiment = models.CharField(max_length=20, blank=True, null=True)
    status = models.CharField(max_length=20, default="received")
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "comments"
