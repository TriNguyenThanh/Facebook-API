from django.contrib import admin

from .models import FailedAction, IncomingEvent, SocialProfile


@admin.register(SocialProfile)
class SocialProfileAdmin(admin.ModelAdmin):
	list_display = ("social_id", "is_blacklisted", "spam_count_24h", "last_spam_at", "updated_at")
	search_fields = ("social_id",)
	list_filter = ("is_blacklisted",)


@admin.register(IncomingEvent)
class IncomingEventAdmin(admin.ModelAdmin):
	list_display = ("event_id", "sender", "status", "is_spam", "intent", "sentiment", "updated_at")
	search_fields = ("event_id", "sender__social_id", "content")
	list_filter = ("status", "is_spam", "intent", "sentiment")


@admin.register(FailedAction)
class FailedActionAdmin(admin.ModelAdmin):
	list_display = ("incoming_event", "action_type", "status", "attempts", "next_retry_at", "updated_at")
	list_filter = ("status", "action_type")
	search_fields = ("incoming_event__event_id", "incoming_event__sender__social_id")
