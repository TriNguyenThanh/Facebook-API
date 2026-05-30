from rest_framework import serializers


class FacebookFromSerializer(serializers.Serializer):
    id = serializers.CharField(required=False, allow_blank=True)
    name = serializers.CharField(required=False, allow_blank=True)


class FacebookFeedCommentValueMinimalSerializer(serializers.Serializer):
    item = serializers.CharField(required=False, allow_blank=True)
    verb = serializers.CharField(required=False, allow_blank=True)
    message = serializers.CharField(required=False, allow_blank=True)
    post_id = serializers.CharField(required=False, allow_blank=True)
    comment_id = serializers.CharField(required=False, allow_blank=True)
    parent_id = serializers.CharField(required=False, allow_blank=True)
    created_time = serializers.IntegerField(required=False)
    permalink_url = serializers.URLField(required=False, allow_blank=True)
    from_field = FacebookFromSerializer(required=False, source="from")


class FacebookFeedChangeMinimalSerializer(serializers.Serializer):
    field = serializers.CharField(required=False, allow_blank=True)
    value = FacebookFeedCommentValueMinimalSerializer(required=False)


class FacebookWebhookEntryMinimalSerializer(serializers.Serializer):
    id = serializers.CharField(required=False, allow_blank=True)
    time = serializers.IntegerField(required=False)
    changes = FacebookFeedChangeMinimalSerializer(many=True, required=False)


class FacebookWebhookEntrySerializer(serializers.Serializer):
    id = serializers.CharField(required=False, allow_blank=True)
    time = serializers.IntegerField(required=False)
    messaging = serializers.ListField(child=serializers.JSONField(), required=False)
    changes = serializers.ListField(child=serializers.JSONField(), required=False)
    standby = serializers.ListField(child=serializers.JSONField(), required=False)


class FacebookWebhookPayloadSerializer(serializers.Serializer):
    object = serializers.CharField()
    entry = FacebookWebhookEntrySerializer(many=True)


class FacebookWebhookPayloadMinimalSerializer(serializers.Serializer):
    schema = serializers.CharField(required=False, allow_blank=True)
    object = serializers.CharField(required=False, allow_blank=True)
    entry = FacebookWebhookEntryMinimalSerializer(many=True)
