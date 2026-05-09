from rest_framework import serializers


class FacebookWebhookEntrySerializer(serializers.Serializer):
    id = serializers.CharField(required=False, allow_blank=True)
    time = serializers.IntegerField(required=False)
    messaging = serializers.ListField(child=serializers.JSONField(), required=False)
    changes = serializers.ListField(child=serializers.JSONField(), required=False)
    standby = serializers.ListField(child=serializers.JSONField(), required=False)


class FacebookWebhookPayloadSerializer(serializers.Serializer):
    object = serializers.CharField()
    entry = FacebookWebhookEntrySerializer(many=True)
