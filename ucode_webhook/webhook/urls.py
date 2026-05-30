from django.urls import path

from .views import FacebookWebhookView, SubscribePageWebhookView
urlpatterns = [
    path("webhook", FacebookWebhookView.as_view(), name="webhook"),
    path("webhook/", FacebookWebhookView.as_view(), name="webhook-slash"),
    path("subscribe-page", SubscribePageWebhookView.as_view(), name="subscribe-page"),
]
