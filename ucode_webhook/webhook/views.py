from django.http import HttpResponse
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from drf_yasg.utils import swagger_auto_schema
from drf_yasg import openapi

from core.settings import PAGE_ID

from .services import FacebookWebhookService, FacebookSubscriptionService, FacebookSubscriptionError
from .serializers import FacebookWebhookPayloadSerializer

class FacebookWebhookView(APIView):
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.service = FacebookWebhookService()
        
    @swagger_auto_schema(
        operation_summary="Xác thực Webhook (GET)",
        operation_description="Dùng để Facebook gửi GET request xác minh Callback URL.",
        manual_parameters=[
            openapi.Parameter('hub.mode', openapi.IN_QUERY, description="Mode từ Facebook", type=openapi.TYPE_STRING),
            openapi.Parameter('hub.verify_token', openapi.IN_QUERY, description="Mã bí mật xác thực", type=openapi.TYPE_STRING),
            openapi.Parameter('hub.challenge', openapi.IN_QUERY, description="Chuỗi challenge", type=openapi.TYPE_STRING),
        ],
        responses={200: "Trả về mã challenge", 403: "Lỗi xác thực"}
    )
    def get(self, request, *args, **kwargs):
        mode = request.query_params.get('hub.mode')
        token = request.query_params.get('hub.verify_token')
        challenge = request.query_params.get('hub.challenge')

        is_valid, challenge_response = self.service.verify_webhook(mode, token, challenge)
        
        if is_valid:
            # Use HttpResponse only when Facebook requires a plain text challenge body.
            return HttpResponse(challenge_response, status=status.HTTP_200_OK, content_type="text/plain")
            
        return Response("Forbidden", status=status.HTTP_403_FORBIDDEN)

    @swagger_auto_schema(
        operation_summary="Nhận Event từ Facebook (POST)",
        operation_description="Xác thực chữ ký, chuẩn hoá payload và đẩy vào Kafka raw_events topic.",
        request_body=FacebookWebhookPayloadSerializer,
        responses={200: "EVENT_RECEIVED", 400: "Invalid payload", 403: "Invalid Signature"}
    )
    def post(self, request, *args, **kwargs):
        signature = request.META.get('HTTP_X_HUB_SIGNATURE_256')
        
        # Verify signature using raw body bytes
        if not self.service.verify_signature(request.body, signature):
            return Response({"error": "Invalid Signature"}, status=status.HTTP_403_FORBIDDEN)
            
        serializer = FacebookWebhookPayloadSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(
                {"error": "Invalid payload", "details": serializer.errors},
                status=status.HTTP_400_BAD_REQUEST,
            )

        payload = serializer.validated_data
        
        # Publish to Kafka raw_events topic
        self.service.publish_event(payload)
        self.service.flush()

        return Response({"status": "EVENT_RECEIVED"}, status=status.HTTP_200_OK)


class SubscribePageWebhookView(APIView):
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.service = FacebookSubscriptionService()

    @swagger_auto_schema(
        operation_summary="Đăng ký Page nhận Webhook từ Facebook",
        operation_description="Gọi Graph API để subscribe page vào webhook (yêu cầu FACEBOOK_PAGE_ACCESS_TOKEN).",
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            properties={
                'page_id': openapi.Schema(type=openapi.TYPE_STRING, description="ID của Page", example=PAGE_ID),
            },
            required=['page_id']
        ),
        responses={200: "Thành công", 400: "Lỗi"}
    )
    def post(self, request, *args, **kwargs):
        page_id = request.data.get('page_id')

        if not page_id:
            return Response({"error": "page_id is required"}, status=status.HTTP_400_BAD_REQUEST)

        try:
            result = self.service.subscribe_page_comment_events(page_id)
            return Response({"status": "Success", "data": result}, status=status.HTTP_200_OK)
        except FacebookSubscriptionError as e:
            return Response({"status": "Failed", "error": str(e)}, status=status.HTTP_400_BAD_REQUEST)