from __future__ import annotations

from django.conf import settings
from django.http import JsonResponse
from rest_framework import status
from rest_framework.permissions import BasePermission
from rest_framework.response import Response
from rest_framework.views import APIView
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema

from api.serializers import PostCreateSerializer
from .services import FacebookGraphError, FacebookService


def api_response(data=None, error: str | None = None, http_status=status.HTTP_200_OK):
    return Response({"success": error is None, "data": data, "error": error}, status=http_status)


def health(_request):
    return JsonResponse({"status": "ok", "service": "backend-api"})


class AdminApiKeyPermission(BasePermission):
    def has_permission(self, request, view):
        expected = getattr(settings, "ADMIN_API_KEY", "")
        if not expected:
            return True
        return request.headers.get("X-Admin-API-Key") == expected


class FacebookAPIView(APIView):
    permission_classes = [AdminApiKeyPermission]

    def handle_facebook(self, callback):
        try:
            return api_response(callback())
        except FacebookGraphError as exc:
            http_status = status.HTTP_503_SERVICE_UNAVAILABLE if exc.retryable else status.HTTP_400_BAD_REQUEST
            return api_response(error=str(exc), http_status=http_status)


class PageInfoView(FacebookAPIView):
    def get(self, request, pageId):
        return self.handle_facebook(lambda: FacebookService().get_page_info(pageId))


class PagePostsView(FacebookAPIView):
    def get(self, request, pageId):
        return self.handle_facebook(lambda: FacebookService().get_posts(pageId))


class PostListCreateView(FacebookAPIView):
    def get(self, request, pageId):
        return self.handle_facebook(lambda: FacebookService().get_posts(pageId))

    @swagger_auto_schema(request_body=PostCreateSerializer)
    def post(self, request, pageId):
        serializer = PostCreateSerializer(data=request.data)
        if not serializer.is_valid():
            return api_response(error=str(serializer.errors), http_status=status.HTTP_400_BAD_REQUEST)

        data = dict(serializer.validated_data)
        message = data.pop("message", None)
        if data.get("scheduled_publish_time"):
            data["published"] = False
        if not message and not data.get("link"):
            return api_response(
                error="Cần có message hoặc link để đăng bài",
                http_status=status.HTTP_400_BAD_REQUEST,
            )

        return self.handle_facebook(lambda: FacebookService().create_post(pageId, message=message, **data))


class PostDetailView(FacebookAPIView):
    def delete(self, request, postId):
        return self.handle_facebook(lambda: FacebookService().delete_post(postId))


class PostCommentsView(FacebookAPIView):
    def get(self, request, postId):
        return self.handle_facebook(lambda: FacebookService().get_comments(postId))


class PostLikesView(FacebookAPIView):
    def get(self, request, postId):
        return self.handle_facebook(lambda: FacebookService().get_likes(postId))


class PageInsightsView(FacebookAPIView):
    @swagger_auto_schema(
        manual_parameters=[
            openapi.Parameter(
                "metric",
                openapi.IN_QUERY,
                description="Chọn loại metric muốn xem (mặc định lấy tất cả)",
                type=openapi.TYPE_STRING,
                enum=[
                    "page_media_view",
                    "post_media_view",
                    "post_total_media_view_unique",
                ],
                required=False,
            )
        ]
    )
    def get(self, request, pageId):
        metric = request.query_params.get("metric")
        return self.handle_facebook(lambda: FacebookService().get_insights(pageId, metric=metric))
