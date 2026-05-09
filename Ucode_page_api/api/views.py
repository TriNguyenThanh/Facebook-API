from rest_framework import status, serializers
from rest_framework.views import APIView
from rest_framework.response import Response
from drf_yasg.utils import swagger_auto_schema
from drf_yasg import openapi

from api.serializers import PostCreateSerializer
from .services import FacebookService

class PageInfoView(APIView):
    def get(self, request, pageId):
        fb = FacebookService()
        data = fb.get_page_info(pageId)
        return Response(data)

class PagePostsView(APIView):
    def get(self, request, pageId):
        fb = FacebookService()
        data = fb.get_posts(pageId)
        return Response(data)

class PostListCreateView(APIView):
    """Xử lý GET (lấy ds bài viết) và POST (tạo bài viết)"""
    def get(self, request, pageId):
        data = FacebookService().get_posts(pageId)
        return Response(data)

    @swagger_auto_schema(request_body=PostCreateSerializer)
    def post(self, request, pageId):
        serializer = PostCreateSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        
        # Ép kiểu an toàn để Pylance hiểu cấu trúc dữ liệu
        validated_data: dict = getattr(serializer, 'validated_data', {})
        data = validated_data.copy()
        message = data.pop('message', None)
        
        # Sửa lỗi Facebook Graph API không cho phép bài viết published mà lại có thời gian đăng (scheduled)
        if data.get('scheduled_publish_time'):
            data['published'] = False
        
        if not message and not data.get('link'):
            return Response({"error": "Cần có message hoặc link để đăng bài"}, status=status.HTTP_400_BAD_REQUEST)
            
        response_data = FacebookService().create_post(pageId, message=message, **data)
        return Response(response_data)

class PostDetailView(APIView):
    """Xử lý DELETE bài viết"""
    def delete(self, request, postId):
        data = FacebookService().delete_post(postId)
        return Response(data)

class PostCommentsView(APIView):
    def get(self, request, postId):
        data = FacebookService().get_comments(postId)
        return Response(data)

class PostLikesView(APIView):
    def get(self, request, postId):
        data = FacebookService().get_likes(postId)
        return Response(data)

class PageInsightsView(APIView):
    @swagger_auto_schema(
        manual_parameters=[
            openapi.Parameter(
                'metric',
                openapi.IN_QUERY,
                description="Chọn loại metric muốn xem (mặc định lấy tất cả)",
                type=openapi.TYPE_STRING,
                enum=[
                    "page_media_view",
                    "post_media_view",
                    "post_total_media_view_unique",
                ],
                required=False
            )
        ]
    )
    def get(self, request, pageId):
        metric = request.query_params.get('metric')
        data = FacebookService().get_insights(pageId, metric=metric)
        return Response(data)