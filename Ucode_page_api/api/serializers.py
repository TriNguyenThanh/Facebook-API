from rest_framework import status, serializers

class PostCreateSerializer(serializers.Serializer):
    message = serializers.CharField(required=False, help_text="Nội dung bài viết")
    link = serializers.URLField(required=False, help_text="Đường link đính kèm")
    published = serializers.BooleanField(required=False, default=True, help_text="Đăng ngay (true) hoặc lưu nháp (false)")