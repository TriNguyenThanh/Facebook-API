import requests
from decouple import config

class FacebookService:
    def __init__(self):
        self.token = config('FB_PAGE_ACCESS_TOKEN')
        self.base_url = "https://graph.facebook.com/v19.0"

    def _get_request(self, endpoint, params=None):
        """Hàm hỗ trợ gọi GET (Viết riêng để tái sử dụng)"""
        url = f"{self.base_url}/{endpoint}"
        default_params = {'access_token': self.token}
        if params:
            default_params.update(params)
        response = requests.get(url, params=default_params)
        return response.json()

    def get_page_info(self, page_id, fields=None):
        """GET /api/page/{pageId}"""
        if fields is None:
            # Lấy các trường thông tin phổ biến mặc định
            fields = "id,name,about,category,fan_count,followers_count,picture,cover,link,website"
        return self._get_request(page_id, params={'fields': fields})

    def get_posts(self, page_id):
        return self._get_request(f"{page_id}/feed")

    def create_post(self, page_id, message=None, **kwargs):
        """POST /api/page/{pageId}/posts"""
        url = f"{self.base_url}/{page_id}/feed"
        payload = {'access_token': self.token}
        if message:
            payload['message'] = message
        if kwargs:
            payload.update(kwargs)
        response = requests.post(url, data=payload)
        return response.json()

    def delete_post(self, post_id):
        """DELETE /api/page/post/{postId}"""
        url = f"{self.base_url}/{post_id}"
        params = {'access_token': self.token}
        response = requests.delete(url, params=params)
        return response.json()

    def get_comments(self, post_id):
        """GET /api/page/post/{postId}/comments"""
        return self._get_request(f"{post_id}/comments")

    def get_likes(self, post_id):
        """GET /api/page/post/{postId}/likes"""
        # Thêm summary=true để lấy tổng số lượt like dễ dàng hơn
        return self._get_request(f"{post_id}/likes", params={'summary': 'true'})

    def get_insights(self, page_id, metric=None):
        """GET /api/page/{pageId}/insights"""
        # Nếu người dùng không truyền, dùng danh sách mặc định
        if not metric:
            INSIGHT_METRIC_CHOICES = [
                "page_media_view",
                "post_media_view",
                "post_total_media_view_unique",
            ]
            metric = ','.join(INSIGHT_METRIC_CHOICES)
            
        params = {'metric': metric}
        return self._get_request(f"{page_id}/insights", params=params)