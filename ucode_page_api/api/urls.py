from django.urls import path
from .views import (
    PageInfoView, PostListCreateView, PostDetailView, 
    PostCommentsView, PostLikesView, PageInsightsView
)

urlpatterns = [
    path('page/<str:pageId>/', PageInfoView.as_view()),
    path('page/<str:pageId>/posts/', PostListCreateView.as_view()),
    path('page/post/<str:postId>/', PostDetailView.as_view()),
    path('page/post/<str:postId>/comments/', PostCommentsView.as_view()),
    path('page/post/<str:postId>/likes/', PostLikesView.as_view()),
    path('page/<str:pageId>/insights/', PageInsightsView.as_view()),
]