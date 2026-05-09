import os
from pathlib import Path

from dotenv import load_dotenv

# Swagger / OpenAPI (drf-yasg)

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

# Load environment variables from .env at project root (same level as manage.py)
load_dotenv(BASE_DIR / ".env")
# Also load from workspace root
load_dotenv(BASE_DIR.parent / ".env")

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = os.getenv("DJANGO_SECRET_KEY", "dev-only-unsafe-secret-key")

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = os.getenv("DJANGO_DEBUG", "0") == "1"

# ALLOWED_HOSTS = [
#     host.strip()
#     for host in os.getenv("DJANGO_ALLOWED_HOSTS", "localhost,127.0.0.1").split(",")
#     if host.strip()
# ]
ALLOWED_HOSTS = ["*"]  # Allow all hosts for development; restrict in production

# Application definition
INSTALLED_APPS = [
    # Django apps
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    # Third-party apps
    "rest_framework",
    "drf_yasg",
    # Local apps
    "webhook",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",  # must be before AuthenticationMiddleware
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "core.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        # You can add BASE_DIR / "templates" if you introduce project-level templates later
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",  # required by admin
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "core.wsgi.application"
ASGI_APPLICATION = "core.asgi.application"

# Database
# Defaulting to SQLite for local dev. Override via env if needed.
DATABASES = {
    "default": {
        "ENGINE": os.getenv("DJANGO_DB_ENGINE", "django.db.backends.sqlite3"),
        "NAME": os.getenv("DJANGO_DB_NAME", str(BASE_DIR / "db.sqlite3")),
        "USER": os.getenv("DJANGO_DB_USER", ""),
        "PASSWORD": os.getenv("DJANGO_DB_PASSWORD", ""),
        "HOST": os.getenv("DJANGO_DB_HOST", ""),
        "PORT": os.getenv("DJANGO_DB_PORT", ""),
    }
}

# Password validation
AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"
    },
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]

# Internationalization
LANGUAGE_CODE = os.getenv("DJANGO_LANGUAGE_CODE", "en-us")
TIME_ZONE = os.getenv("DJANGO_TIME_ZONE", "UTC")
USE_I18N = True
USE_TZ = True

# Static files (CSS, JavaScript, Images)
STATIC_URL = "static/"
STATIC_ROOT = os.getenv("DJANGO_STATIC_ROOT", "")

# Default primary key field type
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# Django REST Framework (minimal; customize as needed)
REST_FRAMEWORK = {
    # Leave auth open by default since webhooks often don't use session auth
    "DEFAULT_PERMISSION_CLASSES": [
        "rest_framework.permissions.AllowAny",
    ],
}

# Optional: Kafka-related env values (use in your producer code)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_WEBHOOK_TOPIC = os.getenv("KAFKA_WEBHOOK_TOPIC", "webhooks")
KAFKA_RAW_EVENTS_TOPIC = os.getenv("KAFKA_RAW_EVENTS_TOPIC", "raw_events")

# Publish raw_events by default; optionally also publish to legacy KAFKA_WEBHOOK_TOPIC.
KAFKA_PUBLISH_WEBHOOKS_TOPIC = os.getenv("KAFKA_PUBLISH_WEBHOOKS_TOPIC", "0") == "1"

# Facebook webhook verification token (used by your verify view)
FACEBOOK_VERIFY_TOKEN = os.getenv("FACEBOOK_VERIFY_TOKEN", "")

# Facebook App Secret (used for verifying payload signatures)
FACEBOOK_APP_SECRET = os.getenv("FACEBOOK_APP_SECRET", "")

# Facebook App ID (used for general API queries if needed)
FACEBOOK_APP_ID = os.getenv("FACEBOOK_APP_ID", "")

# Facebook Graph API Version
FACEBOOK_GRAPH_API_VERSION = os.getenv("FACEBOOK_GRAPH_API_VERSION", "v19.0")

# Facebook Page Access Token
FACEBOOK_PAGE_ACCESS_TOKEN = os.getenv("FACEBOOK_PAGE_ACCESS_TOKEN", "")

PAGE_ID = os.getenv("PAGE_ID", "")