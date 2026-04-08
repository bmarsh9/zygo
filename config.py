from sqlalchemy.engine.url import make_url
from urllib.parse import urlparse
from datetime import timedelta
from cryptography.fernet import Fernet
import os


basedir = os.path.abspath(os.path.dirname(__file__))


def parse_url_with_defaults(url, default_scheme="http", default_port=5000):
    # Ensure the URL has a scheme, add the default if missing
    if "://" not in url:
        url = f"{default_scheme}://{url}"

    # Parse the URL
    parsed_url = urlparse(url)

    # Extract components with defaults
    scheme = parsed_url.scheme or default_scheme
    host_name = parsed_url.hostname or "localhost"

    # If scheme is https, default port to 443
    if scheme == "https" and parsed_url.port is None:
        port = 443
    else:
        port = parsed_url.port or default_port

    # Construct the full URL
    if (scheme == "http" and port == 80) or (scheme == "https" and port == 443):
        full_url = f"{scheme}://{host_name}/"
    else:
        full_url = f"{scheme}://{host_name}:{port}/"

    return scheme, host_name, port, full_url


class Config:
    APP_NAME = os.environ.get("APP_NAME", "Zygo")
    APP_SUBTITLE = os.environ.get("APP_SUBTITLE", "")
    CR_YEAR = os.environ.get("CR_YEAR", "2026")
    VERSION = os.environ.get("VERSION", "1.0.0")

    scheme, host_name, port, full_url = parse_url_with_defaults(
        os.environ.get("HOST_NAME", "localhost")
    )
    HOST_NAME = full_url
    SCHEME = scheme
    PORT = port

    LOG_TYPE = os.environ.get("LOG_TYPE", "stream")
    LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
    ENABLE_GCP_LOGGING = os.environ.get("ENABLE_GCP_LOGGING", "false").lower() == "true"

    SECRET_KEY = os.environ.get("SECRET_KEY", "change_secret_key")
    DOC_LINK = os.environ.get("DOC_LINK", "")
    DEFAULT_EMAIL = os.environ.get("DEFAULT_EMAIL", "admin@example.com")
    DEFAULT_PASSWORD = os.environ.get("DEFAULT_PASSWORD", "admin1234567")
    HELP_EMAIL = os.environ.get("HELP_EMAIL", DEFAULT_EMAIL)

    SQLALCHEMY_COMMIT_ON_TEARDOWN = True
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_RECORD_QUERIES = False
    MAIL_SERVER = os.environ.get("MAIL_SERVER", "smtp.googlemail.com")
    MAIL_PORT = int(os.environ.get("MAIL_PORT", 587))
    MAIL_USE_TLS = os.environ.get("MAIL_USE_TLS", "true").lower() == "true"
    MAIL_DEBUG = os.environ.get("MAIL_DEBUG", "false").lower() == "true"
    MAIL_USERNAME = os.environ.get("MAIL_USERNAME")
    MAIL_DEFAULT_SENDER = os.environ.get("MAIL_DEFAULT_SENDER", DEFAULT_EMAIL)
    MAIL_PASSWORD = os.environ.get("MAIL_PASSWORD")
    EMAIL_PROVIDER = os.environ.get("EMAIL_PROVIDER", "smtp")

    INTERNAL_API_SECRET = os.environ.get("INTERNAL_API_SECRET", "internal-secret-change-me")
    PERMANENT_SESSION_LIFETIME = timedelta(hours=int(os.environ.get("PERMANENT_SESSION_LIFETIME", 10)))

    REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    RATELIMIT_STORAGE_URI = REDIS_URL
    BASE_DIR = basedir

    # Only allowed if email is configured as well - see "is_self_registration_enabled"
    ENABLE_SELF_REGISTRATION = (
        os.environ.get("ENABLE_SELF_REGISTRATION", "false").lower() == "true"
    )
    SELF_REGISTRATION_SECRET = os.environ.get("SELF_REGISTRATION_SECRET")

    ENABLE_GOOGLE_AUTH = os.environ.get("ENABLE_GOOGLE_AUTH", "false").lower() == "true"
    ENABLE_MICROSOFT_AUTH = (
        os.environ.get("ENABLE_MICROSOFT_AUTH", "false").lower() == "true"
    )

    GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
    GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")

    MICROSOFT_CLIENT_ID = os.environ.get("MICROSOFT_CLIENT_ID")
    MICROSOFT_CLIENT_SECRET = os.environ.get("MICROSOFT_CLIENT_SECRET")

    FERNET_KEY = os.environ.get("FERNET_KEY", "ZmDfcTF7_60GrrY167zsiPd67pEvs0aGOv2oasOM1Pg=")
    FERNET = Fernet(FERNET_KEY.encode() if isinstance(FERNET_KEY, str) else FERNET_KEY)

    # Expose certain env vars in front end (DO NOT PLACE SENSITIVE VARS)
    # Use csv (e.g. HOST_NAME, another_var)
    DEBUG_ENV_VARS = (
        os.environ.get("DEBUG_ENV_VARS", "HOST_NAME").upper().split(",")
    )

    @staticmethod
    def init_app(app):
        pass


class ProductionConfig(Config):
    DEBUG = False
    SQLALCHEMY_DATABASE_URI = (
        os.environ.get("SQLALCHEMY_DATABASE_URI") or "postgresql://db1:db1@postgres/db1"
    )
    url = make_url(SQLALCHEMY_DATABASE_URI)
    POSTGRES_HOST = url.host
    POSTGRES_USER = url.username
    POSTGRES_PASSWORD = url.password
    POSTGRES_DB = url.database


class DevelopmentConfig(Config):
    DEBUG = True
    SQLALCHEMY_DATABASE_URI = (
        os.environ.get("SQLALCHEMY_DATABASE_URI") or "postgresql://db1:db1@postgres/db1"
    )
    url = make_url(SQLALCHEMY_DATABASE_URI)
    POSTGRES_HOST = url.host
    POSTGRES_USER = url.username
    POSTGRES_PASSWORD = url.password
    POSTGRES_DB = url.database


class TestingConfig(Config):
    TESTING = True
    DEBUG = True
    SQLALCHEMY_DATABASE_URI = (
        os.environ.get("SQLALCHEMY_DATABASE_URI") or "postgresql://db1:db1@postgres/db1"
    )
    url = make_url(SQLALCHEMY_DATABASE_URI)
    POSTGRES_HOST = url.host
    POSTGRES_USER = url.username
    POSTGRES_PASSWORD = url.password
    POSTGRES_DB = url.database
    WTF_CSRF_ENABLED = False


config = {
    "development": DevelopmentConfig,
    "testing": TestingConfig,
    "default": ProductionConfig,
}
