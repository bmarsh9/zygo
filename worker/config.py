import os


class Config:
    REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    # Base URL of the Flask app — used by job_handler to call the internal API
    APP_BASE_URL = os.environ.get("APP_BASE_URL", "http://localhost:9000")
    INTERNAL_API_SECRET = os.environ.get("INTERNAL_API_SECRET", "internal-secret-change-me")
