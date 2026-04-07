import os


class Config:
    REDIS_URL     = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    APP_BASE_URL  = os.environ.get("APP_BASE_URL", "http://localhost:9000")
    POLL_INTERVAL = int(os.environ.get("SCHEDULER_POLL_INTERVAL", 30))
    INTERNAL_API_SECRET = os.environ.get("INTERNAL_API_SECRET", "internal-secret-change-me")