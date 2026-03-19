import os


class Config:
    REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
    REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
    REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)
    REDIS_DB = int(os.getenv("REDIS_DB", 0))
    REDIS_MAX_CONNECTIONS = int(os.getenv("REDIS_MAX_CONNECTIONS", 50))
    CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "join_workers")
    CONSUMER_NAME = os.getenv("CONSUMER_NAME", "worker-1")
    STREAMS = {
        "stream:orders": "orders",
        "stream:customers": "customers",
        "stream:products": "products",
        "stream:order_items": "order_items",
        "stream:shipping": "shipping",
    }
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", 100))
    BLOCK_MS = int(os.getenv("BLOCK_MS", 2000))
    MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))
    PENDING_CHECK_INTERVAL = int(os.getenv("PENDING_CHECK_INTERVAL", 30))
    CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", 86400))
    DEBOUNCE_ENABLED = os.getenv("DEBOUNCE_ENABLED", "true").lower() == "true"
    DEBOUNCE_INTERVAL_MS = int(os.getenv("DEBOUNCE_INTERVAL_MS", 50))
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FORMAT = os.getenv("LOG_FORMAT", "json")
    LOG_FILE = os.getenv("LOG_FILE", "")
    LOG_MAX_BYTES = int(os.getenv("LOG_MAX_BYTES", 104857600))
    LOG_BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", 5))
    OTEL_ENABLED = os.getenv("OTEL_ENABLED", "true").lower() == "true"
    OTEL_SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "order-cache-worker")
    OTEL_EXPORTER_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4317")
    OTEL_SAMPLE_RATE = float(os.getenv("OTEL_SAMPLE_RATE", "0.1"))
    METRICS_ENABLED = os.getenv("METRICS_ENABLED", "true").lower() == "true"
    METRICS_PORT = int(os.getenv("METRICS_PORT", 8000))
    HEALTH_PORT = int(os.getenv("HEALTH_PORT", 8001))
    DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"
    DEBUG_PORT = int(os.getenv("DEBUG_PORT", 8002))
