import redis
from app.config import Config
from app.observability.logger import get_logger

logger = get_logger(__name__)


def create_redis_client() -> redis.Redis:
    pool = redis.ConnectionPool(
        host=Config.REDIS_HOST,
        port=Config.REDIS_PORT,
        password=Config.REDIS_PASSWORD,
        db=Config.REDIS_DB,
        max_connections=Config.REDIS_MAX_CONNECTIONS,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5,
        retry_on_timeout=True,
        health_check_interval=30,
    )
    client = redis.Redis(connection_pool=pool)
    logger.info(
        "Redis client created",
        extra_fields={
            "host": Config.REDIS_HOST,
            "port": Config.REDIS_PORT,
            "max_connections": Config.REDIS_MAX_CONNECTIONS,
        },
    )
    return client


def ensure_consumer_groups(r: redis.Redis):
    for stream_name in Config.STREAMS:
        try:
            r.xgroup_create(stream_name, Config.CONSUMER_GROUP, id="0", mkstream=True)
            logger.info(
                "Consumer group created",
                extra_fields={"stream": stream_name, "group": Config.CONSUMER_GROUP},
            )
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.info(
                    "Consumer group already exists",
                    extra_fields={"stream": stream_name},
                )
            else:
                raise
