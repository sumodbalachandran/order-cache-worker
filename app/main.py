from __future__ import annotations

import signal
import sys
import time

import redis

from app.config import Config
from app.observability import setup_logging, setup_tracing
from app.observability.debug_inspector import start_debug_server
from app.observability.health import start_health_server, update_health
from app.observability.logger import get_logger
from app.observability.metrics import Metrics, start_lag_monitor
from app.pending_recovery import PendingRecovery
from app.redis_client import create_redis_client, ensure_consumer_groups
from app.stream_consumer import StreamConsumer

logger = get_logger(__name__)


def main() -> None:
    # ------------------------------------------------------------------ #
    # Observability bootstrap                                              #
    # ------------------------------------------------------------------ #
    setup_logging()
    setup_tracing()

    if Config.METRICS_ENABLED:
        Metrics.start_server()

    start_health_server()

    # ------------------------------------------------------------------ #
    # Redis                                                                #
    # ------------------------------------------------------------------ #
    r: redis.Redis = create_redis_client()
    try:
        r.ping()
        update_health(redis_connected=True)
        logger.info("Redis ping OK")
    except redis.exceptions.ConnectionError as exc:
        logger.error("Failed to connect to Redis", extra_fields={"error": str(exc)})
        sys.exit(1)

    ensure_consumer_groups(r)

    # ------------------------------------------------------------------ #
    # Optional debug server                                                #
    # ------------------------------------------------------------------ #
    if Config.DEBUG_MODE:
        start_debug_server(r)

    # ------------------------------------------------------------------ #
    # Lag monitor                                                          #
    # ------------------------------------------------------------------ #
    start_lag_monitor(r)

    # ------------------------------------------------------------------ #
    # Consumer + recovery                                                  #
    # ------------------------------------------------------------------ #
    consumer = StreamConsumer(r)
    recovery = PendingRecovery(r, debouncer=consumer._debouncer)
    recovery.start()

    # ------------------------------------------------------------------ #
    # Uptime tracker                                                       #
    # ------------------------------------------------------------------ #
    start_time = time.monotonic()

    def _update_uptime() -> None:
        import threading

        def _loop() -> None:
            while True:
                time.sleep(10)
                try:
                    Metrics.worker_uptime_seconds.set(time.monotonic() - start_time)
                except Exception:
                    pass

        t = threading.Thread(target=_loop, name="uptime-tracker", daemon=True)
        t.start()

    _update_uptime()

    # ------------------------------------------------------------------ #
    # Signal handlers                                                      #
    # ------------------------------------------------------------------ #
    def _shutdown(signum, frame) -> None:  # noqa: ARG001
        logger.info("Shutdown signal received", extra_fields={"signal": signum})
        consumer.stop()
        recovery.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    logger.info(
        "Order Cache Worker starting",
        extra_fields={
            "service": Config.OTEL_SERVICE_NAME,
            "consumer": Config.CONSUMER_NAME,
            "group": Config.CONSUMER_GROUP,
        },
    )

    # Blocks until stopped
    consumer.run()


if __name__ == "__main__":
    main()
