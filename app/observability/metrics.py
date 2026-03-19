from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from typing import Any

from prometheus_client import Counter, Gauge, Histogram, Info, start_http_server

from app.config import Config
from app.observability.logger import get_logger

logger = get_logger(__name__)

_LATENCY_BUCKETS = (0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0)


class Metrics:
    # --- Counters ----------------------------------------------------------- #
    cdc_events_received_total = Counter(
        "cdc_events_received_total",
        "Total CDC events received from streams",
        ["table"],
    )
    cdc_events_processed_total = Counter(
        "cdc_events_processed_total",
        "Total CDC events successfully processed",
        ["table"],
    )
    cdc_events_failed_total = Counter(
        "cdc_events_failed_total",
        "Total CDC events that failed processing",
        ["table"],
    )
    cdc_events_skipped_total = Counter(
        "cdc_events_skipped_total",
        "Total CDC events skipped (empty payload)",
        ["table"],
    )
    orders_refreshed_total = Counter(
        "orders_refreshed_total",
        "Total order cache views refreshed",
    )
    debounce_coalesced_total = Counter(
        "debounce_coalesced_total",
        "Total refresh requests coalesced by debouncer",
    )
    cache_writes_total = Counter(
        "cache_writes_total",
        "Total raw hash writes to Redis",
        ["table"],
    )
    cache_deletes_total = Counter(
        "cache_deletes_total",
        "Total raw hash deletes from Redis",
        ["table"],
    )

    # --- Histograms --------------------------------------------------------- #
    cdc_event_processing_duration_seconds = Histogram(
        "cdc_event_processing_duration_seconds",
        "Duration of full event processing pipeline",
        ["table"],
        buckets=_LATENCY_BUCKETS,
    )
    order_join_rebuild_duration_seconds = Histogram(
        "order_join_rebuild_duration_seconds",
        "Duration of order view join rebuild",
        ["order_id"],
        buckets=_LATENCY_BUCKETS,
    )
    raw_hash_write_duration_seconds = Histogram(
        "raw_hash_write_duration_seconds",
        "Duration of raw hash write to Redis",
        ["table"],
        buckets=_LATENCY_BUCKETS,
    )
    index_update_duration_seconds = Histogram(
        "index_update_duration_seconds",
        "Duration of index update pipeline",
        ["table"],
        buckets=_LATENCY_BUCKETS,
    )
    consumer_batch_size = Histogram(
        "consumer_batch_size",
        "Number of messages per XREADGROUP batch",
        buckets=(1, 5, 10, 25, 50, 100, 250, 500),
    )

    # --- Gauges ------------------------------------------------------------- #
    consumer_stream_lag = Gauge(
        "consumer_stream_lag",
        "Pending message count per stream",
        ["stream"],
    )
    stream_length = Gauge(
        "stream_length",
        "Total messages in stream",
        ["stream"],
    )
    consumer_pending_messages = Gauge(
        "consumer_pending_messages",
        "Messages pending ACK in consumer group",
        ["stream"],
    )
    active_order_refreshes = Gauge(
        "active_order_refreshes",
        "Number of order refreshes currently in-flight",
    )
    worker_uptime_seconds = Gauge(
        "worker_uptime_seconds",
        "Worker process uptime in seconds",
    )

    # --- Info --------------------------------------------------------------- #
    worker = Info("worker", "Order Cache Worker build information")

    @classmethod
    def start_server(cls) -> None:
        if not Config.METRICS_ENABLED:
            return
        cls.worker.info(
            {
                "service": Config.OTEL_SERVICE_NAME,
                "consumer_name": Config.CONSUMER_NAME,
                "consumer_group": Config.CONSUMER_GROUP,
            }
        )
        start_http_server(Config.METRICS_PORT)
        logger.info(
            "Prometheus metrics server started",
            extra_fields={"port": Config.METRICS_PORT},
        )


# --------------------------------------------------------------------------- #
# MetricsTimer context manager                                                  #
# --------------------------------------------------------------------------- #

@contextmanager
def MetricsTimer(histogram: Histogram, labels: dict[str, Any] | None = None):
    """Time a block of code and record in the given histogram."""
    start = time.perf_counter()
    try:
        yield
    finally:
        duration = time.perf_counter() - start
        try:
            if labels:
                histogram.labels(**labels).observe(duration)
            else:
                histogram.observe(duration)
        except Exception:
            pass


# --------------------------------------------------------------------------- #
# Lag monitor                                                                   #
# --------------------------------------------------------------------------- #

def start_lag_monitor(r) -> None:
    """Background thread that updates stream/lag gauges every 5 s."""

    def _loop() -> None:
        from app.config import Config as Cfg

        while True:
            time.sleep(5)
            try:
                for stream_name in Cfg.STREAMS:
                    try:
                        length = r.xlen(stream_name)
                        Metrics.stream_length.labels(stream=stream_name).set(length)
                    except Exception:
                        pass
                    try:
                        groups = r.xinfo_groups(stream_name)
                        for g in groups:
                            if g.get("name") == Cfg.CONSUMER_GROUP:
                                Metrics.consumer_stream_lag.labels(
                                    stream=stream_name
                                ).set(g.get("pending", 0))
                                Metrics.consumer_pending_messages.labels(
                                    stream=stream_name
                                ).set(g.get("pending", 0))
                    except Exception:
                        pass
            except Exception as exc:
                logger.debug(
                    "Lag monitor error", extra_fields={"error": str(exc)}
                )

    t = threading.Thread(target=_loop, name="lag-monitor", daemon=True)
    t.start()
    logger.info("Lag monitor started")
