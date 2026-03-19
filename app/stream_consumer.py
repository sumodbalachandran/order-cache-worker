from __future__ import annotations

import time
from contextvars import ContextVar

import redis

from app.config import Config
from app.debounce import OrderRefreshDebouncer
from app.event_router import get_affected_order_ids
from app.indexes import update_indexes
from app.join_builder import refresh_order
from app.models import DebeziumEvent
from app.observability.logger import get_logger
from app.observability.metrics import Metrics, MetricsTimer
from app.observability.tracing import trace_span, SpanAttributes

logger = get_logger(__name__)

# Context variables for structured logging within a single event's processing
ctx_stream: ContextVar[str] = ContextVar("ctx_stream", default="")
ctx_table: ContextVar[str] = ContextVar("ctx_table", default="")
ctx_message_id: ContextVar[str] = ContextVar("ctx_message_id", default="")
ctx_order_id: ContextVar[str] = ContextVar("ctx_order_id", default="")
ctx_operation: ContextVar[str] = ContextVar("ctx_operation", default="")


class StreamConsumer:
    def __init__(self, r: redis.Redis) -> None:
        self._r = r
        self._debouncer: OrderRefreshDebouncer | None = None
        if Config.DEBOUNCE_ENABLED:
            self._debouncer = OrderRefreshDebouncer(
                refresh_fn=lambda oid: refresh_order(self._r, oid)
            )
        self._running = False
        self._processed = 0
        self._last_throughput_ts = time.monotonic()

    def stop(self) -> None:
        self._running = False
        if self._debouncer:
            self._debouncer.stop()

    def run(self) -> None:
        self._running = True
        logger.info(
            "StreamConsumer starting",
            extra_fields={
                "streams": list(Config.STREAMS.keys()),
                "group": Config.CONSUMER_GROUP,
                "consumer": Config.CONSUMER_NAME,
                "batch_size": Config.BATCH_SIZE,
            },
        )

        streams_arg = {name: ">" for name in Config.STREAMS}

        while self._running:
            try:
                results = self._r.xreadgroup(
                    groupname=Config.CONSUMER_GROUP,
                    consumername=Config.CONSUMER_NAME,
                    streams=streams_arg,
                    count=Config.BATCH_SIZE,
                    block=Config.BLOCK_MS,
                    noack=False,
                )
            except redis.exceptions.ConnectionError as exc:
                logger.error(
                    "Redis connection error in consumer; retrying",
                    extra_fields={"error": str(exc)},
                )
                time.sleep(5)
                continue
            except Exception as exc:
                logger.error(
                    "Unexpected error in XREADGROUP",
                    extra_fields={"error": str(exc)},
                )
                continue

            if not results:
                continue

            for stream_name, messages in results:
                table = Config.STREAMS.get(stream_name, stream_name)
                ack_ids: list[str] = []

                try:
                    Metrics.consumer_batch_size.observe(len(messages))
                except Exception:
                    pass

                for message_id, data in messages:
                    try:
                        self.process_event(stream_name, table, message_id, data)
                        ack_ids.append(message_id)
                        self._processed += 1
                    except Exception as exc:
                        logger.error(
                            "Unhandled error processing event",
                            extra_fields={
                                "stream": stream_name,
                                "message_id": message_id,
                                "error": str(exc),
                            },
                        )

                # Batch ACK via pipeline
                if ack_ids:
                    pipe = self._r.pipeline(transaction=False)
                    for mid in ack_ids:
                        pipe.xack(stream_name, Config.CONSUMER_GROUP, mid)
                    try:
                        pipe.execute()
                    except Exception as exc:
                        logger.error(
                            "Error ACKing messages",
                            extra_fields={"stream": stream_name, "error": str(exc)},
                        )

            self._log_throughput()

    def process_event(
        self,
        stream_name: str,
        table: str,
        message_id: str,
        data: dict[str, str],
    ) -> None:
        ctx_stream.set(stream_name)
        ctx_table.set(table)
        ctx_message_id.set(message_id)

        try:
            Metrics.cdc_events_received_total.labels(table=table).inc()
        except Exception:
            pass

        with MetricsTimer(Metrics.cdc_event_processing_duration_seconds, {"table": table}):
            with trace_span(
                "stream_consumer.process_event",
                {
                    SpanAttributes.STREAM: stream_name,
                    SpanAttributes.TABLE: table,
                    SpanAttributes.MESSAGE_ID: message_id,
                },
            ):
                event = DebeziumEvent.from_stream_message(stream_name, message_id, table, data)
                ctx_operation.set(event.operation)

                payload = event.payload
                if not payload:
                    try:
                        Metrics.cdc_events_skipped_total.labels(table=table).inc()
                    except Exception:
                        pass
                    return

                # 1. Write raw hash
                with MetricsTimer(Metrics.raw_hash_write_duration_seconds, {"table": table}):
                    self._write_raw_hash(table, payload, event.is_delete)

                # 2. Update indexes
                with MetricsTimer(Metrics.index_update_duration_seconds, {"table": table}):
                    pipe = self._r.pipeline(transaction=False)
                    update_indexes(self._r, table, payload, event.is_delete, pipe=pipe)
                    try:
                        pipe.execute()
                    except Exception as exc:
                        logger.error(
                            "Error executing index pipeline",
                            extra_fields={"table": table, "error": str(exc)},
                        )

                # 3. Route affected orders → schedule refresh
                affected_ids = get_affected_order_ids(self._r, table, payload)
                for order_id in affected_ids:
                    ctx_order_id.set(order_id)
                    if self._debouncer:
                        self._debouncer.schedule_refresh(order_id)
                    else:
                        try:
                            with MetricsTimer(
                                Metrics.order_join_rebuild_duration_seconds,
                                {"order_id": order_id},
                            ):
                                refresh_order(self._r, order_id)
                            Metrics.orders_refreshed_total.inc()
                        except Exception as exc:
                            logger.error(
                                "Error refreshing order",
                                extra_fields={"order_id": order_id, "error": str(exc)},
                            )

                try:
                    Metrics.cdc_events_processed_total.labels(table=table).inc()
                except Exception:
                    pass

    # ------------------------------------------------------------------ #
    # Private                                                              #
    # ------------------------------------------------------------------ #

    def _write_raw_hash(
        self,
        table: str,
        payload: dict,
        is_delete: bool,
    ) -> None:
        pk = self._get_primary_key(table, payload)
        if not pk:
            return
        raw_key = f"raw:{table}:{pk}"
        if is_delete:
            self._r.delete(raw_key)
            try:
                Metrics.cache_deletes_total.labels(table=table).inc()
            except Exception:
                pass
        else:
            self._r.hset(raw_key, mapping={k: str(v) for k, v in payload.items()})
            try:
                Metrics.cache_writes_total.labels(table=table).inc()
            except Exception:
                pass

    @staticmethod
    def _get_primary_key(table: str, payload: dict) -> str | None:
        pk_fields = {
            "orders": ["order_id", "id"],
            "customers": ["customer_id", "id"],
            "products": ["product_id", "id"],
            "order_items": ["item_id", "id"],
            "shipping": ["shipping_id", "id"],
        }
        for field in pk_fields.get(table, ["id"]):
            val = payload.get(field)
            if val is not None:
                return str(val)
        return None

    def _log_throughput(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_throughput_ts
        if elapsed >= 10.0:
            rate = self._processed / elapsed
            logger.info(
                "Consumer throughput",
                extra_fields={
                    "events_per_second": round(rate, 1),
                    "total_processed": self._processed,
                    "elapsed_seconds": round(elapsed, 1),
                },
            )
            self._processed = 0
            self._last_throughput_ts = now
