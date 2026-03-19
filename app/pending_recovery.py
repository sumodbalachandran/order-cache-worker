from __future__ import annotations

import threading
import time

import redis

from app.config import Config
from app.debounce import OrderRefreshDebouncer
from app.event_router import get_affected_order_ids
from app.indexes import update_indexes
from app.join_builder import refresh_order
from app.models import DebeziumEvent
from app.observability.logger import get_logger
from app.observability.metrics import Metrics

logger = get_logger(__name__)

_IDLE_THRESHOLD_MS = 60_000  # reclaim messages idle for > 60 s
_SWEEP_COUNT = 50             # messages per stream per sweep


class PendingRecovery:
    """Background thread that uses XAUTOCLAIM to reclaim timed-out pending messages.

    Every ``PENDING_CHECK_INTERVAL`` seconds it sweeps each stream, claiming
    messages that have been idle for more than ``_IDLE_THRESHOLD_MS`` ms, and
    re-processes them so no event is silently lost.
    """

    def __init__(self, r: redis.Redis, debouncer: OrderRefreshDebouncer | None = None) -> None:
        self._r = r
        self._debouncer = debouncer
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="pending-recovery",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()
        logger.info(
            "PendingRecovery started",
            extra_fields={
                "interval_seconds": Config.PENDING_CHECK_INTERVAL,
                "idle_threshold_ms": _IDLE_THRESHOLD_MS,
            },
        )

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(timeout=Config.PENDING_CHECK_INTERVAL + 5)
        logger.info("PendingRecovery stopped")

    # ------------------------------------------------------------------ #
    # Private                                                              #
    # ------------------------------------------------------------------ #

    def _run_loop(self) -> None:
        while not self._stop_event.wait(timeout=Config.PENDING_CHECK_INTERVAL):
            self._sweep_all_streams()

    def _sweep_all_streams(self) -> None:
        for stream_name, table in Config.STREAMS.items():
            try:
                self._sweep_stream(stream_name, table)
            except Exception as exc:
                logger.error(
                    "Error during pending recovery sweep",
                    extra_fields={"stream": stream_name, "error": str(exc)},
                )

    def _sweep_stream(self, stream_name: str, table: str) -> None:
        try:
            result = self._r.xautoclaim(
                stream_name,
                Config.CONSUMER_GROUP,
                Config.CONSUMER_NAME,
                min_idle_time=_IDLE_THRESHOLD_MS,
                start_id="0-0",
                count=_SWEEP_COUNT,
            )
        except redis.exceptions.ResponseError as exc:
            # XAUTOCLAIM requires Redis 7+; fallback gracefully
            logger.warning(
                "XAUTOCLAIM failed (Redis < 7?); skipping",
                extra_fields={"stream": stream_name, "error": str(exc)},
            )
            return

        # result format: [next_start_id, [[id, fields], ...], [deleted_ids]]
        if not result or len(result) < 2:
            return
        messages = result[1]
        if not messages:
            return

        logger.info(
            "Reclaimed pending messages",
            extra_fields={"stream": stream_name, "count": len(messages)},
        )

        ack_ids: list[str] = []
        for message_id, data in messages:
            try:
                self._reprocess(stream_name, table, message_id, data)
                ack_ids.append(message_id)
            except Exception as exc:
                logger.error(
                    "Error reprocessing recovered message",
                    extra_fields={
                        "stream": stream_name,
                        "message_id": message_id,
                        "error": str(exc),
                    },
                )

        if ack_ids:
            pipe = self._r.pipeline(transaction=False)
            for mid in ack_ids:
                pipe.xack(stream_name, Config.CONSUMER_GROUP, mid)
            try:
                pipe.execute()
            except Exception as exc:
                logger.error(
                    "Error ACKing recovered messages",
                    extra_fields={"stream": stream_name, "error": str(exc)},
                )

    def _reprocess(
        self,
        stream_name: str,
        table: str,
        message_id: str,
        data: dict[str, str],
    ) -> None:
        event = DebeziumEvent.from_stream_message(stream_name, message_id, table, data)
        payload = event.payload
        if not payload:
            return

        # Write raw hash
        pk_fields = {
            "orders": "order_id",
            "customers": "customer_id",
            "products": "product_id",
            "order_items": "item_id",
            "shipping": "shipping_id",
        }
        pk_field = pk_fields.get(table, "id")
        pk = payload.get(pk_field) or payload.get("id")
        if pk:
            raw_key = f"raw:{table}:{pk}"
            if event.is_delete:
                self._r.delete(raw_key)
            else:
                self._r.hset(raw_key, mapping={k: str(v) for k, v in payload.items()})

        # Update indexes
        pipe = self._r.pipeline(transaction=False)
        update_indexes(self._r, table, payload, event.is_delete, pipe=pipe)
        pipe.execute()

        # Schedule refresh
        affected_ids = get_affected_order_ids(self._r, table, payload)
        for order_id in affected_ids:
            if self._debouncer:
                self._debouncer.schedule_refresh(order_id)
            else:
                refresh_order(self._r, order_id)
                try:
                    Metrics.orders_refreshed_total.inc()
                except Exception:
                    pass
