from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING

from app.config import Config
from app.observability.logger import get_logger
from app.observability.metrics import Metrics

if TYPE_CHECKING:
    import redis

logger = get_logger(__name__)


class OrderRefreshDebouncer:
    """Coalesces rapid back-to-back refresh requests for the same order_id.

    A background flush thread fires every ``DEBOUNCE_INTERVAL_MS`` ms and
    calls *refresh_fn* for each pending order_id exactly once, regardless
    of how many times it was scheduled in that window.
    """

    def __init__(self, refresh_fn, interval_ms: int | None = None) -> None:
        self._refresh_fn = refresh_fn
        self._interval = (interval_ms or Config.DEBOUNCE_INTERVAL_MS) / 1000.0
        self._pending: set[str] = set()
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._flush_loop,
            name="debounce-flush",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "OrderRefreshDebouncer started",
            extra_fields={"interval_ms": interval_ms or Config.DEBOUNCE_INTERVAL_MS},
        )

    def schedule_refresh(self, order_id: str) -> None:
        """Enqueue *order_id* for a debounced refresh."""
        with self._lock:
            already_pending = order_id in self._pending
            self._pending.add(order_id)

        if already_pending:
            try:
                Metrics.debounce_coalesced_total.inc()
            except Exception:
                pass

    def stop(self) -> None:
        """Signal the flush thread to stop and perform a final flush."""
        self._stop_event.set()
        self._thread.join(timeout=self._interval * 3 + 1)
        self._flush()  # final flush of any remaining items
        logger.info("OrderRefreshDebouncer stopped")

    # ------------------------------------------------------------------ #
    # Private                                                              #
    # ------------------------------------------------------------------ #

    def _flush_loop(self) -> None:
        while not self._stop_event.wait(timeout=self._interval):
            self._flush()

    def _flush(self) -> None:
        with self._lock:
            if not self._pending:
                return
            batch = self._pending.copy()
            self._pending.clear()

        try:
            Metrics.active_order_refreshes.set(len(batch))
        except Exception:
            pass

        for order_id in batch:
            try:
                self._refresh_fn(order_id)
                try:
                    Metrics.orders_refreshed_total.inc()
                except Exception:
                    pass
            except Exception as exc:
                logger.error(
                    "Error refreshing order in debouncer",
                    extra_fields={"order_id": order_id, "error": str(exc)},
                )

        try:
            Metrics.active_order_refreshes.set(0)
        except Exception:
            pass
