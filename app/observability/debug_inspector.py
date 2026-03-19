from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any
from urllib.parse import urlparse

import redis

from app.config import Config
from app.join_builder import build_order_view, refresh_order
from app.observability.logger import get_logger

logger = get_logger(__name__)

_redis_client: redis.Redis | None = None


def _r() -> redis.Redis:
    if _redis_client is None:
        raise RuntimeError("Debug server not initialized")
    return _redis_client


class DebugHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args) -> None:  # noqa: A002
        pass

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        parts = [p for p in parsed.path.split("/") if p]

        if not parts or parts[0] != "debug":
            self._send(404, {"error": "not found"})
            return

        try:
            if len(parts) == 3 and parts[1] == "order":
                self._debug_order(parts[2])
            elif len(parts) == 3 and parts[1] == "stream":
                self._debug_stream(parts[2])
            elif len(parts) == 3 and parts[1] == "indexes":
                self._debug_indexes(parts[2])
            elif len(parts) == 4 and parts[1] == "raw":
                self._debug_raw(parts[2], parts[3])
            elif len(parts) == 3 and parts[1] == "rebuild":
                self._debug_rebuild(parts[2])
            elif len(parts) == 3 and parts[1] == "pipeline" and parts[2] == "stats":
                self._debug_pipeline_stats()
            else:
                self._send(404, {"error": "unknown debug endpoint"})
        except Exception as exc:
            self._send(500, {"error": str(exc)})

    def _debug_order(self, order_id: str) -> None:
        r = _r()
        raw = r.hgetall(f"raw:orders:{order_id}")
        cached = r.hgetall(f"cache:order:{order_id}")
        ttl = r.ttl(f"cache:order:{order_id}")
        item_ids = list(r.smembers(f"idx:order_items:{order_id}"))
        shipping_ids = list(r.smembers(f"idx:order_shipping:{order_id}"))
        staleness: str | None = None
        if cached.get("updated_at") and raw.get("updated_at"):
            staleness = "stale" if cached["updated_at"] != raw["updated_at"] else "fresh"
        self._send(
            200,
            {
                "order_id": order_id,
                "raw_order": raw,
                "cached_view": cached,
                "ttl_seconds": ttl,
                "item_ids": item_ids,
                "shipping_ids": shipping_ids,
                "staleness": staleness,
            },
        )

    def _debug_stream(self, stream_name: str) -> None:
        r = _r()
        try:
            info = r.xinfo_stream(stream_name)
            groups = r.xinfo_groups(stream_name)
            last_msgs = r.xrevrange(stream_name, count=5)
            self._send(
                200,
                {
                    "stream": stream_name,
                    "info": info,
                    "groups": groups,
                    "last_5_messages": last_msgs,
                },
            )
        except redis.exceptions.ResponseError as exc:
            self._send(404, {"error": str(exc)})

    def _debug_indexes(self, order_id: str) -> None:
        r = _r()
        order_raw = r.hgetall(f"raw:orders:{order_id}")
        customer_id = order_raw.get("customer_id")
        item_ids = list(r.smembers(f"idx:order_items:{order_id}"))
        shipping_ids = list(r.smembers(f"idx:order_shipping:{order_id}"))

        # Bidirectional check: item_id → order_id should round-trip
        bidi_items: dict[str, Any] = {}
        for iid in item_ids:
            resolved_order = r.get(f"idx:item_order:{iid}")
            bidi_items[iid] = {
                "idx:item_order": resolved_order,
                "consistent": resolved_order == order_id,
            }

        bidi_shipping: dict[str, Any] = {}
        for sid in shipping_ids:
            resolved_order = r.get(f"idx:shipping_order:{sid}")
            bidi_shipping[sid] = {
                "idx:shipping_order": resolved_order,
                "consistent": resolved_order == order_id,
            }

        customer_orders: list[str] = []
        if customer_id:
            customer_orders = list(r.smembers(f"idx:customer_orders:{customer_id}"))

        self._send(
            200,
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "idx:order_items": item_ids,
                "idx:order_shipping": shipping_ids,
                "idx:customer_orders": customer_orders,
                "bidi_items_check": bidi_items,
                "bidi_shipping_check": bidi_shipping,
            },
        )

    def _debug_raw(self, table: str, pk: str) -> None:
        raw = _r().hgetall(f"raw:{table}:{pk}")
        self._send(200, {"table": table, "pk": pk, "data": raw})

    def _debug_rebuild(self, order_id: str) -> None:
        start = time.perf_counter()
        refresh_order(_r(), order_id)
        elapsed_ms = (time.perf_counter() - start) * 1000
        view = _r().hgetall(f"cache:order:{order_id}")
        self._send(
            200,
            {
                "order_id": order_id,
                "rebuild_ms": round(elapsed_ms, 2),
                "view": view,
            },
        )

    def _debug_pipeline_stats(self) -> None:
        r = _r()
        info = r.info("all")
        stream_stats: dict[str, Any] = {}
        for stream_name in Config.STREAMS:
            try:
                stream_stats[stream_name] = {
                    "length": r.xlen(stream_name),
                    "groups": r.xinfo_groups(stream_name),
                }
            except Exception:
                pass
        self._send(200, {"redis_info": info, "stream_stats": stream_stats})

    def _send(self, code: int, body: dict) -> None:
        data = json.dumps(body, default=str).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def start_debug_server(r: redis.Redis) -> None:
    global _redis_client
    if not Config.DEBUG_MODE:
        return
    _redis_client = r
    server = HTTPServer(("0.0.0.0", Config.DEBUG_PORT), DebugHandler)

    def _run() -> None:
        try:
            server.serve_forever()
        except Exception as exc:
            logger.error("Debug server error", extra_fields={"error": str(exc)})

    t = threading.Thread(target=_run, name="debug-server", daemon=True)
    t.start()
    logger.info("Debug server started", extra_fields={"port": Config.DEBUG_PORT})
