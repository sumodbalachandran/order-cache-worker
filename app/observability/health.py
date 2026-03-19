from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

from app.config import Config
from app.observability.logger import get_logger

logger = get_logger(__name__)

# --------------------------------------------------------------------------- #
# Health state                                                                  #
# --------------------------------------------------------------------------- #

_health_state: dict[str, Any] = {
    "redis_connected": False,
    "consumer_group_ready": False,
    "start_time": time.time(),
    "last_event_ts": None,
    "error_count": 0,
    "events_processed": 0,
}
_state_lock = threading.Lock()


def update_health(
    redis_connected: bool | None = None,
    consumer_group_ready: bool | None = None,
) -> None:
    with _state_lock:
        if redis_connected is not None:
            _health_state["redis_connected"] = redis_connected
        if consumer_group_ready is not None:
            _health_state["consumer_group_ready"] = consumer_group_ready


def record_health_event_processed() -> None:
    with _state_lock:
        _health_state["events_processed"] += 1
        _health_state["last_event_ts"] = time.time()


def record_health_error() -> None:
    with _state_lock:
        _health_state["error_count"] += 1


# --------------------------------------------------------------------------- #
# HTTP handler                                                                  #
# --------------------------------------------------------------------------- #

class HealthHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args) -> None:  # noqa: A002
        pass  # suppress default access logs

    def do_GET(self) -> None:
        path = self.path.rstrip("/")
        if path == "/health":
            self._liveness()
        elif path == "/health/ready":
            self._readiness()
        elif path == "/health/details":
            self._details()
        else:
            self._send(404, {"error": "not found"})

    def _liveness(self) -> None:
        self._send(200, {"status": "alive"})

    def _readiness(self) -> None:
        with _state_lock:
            redis_ok = _health_state["redis_connected"]
            group_ok = _health_state["consumer_group_ready"]
            errors = _health_state["error_count"]

        ready = redis_ok and group_ok and errors < 10
        code = 200 if ready else 503
        self._send(
            code,
            {
                "status": "ready" if ready else "not_ready",
                "redis_connected": redis_ok,
                "consumer_group_ready": group_ok,
                "error_count": errors,
            },
        )

    def _details(self) -> None:
        with _state_lock:
            state = dict(_health_state)
        uptime = time.time() - state.get("start_time", time.time())
        state["uptime_seconds"] = round(uptime, 1)
        self._send(200, state)

    def _send(self, code: int, body: dict) -> None:
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


# --------------------------------------------------------------------------- #
# Server                                                                        #
# --------------------------------------------------------------------------- #

def start_health_server() -> None:
    server = HTTPServer(("0.0.0.0", Config.HEALTH_PORT), HealthHandler)

    def _run() -> None:
        try:
            server.serve_forever()
        except Exception as exc:
            logger.error("Health server error", extra_fields={"error": str(exc)})

    t = threading.Thread(target=_run, name="health-server", daemon=True)
    t.start()
    logger.info("Health server started", extra_fields={"port": Config.HEALTH_PORT})
