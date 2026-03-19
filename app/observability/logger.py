from __future__ import annotations

import json
import logging
import logging.handlers
import sys
import traceback
from contextvars import ContextVar
from typing import Any

from app.config import Config

# --------------------------------------------------------------------------- #
# Context variables                                                             #
# --------------------------------------------------------------------------- #
ctx_stream: ContextVar[str] = ContextVar("ctx_stream", default="")
ctx_table: ContextVar[str] = ContextVar("ctx_table", default="")
ctx_message_id: ContextVar[str] = ContextVar("ctx_message_id", default="")
ctx_order_id: ContextVar[str] = ContextVar("ctx_order_id", default="")
ctx_operation: ContextVar[str] = ContextVar("ctx_operation", default="")


# --------------------------------------------------------------------------- #
# Formatters                                                                    #
# --------------------------------------------------------------------------- #

def _get_trace_info() -> tuple[str, str]:
    """Try to extract trace/span id from the active OTel span."""
    try:
        from opentelemetry import trace

        span = trace.get_current_span()
        ctx = span.get_span_context()
        if ctx and ctx.is_valid:
            return format(ctx.trace_id, "032x"), format(ctx.span_id, "016x")
    except Exception:
        pass
    return "", ""


class JSONFormatter(logging.Formatter):
    """Single-line JSON log records enriched with trace, span and context vars."""

    def format(self, record: logging.LogRecord) -> str:
        trace_id, span_id = _get_trace_info()
        doc: dict[str, Any] = {
            "ts": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "trace_id": trace_id,
            "span_id": span_id,
            "stream": ctx_stream.get(""),
            "table": ctx_table.get(""),
            "message_id": ctx_message_id.get(""),
            "order_id": ctx_order_id.get(""),
            "operation": ctx_operation.get(""),
        }
        # extra_fields injected by ContextLogger
        extra = getattr(record, "extra_fields", {})
        if extra:
            doc.update(extra)
        if record.exc_info:
            doc["exception"] = self.formatException(record.exc_info)
        return json.dumps(doc, default=str)


class TextFormatter(logging.Formatter):
    """Human-readable formatter with optional trace prefix."""

    _FMT = "%(asctime)s [%(levelname)-8s] %(name)s %(message)s"
    _DATEFMT = "%Y-%m-%dT%H:%M:%S"

    def __init__(self) -> None:
        super().__init__(fmt=self._FMT, datefmt=self._DATEFMT)

    def format(self, record: logging.LogRecord) -> str:
        trace_id, span_id = _get_trace_info()
        prefix = f"[{trace_id[:8]}..{span_id[:8]}] " if trace_id else ""
        extra = getattr(record, "extra_fields", {})
        if extra:
            record.msg = f"{record.msg} | {extra}"
        base = super().format(record)
        return f"{prefix}{base}"


# --------------------------------------------------------------------------- #
# ContextLogger                                                                 #
# --------------------------------------------------------------------------- #

class ContextLogger(logging.LoggerAdapter):
    """LoggerAdapter that accepts an ``extra_fields`` keyword for structured logging."""

    def process(self, msg: Any, kwargs: dict) -> tuple[Any, dict]:
        extra_fields = kwargs.pop("extra_fields", {})
        kwargs.setdefault("extra", {})["extra_fields"] = extra_fields
        return msg, kwargs

    # Convenience overrides to surface extra_fields in all level methods
    def debug(self, msg, *args, **kwargs) -> None:  # type: ignore[override]
        self.log(logging.DEBUG, msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs) -> None:  # type: ignore[override]
        self.log(logging.INFO, msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs) -> None:  # type: ignore[override]
        self.log(logging.WARNING, msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs) -> None:  # type: ignore[override]
        self.log(logging.ERROR, msg, *args, **kwargs)

    def critical(self, msg, *args, **kwargs) -> None:  # type: ignore[override]
        self.log(logging.CRITICAL, msg, *args, **kwargs)

    def exception(self, msg, *args, **kwargs) -> None:  # type: ignore[override]
        kwargs.setdefault("exc_info", True)
        self.log(logging.ERROR, msg, *args, **kwargs)


# --------------------------------------------------------------------------- #
# Setup                                                                         #
# --------------------------------------------------------------------------- #

_configured = False


def setup_logging() -> None:
    global _configured
    if _configured:
        return
    _configured = True

    level = getattr(logging, Config.LOG_LEVEL.upper(), logging.INFO)
    formatter: logging.Formatter = (
        JSONFormatter() if Config.LOG_FORMAT.lower() == "json" else TextFormatter()
    )

    root = logging.getLogger()
    root.setLevel(level)

    # Remove any existing handlers to avoid duplicate logs
    root.handlers.clear()

    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    root.addHandler(console)

    # Optional rotating file handler
    if Config.LOG_FILE:
        file_handler = logging.handlers.RotatingFileHandler(
            Config.LOG_FILE,
            maxBytes=Config.LOG_MAX_BYTES,
            backupCount=Config.LOG_BACKUP_COUNT,
        )
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)

    # Quieten noisy libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("redis").setLevel(logging.WARNING)


def get_logger(name: str) -> ContextLogger:
    return ContextLogger(logging.getLogger(name), extra={})
