from __future__ import annotations

from contextlib import contextmanager
from typing import Any

from app.config import Config
from app.observability.logger import get_logger

logger = get_logger(__name__)

_tracer = None
_provider = None


class SpanAttributes:
    """Constants for span attribute keys."""

    ORDER_ID = "order.id"
    STREAM = "messaging.destination"
    TABLE = "db.table"
    MESSAGE_ID = "messaging.message_id"
    OPERATION = "db.operation"
    ERROR = "error"


def setup_tracing() -> None:
    global _tracer, _provider

    if not Config.OTEL_ENABLED:
        logger.info("OpenTelemetry tracing disabled")
        return

    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
            OTLPSpanExporter,
        )
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
        from opentelemetry.sdk.trace.sampling import ParentBasedTraceIdRatio

        resource = Resource.create(
            {
                "service.name": Config.OTEL_SERVICE_NAME,
                "service.version": "1.0.0",
            }
        )
        sampler = ParentBasedTraceIdRatio(Config.OTEL_SAMPLE_RATE)
        provider = TracerProvider(resource=resource, sampler=sampler)

        # Try OTLP exporter; fall back to console
        try:
            exporter = OTLPSpanExporter(endpoint=Config.OTEL_EXPORTER_ENDPOINT, insecure=True)
            provider.add_span_processor(BatchSpanProcessor(exporter))
            logger.info(
                "OTLP span exporter configured",
                extra_fields={"endpoint": Config.OTEL_EXPORTER_ENDPOINT},
            )
        except Exception as exc:
            logger.warning(
                "Failed to create OTLP exporter; falling back to console",
                extra_fields={"error": str(exc)},
            )
            provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))

        trace.set_tracer_provider(provider)
        _provider = provider
        _tracer = trace.get_tracer(Config.OTEL_SERVICE_NAME)
        logger.info("OpenTelemetry tracing configured")

    except ImportError as exc:
        logger.warning(
            "OpenTelemetry SDK not available; tracing disabled",
            extra_fields={"error": str(exc)},
        )


def get_tracer():
    global _tracer
    if _tracer is None:
        try:
            from opentelemetry import trace

            _tracer = trace.get_tracer(Config.OTEL_SERVICE_NAME)
        except ImportError:
            return None
    return _tracer


@contextmanager
def trace_span(name: str, attributes: dict[str, Any] | None = None):
    """Context manager that creates an OTel span if tracing is available."""
    tracer = get_tracer()
    if tracer is None:
        yield
        return

    try:
        from opentelemetry import trace
        from opentelemetry.trace.status import Status, StatusCode

        with tracer.start_as_current_span(name) as span:
            if attributes:
                for k, v in attributes.items():
                    span.set_attribute(k, str(v))
            try:
                yield span
            except Exception as exc:
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR, str(exc)))
                raise
    except ImportError:
        yield
