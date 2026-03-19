"""Tests for app.models — DebeziumEvent parsing and properties."""
from __future__ import annotations

import json

import pytest

from app.models import DebeziumEvent


class TestEnvelopeParsing:
    def test_full_envelope_create(self, envelope_message):
        evt = DebeziumEvent.from_stream_message(
            stream="stream:orders",
            message_id="1-1",
            table="orders",
            data=envelope_message,
        )
        assert evt.operation == "c"
        assert evt.after["order_id"] == "ord-1"
        assert evt.before is None
        assert evt.ts_ms == 1700000000000
        assert not evt.is_delete

    def test_full_envelope_payload_property(self, envelope_message):
        evt = DebeziumEvent.from_stream_message("stream:orders", "1-1", "orders", envelope_message)
        assert evt.payload["order_id"] == "ord-1"

    def test_full_envelope_source(self, envelope_message):
        evt = DebeziumEvent.from_stream_message("stream:orders", "1-1", "orders", envelope_message)
        assert evt.source.get("table") == "orders"


class TestFlattenedParsing:
    def test_flattened_create(self, flattened_message):
        evt = DebeziumEvent.from_stream_message(
            "stream:orders", "2-1", "orders", flattened_message
        )
        assert evt.operation == "c"
        assert evt.after["order_id"] == "ord-2"
        assert evt.after["customer_id"] == "cust-2"

    def test_flattened_payload_property(self, flattened_message):
        evt = DebeziumEvent.from_stream_message("stream:orders", "2-1", "orders", flattened_message)
        assert evt.payload["status"] == "pending"

    def test_flat_format_no_prefix(self):
        """Test flat format where keys have no before_/after_ prefix."""
        data = {
            "op": "r",
            "order_id": "ord-99",
            "customer_id": "cust-99",
            "ts_ms": "0",
        }
        evt = DebeziumEvent.from_stream_message("stream:orders", "3-1", "orders", data)
        assert evt.payload["order_id"] == "ord-99"


class TestDeleteDetection:
    def test_is_delete_true(self, delete_message):
        evt = DebeziumEvent.from_stream_message("stream:orders", "4-1", "orders", delete_message)
        assert evt.is_delete is True

    def test_delete_payload_uses_before(self, delete_message):
        evt = DebeziumEvent.from_stream_message("stream:orders", "4-1", "orders", delete_message)
        assert evt.payload["order_id"] == "ord-3"

    def test_non_delete_payload_uses_after(self, flattened_message):
        evt = DebeziumEvent.from_stream_message("stream:orders", "2-1", "orders", flattened_message)
        assert not evt.is_delete
        assert evt.payload == evt.after

    def test_empty_payload_returns_empty_dict(self):
        data = {"op": "d", "ts_ms": "0"}
        evt = DebeziumEvent.from_stream_message("stream:orders", "5-1", "orders", data)
        assert evt.payload == {}

    def test_envelope_delete(self):
        data = {
            "payload": json.dumps(
                {
                    "op": "d",
                    "before": {"order_id": "ord-del", "status": "old"},
                    "after": None,
                    "ts_ms": 999,
                }
            )
        }
        evt = DebeziumEvent.from_stream_message("stream:orders", "6-1", "orders", data)
        assert evt.is_delete
        assert evt.payload["order_id"] == "ord-del"
