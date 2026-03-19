"""Shared pytest fixtures for the Order Cache Worker test suite."""
from __future__ import annotations

import os
from unittest.mock import MagicMock

import pytest

# ------------------------------------------------------------------ #
# Mock Redis fixture (unit tests)                                      #
# ------------------------------------------------------------------ #

@pytest.fixture
def mock_redis():
    """Return a MagicMock that mimics a redis.Redis instance."""
    r = MagicMock()
    # smembers returns a set by default
    r.smembers.return_value = set()
    r.hgetall.return_value = {}
    r.get.return_value = None
    # Pipeline mock
    pipe = MagicMock()
    pipe.__enter__ = lambda s: s
    pipe.__exit__ = MagicMock(return_value=False)
    pipe.execute.return_value = []
    r.pipeline.return_value = pipe
    return r


# ------------------------------------------------------------------ #
# Real Redis fixture (integration tests — skipped if unavailable)     #
# ------------------------------------------------------------------ #

@pytest.fixture
def real_redis():
    """Connect to a real Redis instance on DB 15 (test isolation)."""
    redis = pytest.importorskip("redis")
    host = os.getenv("REDIS_HOST", "127.0.0.1")
    port = int(os.getenv("REDIS_PORT", 6379))
    r = redis.Redis(host=host, port=port, db=15, decode_responses=True)
    try:
        r.ping()
    except Exception:
        pytest.skip("Redis not available")
    r.flushdb()
    yield r
    r.flushdb()
    r.close()


# ------------------------------------------------------------------ #
# Sample event payloads                                                #
# ------------------------------------------------------------------ #

@pytest.fixture
def sample_order_payload():
    return {
        "order_id": "ord-1",
        "customer_id": "cust-1",
        "status": "confirmed",
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z",
    }


@pytest.fixture
def sample_customer_payload():
    return {
        "customer_id": "cust-1",
        "name": "Alice",
        "email": "alice@example.com",
    }


@pytest.fixture
def sample_product_payload():
    return {
        "product_id": "prod-1",
        "name": "Widget",
        "sku": "W-001",
        "price": "9.99",
    }


@pytest.fixture
def sample_order_item_payload():
    return {
        "item_id": "item-1",
        "order_id": "ord-1",
        "product_id": "prod-1",
        "quantity": "2",
        "unit_price": "9.99",
    }


@pytest.fixture
def sample_shipping_payload():
    return {
        "shipping_id": "ship-1",
        "order_id": "ord-1",
        "carrier": "UPS",
        "tracking_number": "1Z999",
        "status": "shipped",
        "shipped_at": "2024-01-02T00:00:00Z",
        "delivered_at": "",
    }


# ------------------------------------------------------------------ #
# Sample stream messages                                               #
# ------------------------------------------------------------------ #

@pytest.fixture
def envelope_message():
    """Debezium full-envelope format."""
    import json

    return {
        "payload": json.dumps(
            {
                "op": "c",
                "after": {
                    "order_id": "ord-1",
                    "customer_id": "cust-1",
                    "status": "new",
                },
                "before": None,
                "source": {"table": "orders"},
                "ts_ms": 1700000000000,
            }
        )
    }


@pytest.fixture
def flattened_message():
    """Flattened SMT format with after_ prefix."""
    return {
        "op": "c",
        "after_order_id": "ord-2",
        "after_customer_id": "cust-2",
        "after_status": "pending",
        "ts_ms": "1700000000001",
    }


@pytest.fixture
def delete_message():
    """Delete event with before_ payload."""
    return {
        "op": "d",
        "before_order_id": "ord-3",
        "before_customer_id": "cust-3",
        "before_status": "cancelled",
        "ts_ms": "1700000000002",
    }
