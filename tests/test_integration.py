"""Integration tests — require a live Redis instance on DB 15.

These tests seed raw hashes and indexes directly, then call refresh_order
and assert the cached view contains the expected denormalized fields.
"""
from __future__ import annotations

import json

import pytest

from app.indexes import update_indexes
from app.join_builder import build_order_view, refresh_order


@pytest.mark.integration
class TestFullOrderRefresh:
    def _seed(self, r):
        """Seed raw hashes and indexes for one order."""
        # Raw hashes
        r.hset("raw:customers:cust-1", mapping={"customer_id": "cust-1", "name": "Alice", "email": "alice@example.com"})
        r.hset("raw:products:prod-1", mapping={"product_id": "prod-1", "name": "Widget", "sku": "W-001", "price": "9.99"})
        r.hset("raw:products:prod-2", mapping={"product_id": "prod-2", "name": "Gadget", "sku": "G-001", "price": "49.99"})
        r.hset("raw:orders:ord-1", mapping={
            "order_id": "ord-1",
            "customer_id": "cust-1",
            "status": "confirmed",
            "created_at": "2024-01-01",
            "updated_at": "2024-01-01",
        })
        r.hset("raw:order_items:item-1", mapping={
            "item_id": "item-1",
            "order_id": "ord-1",
            "product_id": "prod-1",
            "quantity": "1",
            "unit_price": "9.99",
        })
        r.hset("raw:order_items:item-2", mapping={
            "item_id": "item-2",
            "order_id": "ord-1",
            "product_id": "prod-2",
            "quantity": "2",
            "unit_price": "49.99",
        })
        r.hset("raw:shipping:ship-1", mapping={
            "shipping_id": "ship-1",
            "order_id": "ord-1",
            "carrier": "FedEx",
            "tracking_number": "FX1",
            "status": "shipped",
            "shipped_at": "2024-01-02",
            "delivered_at": "",
        })

        # Indexes
        update_indexes(r, "orders", {"order_id": "ord-1", "customer_id": "cust-1"}, False)
        update_indexes(r, "order_items", {"item_id": "item-1", "order_id": "ord-1", "product_id": "prod-1"}, False)
        update_indexes(r, "order_items", {"item_id": "item-2", "order_id": "ord-1", "product_id": "prod-2"}, False)
        update_indexes(r, "shipping", {"shipping_id": "ship-1", "order_id": "ord-1"}, False)

    def test_refresh_builds_correct_view(self, real_redis):
        self._seed(real_redis)
        refresh_order(real_redis, "ord-1")

        view = real_redis.hgetall("cache:order:ord-1")

        assert view["order_id"] == "ord-1"
        assert view["customer_name"] == "Alice"
        assert view["customer_email"] == "alice@example.com"
        assert view["status"] == "confirmed"
        assert float(view["total_amount"]) == pytest.approx(109.97, rel=1e-3)
        assert int(view["item_count"]) == 2

    def test_refresh_items_json(self, real_redis):
        self._seed(real_redis)
        refresh_order(real_redis, "ord-1")
        view = real_redis.hgetall("cache:order:ord-1")
        items = json.loads(view["items"])
        assert len(items) == 2
        product_names = {i["product_name"] for i in items}
        assert "Widget" in product_names
        assert "Gadget" in product_names

    def test_refresh_shipping_json(self, real_redis):
        self._seed(real_redis)
        refresh_order(real_redis, "ord-1")
        view = real_redis.hgetall("cache:order:ord-1")
        shipping = json.loads(view["shipping"])
        assert len(shipping) == 1
        assert shipping[0]["carrier"] == "FedEx"

    def test_ttl_is_set(self, real_redis):
        self._seed(real_redis)
        refresh_order(real_redis, "ord-1")
        ttl = real_redis.ttl("cache:order:ord-1")
        assert ttl > 0

    def test_missing_order_deletes_view(self, real_redis):
        # Pre-populate stale view
        real_redis.hset("cache:order:ghost-1", mapping={"order_id": "ghost-1"})
        refresh_order(real_redis, "ghost-1")
        assert not real_redis.exists("cache:order:ghost-1")
