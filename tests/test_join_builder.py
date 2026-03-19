"""Tests for app.join_builder — order view building, writing, deleting."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, call, patch

import pytest

from app.join_builder import (
    build_order_view,
    delete_order_view,
    refresh_order,
    write_order_view,
)


def _make_pipeline(*result_sets):
    """Build a pipeline mock whose execute() returns successive result sets."""
    pipe = MagicMock()
    pipe.__enter__ = lambda s: s
    pipe.__exit__ = MagicMock(return_value=False)
    pipe.execute.side_effect = list(result_sets)
    return pipe


class TestBuildOrderView:
    def _setup_redis(self, mock_redis, order, customer, items, shipping, products):
        """Configure pipeline execute() side effects for 3-round-trip build."""
        pipe = MagicMock()
        pipe.__enter__ = lambda s: s
        pipe.__exit__ = MagicMock(return_value=False)

        item_hashes = [items[iid] for iid in sorted(items)]
        shipping_hashes = [shipping[sid] for sid in sorted(shipping)]
        product_hashes = [products[pid] for pid in sorted(products)]

        # Round-trip 1
        rt1 = [
            order,
            None,
            set(items.keys()),
            set(shipping.keys()),
        ]
        # Round-trip 2
        rt2 = [customer] + item_hashes + shipping_hashes
        # Round-trip 3
        rt3 = product_hashes

        pipe.execute.side_effect = [rt1, rt2, rt3]
        mock_redis.pipeline.return_value = pipe
        return pipe

    def test_build_complete_view(self, mock_redis):
        order = {
            "order_id": "ord-1",
            "customer_id": "cust-1",
            "status": "confirmed",
            "created_at": "2024-01-01",
            "updated_at": "2024-01-01",
        }
        customer = {"name": "Alice", "email": "alice@example.com"}
        items = {
            "item-1": {
                "item_id": "item-1",
                "product_id": "prod-1",
                "quantity": "2",
                "unit_price": "10.00",
            }
        }
        shipping = {
            "ship-1": {
                "shipping_id": "ship-1",
                "carrier": "UPS",
                "tracking_number": "1Z",
                "status": "shipped",
                "shipped_at": "2024-01-02",
                "delivered_at": "",
            }
        }
        products = {"prod-1": {"name": "Widget", "sku": "W-001"}}
        self._setup_redis(mock_redis, order, customer, items, shipping, products)

        view = build_order_view(mock_redis, "ord-1")

        assert view is not None
        assert view["order_id"] == "ord-1"
        assert view["customer_name"] == "Alice"
        assert view["customer_email"] == "alice@example.com"
        assert view["total_amount"] == 20.0
        assert view["item_count"] == 1

        items_parsed = json.loads(view["items"])
        assert len(items_parsed) == 1
        assert items_parsed[0]["product_name"] == "Widget"
        assert items_parsed[0]["line_total"] == 20.0

        shipping_parsed = json.loads(view["shipping"])
        assert len(shipping_parsed) == 1
        assert shipping_parsed[0]["carrier"] == "UPS"

    def test_returns_none_when_order_missing(self, mock_redis):
        pipe = MagicMock()
        pipe.__enter__ = lambda s: s
        pipe.__exit__ = MagicMock(return_value=False)
        pipe.execute.return_value = [{}, None, set(), set()]
        mock_redis.pipeline.return_value = pipe

        view = build_order_view(mock_redis, "nonexistent")
        assert view is None

    def test_total_amount_multi_items(self, mock_redis):
        order = {"order_id": "ord-2", "customer_id": "cust-2", "status": "new",
                 "created_at": "", "updated_at": ""}
        customer = {"name": "Bob", "email": "bob@example.com"}
        items = {
            "item-A": {"item_id": "item-A", "product_id": "p1", "quantity": "3", "unit_price": "5.00"},
            "item-B": {"item_id": "item-B", "product_id": "p2", "quantity": "1", "unit_price": "50.00"},
        }
        shipping = {}
        products = {
            "p1": {"name": "Pen", "sku": "P1"},
            "p2": {"name": "Notebook", "sku": "N1"},
        }
        self._setup_redis(mock_redis, order, customer, items, shipping, products)
        view = build_order_view(mock_redis, "ord-2")
        assert view is not None
        assert view["total_amount"] == 65.0


class TestWriteOrderView:
    def test_write_uses_atomic_pipeline(self, mock_redis):
        pipe = MagicMock()
        pipe.__enter__ = lambda s: s
        pipe.__exit__ = MagicMock(return_value=False)
        mock_redis.pipeline.return_value = pipe

        view = {"order_id": "ord-1", "status": "ok"}
        write_order_view(mock_redis, "ord-1", view)

        pipe.delete.assert_called_once_with("cache:order:ord-1")
        pipe.hset.assert_called_once()
        pipe.expire.assert_called_once()
        pipe.execute.assert_called_once()

    def test_ttl_is_set(self, mock_redis):
        from app.config import Config

        pipe = MagicMock()
        pipe.__enter__ = lambda s: s
        pipe.__exit__ = MagicMock(return_value=False)
        mock_redis.pipeline.return_value = pipe

        write_order_view(mock_redis, "ord-1", {"order_id": "ord-1"})
        pipe.expire.assert_called_once_with("cache:order:ord-1", Config.CACHE_TTL_SECONDS)


class TestDeleteOrderView:
    def test_delete_calls_redis_delete(self, mock_redis):
        delete_order_view(mock_redis, "ord-1")
        mock_redis.delete.assert_called_once_with("cache:order:ord-1")


class TestRefreshOrder:
    def test_refresh_writes_when_view_available(self, mock_redis):
        order = {"order_id": "ord-1", "customer_id": "c1", "status": "ok",
                 "created_at": "", "updated_at": ""}
        customer = {"name": "X", "email": "x@x.com"}
        pipe = MagicMock()
        pipe.__enter__ = lambda s: s
        pipe.__exit__ = MagicMock(return_value=False)
        pipe.execute.side_effect = [
            [order, None, set(), set()],
            [customer],
            [],
        ]
        mock_redis.pipeline.return_value = pipe

        with patch("app.join_builder.write_order_view") as mock_write:
            refresh_order(mock_redis, "ord-1")
            mock_write.assert_called_once()

    def test_refresh_deletes_when_order_missing(self, mock_redis):
        pipe = MagicMock()
        pipe.__enter__ = lambda s: s
        pipe.__exit__ = MagicMock(return_value=False)
        pipe.execute.return_value = [{}, None, set(), set()]
        mock_redis.pipeline.return_value = pipe

        with patch("app.join_builder.delete_order_view") as mock_del:
            refresh_order(mock_redis, "nonexistent")
            mock_del.assert_called_once_with(mock_redis, "nonexistent")
