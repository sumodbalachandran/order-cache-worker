"""Tests for app.event_router — affected order_id routing."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from app.event_router import get_affected_order_ids


class TestOrdersRouting:
    def test_direct_order_id(self, mock_redis, sample_order_payload):
        result = get_affected_order_ids(mock_redis, "orders", sample_order_payload)
        assert result == {"ord-1"}

    def test_id_fallback(self, mock_redis):
        result = get_affected_order_ids(mock_redis, "orders", {"id": "ord-99"})
        assert result == {"ord-99"}

    def test_empty_payload(self, mock_redis):
        result = get_affected_order_ids(mock_redis, "orders", {})
        assert result == set()


class TestCustomersRouting:
    def test_fans_out_to_all_orders(self, mock_redis, sample_customer_payload):
        mock_redis.smembers.return_value = {"ord-1", "ord-2"}
        result = get_affected_order_ids(mock_redis, "customers", sample_customer_payload)
        mock_redis.smembers.assert_called_once_with("idx:customer_orders:cust-1")
        assert result == {"ord-1", "ord-2"}

    def test_empty_customer_orders(self, mock_redis, sample_customer_payload):
        mock_redis.smembers.return_value = set()
        result = get_affected_order_ids(mock_redis, "customers", sample_customer_payload)
        assert result == set()

    def test_missing_customer_id(self, mock_redis):
        result = get_affected_order_ids(mock_redis, "customers", {})
        assert result == set()


class TestProductsRouting:
    def test_routes_via_items(self, mock_redis, sample_product_payload):
        mock_redis.smembers.return_value = {"item-1", "item-2"}
        pipe = MagicMock()
        pipe.execute.return_value = ["ord-1", "ord-2"]
        mock_redis.pipeline.return_value = pipe

        result = get_affected_order_ids(mock_redis, "products", sample_product_payload)
        assert "ord-1" in result
        assert "ord-2" in result

    def test_no_items_for_product(self, mock_redis, sample_product_payload):
        mock_redis.smembers.return_value = set()
        result = get_affected_order_ids(mock_redis, "products", sample_product_payload)
        assert result == set()

    def test_nil_item_order_ignored(self, mock_redis, sample_product_payload):
        mock_redis.smembers.return_value = {"item-orphan"}
        pipe = MagicMock()
        pipe.execute.return_value = [None]
        mock_redis.pipeline.return_value = pipe
        result = get_affected_order_ids(mock_redis, "products", sample_product_payload)
        assert result == set()


class TestOrderItemsRouting:
    def test_direct_order_id(self, mock_redis, sample_order_item_payload):
        result = get_affected_order_ids(mock_redis, "order_items", sample_order_item_payload)
        assert result == {"ord-1"}

    def test_missing_order_id(self, mock_redis):
        result = get_affected_order_ids(mock_redis, "order_items", {"item_id": "item-1"})
        assert result == set()


class TestShippingRouting:
    def test_direct_order_id(self, mock_redis, sample_shipping_payload):
        result = get_affected_order_ids(mock_redis, "shipping", sample_shipping_payload)
        assert result == {"ord-1"}

    def test_missing_order_id(self, mock_redis):
        result = get_affected_order_ids(mock_redis, "shipping", {"shipping_id": "ship-1"})
        assert result == set()


class TestUnknownTableRouting:
    def test_unknown_table_returns_empty(self, mock_redis):
        result = get_affected_order_ids(mock_redis, "nonexistent", {"id": "1"})
        assert result == set()
