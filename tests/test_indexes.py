"""Tests for app.indexes — reverse index creation and deletion."""
from __future__ import annotations

from unittest.mock import MagicMock, call

import pytest

from app.indexes import update_indexes


class TestOrdersIndexes:
    def test_create_adds_to_customer_orders(self, mock_redis, sample_order_payload):
        update_indexes(mock_redis, "orders", sample_order_payload, is_delete=False)
        mock_redis.sadd.assert_called_once_with("idx:customer_orders:cust-1", "ord-1")

    def test_delete_removes_from_customer_orders(self, mock_redis, sample_order_payload):
        update_indexes(mock_redis, "orders", sample_order_payload, is_delete=True)
        mock_redis.srem.assert_called_once_with("idx:customer_orders:cust-1", "ord-1")

    def test_missing_customer_id_is_noop(self, mock_redis):
        update_indexes(mock_redis, "orders", {"order_id": "ord-x"}, is_delete=False)
        mock_redis.sadd.assert_not_called()

    def test_missing_order_id_is_noop(self, mock_redis):
        update_indexes(mock_redis, "orders", {"customer_id": "cust-x"}, is_delete=False)
        mock_redis.sadd.assert_not_called()


class TestOrderItemsIndexes:
    def test_create_adds_all_indexes(self, mock_redis, sample_order_item_payload):
        update_indexes(mock_redis, "order_items", sample_order_item_payload, is_delete=False)
        mock_redis.sadd.assert_any_call("idx:order_items:ord-1", "item-1")
        mock_redis.set.assert_any_call("idx:item_order:item-1", "ord-1")
        mock_redis.sadd.assert_any_call("idx:product_items:prod-1", "item-1")

    def test_delete_removes_all_indexes(self, mock_redis, sample_order_item_payload):
        update_indexes(mock_redis, "order_items", sample_order_item_payload, is_delete=True)
        mock_redis.srem.assert_any_call("idx:order_items:ord-1", "item-1")
        mock_redis.delete.assert_any_call("idx:item_order:item-1")
        mock_redis.srem.assert_any_call("idx:product_items:prod-1", "item-1")

    def test_missing_item_id_is_noop(self, mock_redis):
        update_indexes(mock_redis, "order_items", {"order_id": "ord-1"}, is_delete=False)
        mock_redis.sadd.assert_not_called()

    def test_uses_pipeline_when_provided(self, mock_redis, sample_order_item_payload):
        pipe = MagicMock()
        update_indexes(mock_redis, "order_items", sample_order_item_payload, is_delete=False, pipe=pipe)
        pipe.sadd.assert_called()


class TestShippingIndexes:
    def test_create_adds_shipping_indexes(self, mock_redis, sample_shipping_payload):
        update_indexes(mock_redis, "shipping", sample_shipping_payload, is_delete=False)
        mock_redis.sadd.assert_called_with("idx:order_shipping:ord-1", "ship-1")
        mock_redis.set.assert_called_with("idx:shipping_order:ship-1", "ord-1")

    def test_delete_removes_shipping_indexes(self, mock_redis, sample_shipping_payload):
        update_indexes(mock_redis, "shipping", sample_shipping_payload, is_delete=True)
        mock_redis.srem.assert_called_with("idx:order_shipping:ord-1", "ship-1")
        mock_redis.delete.assert_called_with("idx:shipping_order:ship-1")

    def test_missing_fields_is_noop(self, mock_redis):
        update_indexes(mock_redis, "shipping", {}, is_delete=False)
        mock_redis.sadd.assert_not_called()


class TestUnknownTable:
    def test_unknown_table_is_noop(self, mock_redis):
        update_indexes(mock_redis, "unknown_table", {"id": "1"}, is_delete=False)
        mock_redis.sadd.assert_not_called()


class TestCustomersAndProductsAreNoop:
    def test_customers_no_indexes(self, mock_redis, sample_customer_payload):
        update_indexes(mock_redis, "customers", sample_customer_payload, is_delete=False)
        mock_redis.sadd.assert_not_called()

    def test_products_no_indexes(self, mock_redis, sample_product_payload):
        update_indexes(mock_redis, "products", sample_product_payload, is_delete=False)
        mock_redis.sadd.assert_not_called()
