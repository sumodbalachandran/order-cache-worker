from __future__ import annotations

from typing import Any

import redis

from app.observability.logger import get_logger

logger = get_logger(__name__)


def update_indexes(
    r: redis.Redis,
    table: str,
    payload: dict[str, Any],
    is_delete: bool,
    pipe: redis.client.Pipeline | None = None,
) -> None:
    """Maintain reverse look-up indexes for the five CDC tables.

    Indexes maintained:
      orders      → idx:customer_orders:{customer_id}   (Set of order_ids)
      order_items → idx:order_items:{order_id}          (Set of item_ids)
                    idx:item_order:{item_id}             (String → order_id)
                    idx:product_items:{product_id}       (Set of item_ids)
      shipping    → idx:order_shipping:{order_id}        (Set of shipping_ids)
                    idx:shipping_order:{shipping_id}     (String → order_id)
    """
    executor = pipe if pipe is not None else r

    if table == "orders":
        _update_orders_indexes(executor, payload, is_delete)
    elif table == "order_items":
        _update_order_items_indexes(executor, payload, is_delete)
    elif table == "shipping":
        _update_shipping_indexes(executor, payload, is_delete)
    # customers and products have no dedicated reverse indexes here;
    # they are looked up via the orders/order_items indexes.


# --------------------------------------------------------------------------- #
# Private helpers                                                               #
# --------------------------------------------------------------------------- #

def _update_orders_indexes(
    executor: Any,
    payload: dict[str, Any],
    is_delete: bool,
) -> None:
    order_id = payload.get("order_id") or payload.get("id")
    customer_id = payload.get("customer_id")
    if not order_id or not customer_id:
        return
    key = f"idx:customer_orders:{customer_id}"
    if is_delete:
        executor.srem(key, str(order_id))
    else:
        executor.sadd(key, str(order_id))


def _update_order_items_indexes(
    executor: Any,
    payload: dict[str, Any],
    is_delete: bool,
) -> None:
    item_id = payload.get("item_id") or payload.get("id")
    order_id = payload.get("order_id")
    product_id = payload.get("product_id")
    if not item_id:
        return

    if order_id:
        key_order_items = f"idx:order_items:{order_id}"
        key_item_order = f"idx:item_order:{item_id}"
        if is_delete:
            executor.srem(key_order_items, str(item_id))
            executor.delete(key_item_order)
        else:
            executor.sadd(key_order_items, str(item_id))
            executor.set(key_item_order, str(order_id))

    if product_id:
        key_product_items = f"idx:product_items:{product_id}"
        if is_delete:
            executor.srem(key_product_items, str(item_id))
        else:
            executor.sadd(key_product_items, str(item_id))


def _update_shipping_indexes(
    executor: Any,
    payload: dict[str, Any],
    is_delete: bool,
) -> None:
    shipping_id = payload.get("shipping_id") or payload.get("id")
    order_id = payload.get("order_id")
    if not shipping_id or not order_id:
        return

    key_order_shipping = f"idx:order_shipping:{order_id}"
    key_shipping_order = f"idx:shipping_order:{shipping_id}"
    if is_delete:
        executor.srem(key_order_shipping, str(shipping_id))
        executor.delete(key_shipping_order)
    else:
        executor.sadd(key_order_shipping, str(shipping_id))
        executor.set(key_shipping_order, str(order_id))
