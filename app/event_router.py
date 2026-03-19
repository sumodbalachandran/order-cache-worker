from __future__ import annotations

from typing import Any

import redis

from app.observability.logger import get_logger

logger = get_logger(__name__)


def get_affected_order_ids(
    r: redis.Redis,
    table: str,
    payload: dict[str, Any],
) -> set[str]:
    """Return the set of order_ids affected by a CDC event on *table*.

    Routing logic per table:
      orders      → direct {order_id}
      customers   → SMEMBERS idx:customer_orders:{customer_id}
      products    → idx:product_items:{product_id} → for each item_id:
                    GET idx:item_order:{item_id} → order_id
      order_items → direct {order_id}
      shipping    → direct {order_id}
    """
    if table == "orders":
        return _route_orders(payload)
    elif table == "customers":
        return _route_customers(r, payload)
    elif table == "products":
        return _route_products(r, payload)
    elif table == "order_items":
        return _route_order_items(payload)
    elif table == "shipping":
        return _route_shipping(payload)
    else:
        logger.warning("Unknown table in event router", extra_fields={"table": table})
        return set()


# --------------------------------------------------------------------------- #
# Per-table routing                                                            #
# --------------------------------------------------------------------------- #

def _route_orders(payload: dict[str, Any]) -> set[str]:
    order_id = payload.get("order_id") or payload.get("id")
    if order_id:
        return {str(order_id)}
    return set()


def _route_customers(r: redis.Redis, payload: dict[str, Any]) -> set[str]:
    customer_id = payload.get("customer_id") or payload.get("id")
    if not customer_id:
        return set()
    order_ids = r.smembers(f"idx:customer_orders:{customer_id}")
    return set(order_ids)


def _route_products(r: redis.Redis, payload: dict[str, Any]) -> set[str]:
    product_id = payload.get("product_id") or payload.get("id")
    if not product_id:
        return set()

    # Step 1: get all item_ids that reference this product
    item_ids: set[str] = r.smembers(f"idx:product_items:{product_id}")
    if not item_ids:
        return set()

    # Step 2: for each item_id, look up the owning order_id via pipeline
    pipe = r.pipeline(transaction=False)
    for item_id in item_ids:
        pipe.get(f"idx:item_order:{item_id}")
    results = pipe.execute()

    order_ids: set[str] = set()
    for order_id in results:
        if order_id:
            order_ids.add(str(order_id))
    return order_ids


def _route_order_items(payload: dict[str, Any]) -> set[str]:
    order_id = payload.get("order_id")
    if order_id:
        return {str(order_id)}
    return set()


def _route_shipping(payload: dict[str, Any]) -> set[str]:
    order_id = payload.get("order_id")
    if order_id:
        return {str(order_id)}
    return set()
