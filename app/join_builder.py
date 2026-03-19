from __future__ import annotations

import json
from typing import Any

import redis

from app.config import Config
from app.observability.logger import get_logger
from app.observability.tracing import trace_span, SpanAttributes

logger = get_logger(__name__)


def build_order_view(r: redis.Redis, order_id: str) -> dict[str, Any] | None:
    """Build a denormalized order view using 3 round-trip pipeline batches.

    Round-trip 1: Fetch order hash + customer_id + item_ids + shipping_ids
    Round-trip 2: Fetch customer hash + each item hash + each shipping hash
    Round-trip 3: Fetch product hash for each unique product_id in items

    Returns None if the order record itself does not exist.
    """
    with trace_span(
        "join_builder.build_order_view",
        {SpanAttributes.ORDER_ID: order_id},
    ):
        # ------------------------------------------------------------------ #
        # Round-trip 1: order, index lookups                                   #
        # ------------------------------------------------------------------ #
        pipe1 = r.pipeline(transaction=False)
        pipe1.hgetall(f"raw:orders:{order_id}")
        pipe1.get(f"idx:customer_id:{order_id}")  # customer_id stored during raw write
        pipe1.smembers(f"idx:order_items:{order_id}")
        pipe1.smembers(f"idx:order_shipping:{order_id}")
        order_hash, _, item_ids, shipping_ids = pipe1.execute()

        # The customer_id lives inside the order hash itself
        customer_id = (order_hash or {}).get("customer_id")

        if not order_hash:
            logger.debug(
                "Order not found for join",
                extra_fields={"order_id": order_id},
            )
            return None

        item_ids = item_ids or set()
        shipping_ids = shipping_ids or set()

        # ------------------------------------------------------------------ #
        # Round-trip 2: customer + items + shipments                           #
        # ------------------------------------------------------------------ #
        pipe2 = r.pipeline(transaction=False)
        if customer_id:
            pipe2.hgetall(f"raw:customers:{customer_id}")
        else:
            pipe2.hgetall("__nonexistent__")

        item_id_list = list(item_ids)
        for iid in item_id_list:
            pipe2.hgetall(f"raw:order_items:{iid}")

        shipping_id_list = list(shipping_ids)
        for sid in shipping_id_list:
            pipe2.hgetall(f"raw:shipping:{sid}")

        results2 = pipe2.execute()
        customer_hash: dict[str, str] = results2[0] or {}
        item_hashes: list[dict[str, str]] = results2[1 : 1 + len(item_id_list)]
        shipping_hashes: list[dict[str, str]] = results2[1 + len(item_id_list) :]

        # ------------------------------------------------------------------ #
        # Round-trip 3: products                                               #
        # ------------------------------------------------------------------ #
        product_ids: list[str] = []
        for ih in item_hashes:
            pid = (ih or {}).get("product_id")
            if pid and pid not in product_ids:
                product_ids.append(pid)

        pipe3 = r.pipeline(transaction=False)
        for pid in product_ids:
            pipe3.hgetall(f"raw:products:{pid}")
        product_results = pipe3.execute()
        products_by_id: dict[str, dict[str, str]] = {
            pid: (product_results[i] or {})
            for i, pid in enumerate(product_ids)
        }

        # ------------------------------------------------------------------ #
        # Assemble view                                                         #
        # ------------------------------------------------------------------ #
        items_view: list[dict[str, Any]] = []
        total_amount = 0.0
        for ih in item_hashes:
            if not ih:
                continue
            pid = ih.get("product_id", "")
            product_data = products_by_id.get(pid, {})
            qty = int(ih.get("quantity", 1))
            unit_price = float(ih.get("unit_price", 0.0))
            line_total = qty * unit_price
            total_amount += line_total
            items_view.append(
                {
                    "item_id": ih.get("item_id") or ih.get("id", ""),
                    "product_id": pid,
                    "product_name": product_data.get("name", ""),
                    "sku": product_data.get("sku", ""),
                    "quantity": qty,
                    "unit_price": unit_price,
                    "line_total": line_total,
                }
            )

        shipping_view: list[dict[str, Any]] = []
        for sh in shipping_hashes:
            if not sh:
                continue
            shipping_view.append(
                {
                    "shipping_id": sh.get("shipping_id") or sh.get("id", ""),
                    "carrier": sh.get("carrier", ""),
                    "tracking_number": sh.get("tracking_number", ""),
                    "status": sh.get("status", ""),
                    "shipped_at": sh.get("shipped_at", ""),
                    "delivered_at": sh.get("delivered_at", ""),
                }
            )

        view: dict[str, Any] = {
            # Order fields
            "order_id": order_id,
            "status": order_hash.get("status", ""),
            "created_at": order_hash.get("created_at", ""),
            "updated_at": order_hash.get("updated_at", ""),
            # Customer fields
            "customer_id": customer_id or "",
            "customer_name": customer_hash.get("name", ""),
            "customer_email": customer_hash.get("email", ""),
            # Computed
            "total_amount": round(total_amount, 2),
            "item_count": len(items_view),
            # JSON arrays
            "items": json.dumps(items_view),
            "shipping": json.dumps(shipping_view),
        }

        return view


def write_order_view(r: redis.Redis, order_id: str, view: dict[str, Any]) -> None:
    """Atomically write the denormalized view to Redis.

    Uses MULTI pipeline: DEL + HSET + EXPIRE in one round-trip.
    """
    cache_key = f"cache:order:{order_id}"
    with r.pipeline(transaction=True) as pipe:
        pipe.delete(cache_key)
        pipe.hset(cache_key, mapping=view)
        pipe.expire(cache_key, Config.CACHE_TTL_SECONDS)
        pipe.execute()

    logger.debug(
        "Order view written",
        extra_fields={"order_id": order_id, "key": cache_key},
    )


def delete_order_view(r: redis.Redis, order_id: str) -> None:
    """Remove the cached order view."""
    cache_key = f"cache:order:{order_id}"
    r.delete(cache_key)
    logger.debug("Order view deleted", extra_fields={"order_id": order_id})


def refresh_order(r: redis.Redis, order_id: str) -> None:
    """Build and write (or delete) the cached view for *order_id*."""
    with trace_span(
        "join_builder.refresh_order",
        {SpanAttributes.ORDER_ID: order_id},
    ):
        view = build_order_view(r, order_id)
        if view is None:
            delete_order_view(r, order_id)
        else:
            write_order_view(r, order_id, view)
