#!/usr/bin/env python3
"""seed_test_data.py — Inject sample CDC events into all 5 Redis Streams."""
from __future__ import annotations

import json
import os
import sys

import redis

REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

CUSTOMER = {
    "op": "c",
    "after_customer_id": "cust-1",
    "after_name": "Alice Smith",
    "after_email": "alice@example.com",
    "after_phone": "+1-555-0100",
    "ts_ms": "0",
}

PRODUCT_1 = {
    "op": "c",
    "after_product_id": "prod-1",
    "after_name": "Wireless Keyboard",
    "after_sku": "WK-2024",
    "after_price": "79.99",
    "ts_ms": "0",
}

PRODUCT_2 = {
    "op": "c",
    "after_product_id": "prod-2",
    "after_name": "USB-C Hub",
    "after_sku": "UC-HUB-7",
    "after_price": "39.99",
    "ts_ms": "0",
}

ORDER = {
    "op": "c",
    "after_order_id": "ord-1001",
    "after_customer_id": "cust-1",
    "after_status": "confirmed",
    "after_created_at": "2024-01-15T10:00:00Z",
    "after_updated_at": "2024-01-15T10:00:00Z",
    "ts_ms": "0",
}

ITEM_1 = {
    "op": "c",
    "after_item_id": "item-1",
    "after_order_id": "ord-1001",
    "after_product_id": "prod-1",
    "after_quantity": "1",
    "after_unit_price": "79.99",
    "ts_ms": "0",
}

ITEM_2 = {
    "op": "c",
    "after_item_id": "item-2",
    "after_order_id": "ord-1001",
    "after_product_id": "prod-2",
    "after_quantity": "2",
    "after_unit_price": "39.99",
    "ts_ms": "0",
}

SHIPPING = {
    "op": "c",
    "after_shipping_id": "ship-1",
    "after_order_id": "ord-1001",
    "after_carrier": "FedEx",
    "after_tracking_number": "FX123456789",
    "after_status": "in_transit",
    "after_shipped_at": "2024-01-15T14:00:00Z",
    "after_delivered_at": "",
    "ts_ms": "0",
}

EVENTS = [
    ("stream:customers", CUSTOMER),
    ("stream:products", PRODUCT_1),
    ("stream:products", PRODUCT_2),
    ("stream:orders", ORDER),
    ("stream:order_items", ITEM_1),
    ("stream:order_items", ITEM_2),
    ("stream:shipping", SHIPPING),
]

print(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}")
try:
    r.ping()
except redis.exceptions.ConnectionError as exc:
    print(f"Cannot connect to Redis: {exc}", file=sys.stderr)
    sys.exit(1)

for stream, data in EVENTS:
    msg_id = r.xadd(stream, data)
    print(f"  [{stream}] → {msg_id}")

print("Done. Seed events injected successfully.")
