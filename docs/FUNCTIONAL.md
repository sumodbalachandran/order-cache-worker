# Functional Specification — Order Cache Worker

## Table of Contents

1. [Purpose](#purpose)
2. [Business Context](#business-context)
3. [System Behavior](#system-behavior)
4. [Data Sources](#data-sources)
5. [Cache Output Schema](#cache-output-schema)
6. [Processing Rules](#processing-rules)
7. [Delete Handling](#delete-handling)
8. [Debounce Behavior](#debounce-behavior)
9. [Consistency Guarantees](#consistency-guarantees)
10. [Performance Characteristics](#performance-characteristics)
11. [Failure Modes](#failure-modes)
12. [Scaling](#scaling)
13. [Monitoring & Alerting](#monitoring--alerting)
14. [Operational Runbook](#operational-runbook)
15. [Deployment Guide](#deployment-guide)
16. [Security](#security)
17. [Capacity Planning](#capacity-planning)
18. [SLA](#sla)

---

## Purpose

The Order Cache Worker maintains a **denormalized, always-fresh Redis cache** of order views. It removes the need for API servers to perform expensive multi-table database queries at read time, enabling sub-millisecond order lookups regardless of database load.

---

## Business Context

- **Problem**: An e-commerce platform's order API was performing 5-table SQL joins on every request, causing high database load and >100ms API latency.
- **Solution**: CDC events propagate every database change in real time into Redis. The worker re-joins the affected order once per change window (debounced) and stores the denormalized result.
- **Benefit**: Order API reads drop to a single `HGETALL cache:order:{id}` call (~0.2ms).

---

## System Behavior

1. **Event ingestion**: Debezium publishes change events for 5 tables to Redis Streams.
2. **Raw storage**: Every incoming row is stored verbatim as `raw:{table}:{pk}` for auditability and re-joins.
3. **Index maintenance**: Reverse indexes allow the worker to find all orders affected by a change to any related table.
4. **Debouncing**: Burst changes to the same order are coalesced — only one join rebuild happens per debounce window (50ms by default).
5. **Join rebuild**: A 3-round-trip pipeline read assembles the order view and writes it atomically.
6. **Recovery**: Unacknowledged messages are reclaimed and reprocessed by the pending recovery thread.

---

## Data Sources

### Entity-Relationship Diagram

```
customers (1) ──── (N) orders (1) ──── (N) order_items (N) ──── (1) products
                        │
                        └─────────── (N) shipping
```

### Tables

| Table | Primary Key | Tracked Fields |
|---|---|---|
| `orders` | `order_id` | `customer_id`, `status`, `created_at`, `updated_at` |
| `customers` | `customer_id` | `name`, `email`, `phone` |
| `products` | `product_id` | `name`, `sku`, `price` |
| `order_items` | `item_id` | `order_id`, `product_id`, `quantity`, `unit_price` |
| `shipping` | `shipping_id` | `order_id`, `carrier`, `tracking_number`, `status`, `shipped_at`, `delivered_at` |

---

## Cache Output Schema

Each cached order view is stored as a Redis Hash at `cache:order:{order_id}`:

```json
{
  "order_id":        "ord-1001",
  "status":          "confirmed",
  "created_at":      "2024-01-15T10:00:00Z",
  "updated_at":      "2024-01-15T12:00:00Z",
  "customer_id":     "cust-42",
  "customer_name":   "Alice Smith",
  "customer_email":  "alice@example.com",
  "total_amount":    "159.97",
  "item_count":      "3",
  "items":           "[{\"item_id\":\"item-1\",\"product_id\":\"prod-1\",\"product_name\":\"Keyboard\",\"sku\":\"KB-001\",\"quantity\":1,\"unit_price\":79.99,\"line_total\":79.99}, ...]",
  "shipping":        "[{\"shipping_id\":\"ship-1\",\"carrier\":\"FedEx\",\"tracking_number\":\"FX123\",\"status\":\"shipped\",\"shipped_at\":\"2024-01-15T14:00:00Z\",\"delivered_at\":\"\"}]"
}
```

### Field Descriptions

| Field | Source | Description |
|---|---|---|
| `order_id` | orders | Primary key |
| `status` | orders | Current order status |
| `created_at` | orders | Order creation timestamp |
| `updated_at` | orders | Last update timestamp |
| `customer_id` | orders | Foreign key to customers |
| `customer_name` | customers | Denormalized customer name |
| `customer_email` | customers | Denormalized customer email |
| `total_amount` | computed | Sum of (quantity × unit_price) across all items |
| `item_count` | computed | Number of order items |
| `items` | order_items + products | JSON array of item objects |
| `shipping` | shipping | JSON array of shipment objects |

---

## Processing Rules

### Per-Table Rules

**orders**
- Create/Update → refresh the order view.
- Delete → delete the order view and all associated raw hashes.

**customers**
- Any change → refresh all orders for that customer (fan-out via `idx:customer_orders:{customer_id}`).

**products**
- Any change → refresh all orders that contain an item referencing that product (2-hop fan-out: `idx:product_items` → `idx:item_order`).

**order_items**
- Create/Update → update indexes, refresh the parent order.
- Delete → remove item from indexes, refresh the parent order.

**shipping**
- Create/Update → update indexes, refresh the parent order.
- Delete → remove shipment from indexes, refresh the parent order.

### Total Amount Calculation

```python
total_amount = sum(item["quantity"] * item["unit_price"] for item in items)
```

Rounded to 2 decimal places.

---

## Delete Handling

| Event | Action |
|---|---|
| `orders` delete | DEL `cache:order:{id}`, DEL `raw:orders:{id}` |
| `order_items` delete | Remove from `idx:order_items`, DEL `idx:item_order`, remove from `idx:product_items`, refresh order |
| `shipping` delete | Remove from `idx:order_shipping`, DEL `idx:shipping_order`, refresh order |
| `customers` delete | Raw hash removed; customer fields in next rebuild will be empty strings |
| `products` delete | Raw hash removed; product fields in next rebuild will be empty strings |

---

## Debounce Behavior

When a burst of changes arrives for the same order (e.g., 10 order_items inserted in rapid succession):

1. Each event calls `debouncer.schedule_refresh("ord-X")`.
2. The second through tenth calls find the order already in the pending set → `debounce_coalesced_total` is incremented.
3. After `DEBOUNCE_INTERVAL_MS` (50ms), the flush thread calls `refresh_order("ord-X")` exactly once.

This reduces join rebuilds by up to 90% during bulk insert workloads.

---

## Consistency Guarantees

- **At-least-once processing**: Messages are only ACKed after successful processing. Redis's `XAUTOCLAIM` ensures unacknowledged messages are reprocessed.
- **Eventual consistency**: The cache will be updated within `DEBOUNCE_INTERVAL_MS` + join latency of any change.
- **Atomic cache writes**: Each `cache:order:*` write uses a `MULTI`/`EXEC` pipeline (`DEL → HSET → EXPIRE`), preventing partial reads.
- **No cross-shard transactions**: All data for a single order is co-located in the same Redis instance.

---

## Performance Characteristics

| Metric | Value |
|---|---|
| Target throughput | 10,000+ events/second |
| Debounced rebuild rate | ~1,000 orders/second per worker |
| Join latency (p99) | < 5ms (3 pipeline round-trips) |
| Cache write latency | < 1ms (single pipeline) |
| Memory per order view | ~800 bytes average |
| Stream consumer lag (steady state) | < 100 messages |

---

## Failure Modes

| Failure | Behavior |
|---|---|
| Redis connection lost | Consumer sleeps 5s, retries. Recovery thread retries on next sweep. |
| Redis restart | Messages remain in stream PEL; XAUTOCLAIM reclaims after 60s idle. |
| Worker crash | Messages in PEL reclaimed by sibling worker or on restart. |
| Missing raw hash | Join returns empty fields for missing data; view written with partial data. |
| Corrupted event payload | Logged as error, event skipped, `cdc_events_failed_total` incremented. |
| OTel exporter down | Falls back to ConsoleSpanExporter; no impact on throughput. |

---

## Scaling

### Horizontal Scaling

Run multiple worker instances with unique `CONSUMER_NAME` values in the same `CONSUMER_GROUP`. Redis automatically distributes stream messages across consumers.

```bash
./scripts/scale_workers.sh 4  # Start 4 workers
```

### Vertical Scaling

Increase `BATCH_SIZE` (up to 1000) and `REDIS_MAX_CONNECTIONS` for higher single-worker throughput.

### Partitioning

For very high fan-out (e.g., customer with 10,000 orders), consider sharding the `idx:customer_orders:*` set across a Redis Cluster.

---

## Monitoring & Alerting

### Recommended Alerts

| Alert | Condition | Severity |
|---|---|---|
| High stream lag | `consumer_stream_lag > 10000` for 5m | Critical |
| High error rate | `rate(cdc_events_failed_total[5m]) > 10` | Warning |
| Worker not processing | `rate(cdc_events_processed_total[5m]) == 0` for 2m | Critical |
| Cache write failures | `rate(cache_writes_total[5m]) == 0` and lag > 0 | Critical |
| Health not ready | `/health/ready` returns non-200 | Critical |

---

## Operational Runbook

### Worker not processing events

1. Check `journalctl -u order-cache-worker@worker-1 -n 100`
2. Check health: `curl http://localhost:8001/health/ready`
3. Check Redis: `redis-cli ping`
4. Check stream lag: `redis-cli xlen stream:orders`

### Stale cache view

1. Identify affected order: `curl http://localhost:8002/debug/order/{id}` (DEBUG_MODE=true)
2. Force rebuild: `curl http://localhost:8002/debug/rebuild/{id}`
3. Check index consistency: `curl http://localhost:8002/debug/indexes/{id}`

### Recovering from a full Redis restart

The streams and raw hashes persist via AOF. Consumer groups and pending entries are restored automatically. Workers reconnect and process the backlog via `XAUTOCLAIM`.

---

## Deployment Guide

### Bare-Metal (Ubuntu/Debian)

```bash
# 1. Server setup (once)
sudo ./scripts/install_server.sh

# 2. App deployment
sudo ./scripts/install_app.sh

# 3. Scale to N workers
sudo ./scripts/scale_workers.sh 3
```

### Verifying Deployment

```bash
./scripts/health_check.sh         # Returns READY or NOT READY
curl http://localhost:8000/metrics # Prometheus metrics
systemctl status order-cache-worker@worker-1
```

---

## Security

- Redis `protected-mode yes` and bound to `127.0.0.1` by default.
- Set `REDIS_PASSWORD` in production.
- Health and metrics ports should be firewalled from public access.
- Debug port (`8002`) is only enabled when `DEBUG_MODE=true` — **never enable in production**.
- Application runs as `ocw` system user with no shell and restricted directories.

---

## Capacity Planning

### Memory

- Raw hashes: ~500 bytes/row × (orders + items + customers + products + shipping)
- Indexes: ~100 bytes/entry
- Cache views: ~800 bytes/order
- For 1M orders with 5 items each: ~10GB raw + ~1GB indexes + ~800MB cache = ~12GB

### CPU

- One worker core handles ~3,000–5,000 events/second.
- Scale horizontally for higher throughput.

### Redis Connections

- Each worker uses 1 connection pool with `REDIS_MAX_CONNECTIONS` (default 50).
- For 4 workers: 200 connections peak.

---

## SLA

| Metric | Target |
|---|---|
| Cache freshness (p99) | < 100ms after source change |
| Cache read latency | < 1ms (HGETALL) |
| Throughput | ≥ 10,000 events/second per worker |
| Availability | 99.9% (3 workers + Redis AOF) |
| Data loss | Zero (at-least-once + XAUTOCLAIM) |
