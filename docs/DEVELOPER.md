# Developer Guide — Order Cache Worker

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Prerequisites](#prerequisites)
3. [Project Structure](#project-structure)
4. [Local Development Setup](#local-development-setup)
5. [Configuration Reference](#configuration-reference)
6. [Code Walkthrough](#code-walkthrough)
7. [Observability](#observability)
8. [Testing](#testing)
9. [Extending the System](#extending-the-system)
10. [Troubleshooting](#troubleshooting)
11. [Coding Conventions](#coding-conventions)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│  SQL Server  (5 tables)                                             │
│  orders / customers / products / order_items / shipping             │
└──────────────────────────┬──────────────────────────────────────────┘
                           │  Debezium CDC connector
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Redis Streams                                                       │
│  stream:orders  stream:customers  stream:products                   │
│  stream:order_items  stream:shipping                                │
└──────────────────────────┬──────────────────────────────────────────┘
                           │  XREADGROUP (consumer group: join_workers)
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│  StreamConsumer (app/stream_consumer.py)                            │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ 1. Parse DebeziumEvent (models.py)                          │   │
│  │ 2. Write raw:* hash  (HSET)                                 │   │
│  │ 3. Update reverse indexes (indexes.py)                      │   │
│  │ 4. Route affected order IDs (event_router.py)               │   │
│  │ 5. Schedule refresh (debounce.py)                           │   │
│  └─────────────────────────────────────────────────────────────┘   │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│  OrderRefreshDebouncer (app/debounce.py)                            │
│  Coalesces rapid refresh requests, flushes every DEBOUNCE_INTERVAL  │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│  JoinBuilder (app/join_builder.py)                                  │
│  3-round-trip pipeline join → cache:order:{id} HSET + EXPIRE        │
└─────────────────────────────────────────────────────────────────────┘
```

### Background Threads

| Thread | Module | Purpose |
|---|---|---|
| `debounce-flush` | debounce.py | Flush pending refresh queue |
| `pending-recovery` | pending_recovery.py | XAUTOCLAIM idle messages |
| `lag-monitor` | metrics.py | Update stream lag gauges |
| `health-server` | health.py | HTTP /health endpoint |
| `debug-server` | debug_inspector.py | HTTP /debug endpoint |
| `uptime-tracker` | main.py | Update worker_uptime_seconds gauge |

---

## Prerequisites

- Python 3.11+
- Redis 7+ (XAUTOCLAIM requires Redis 7)
- (Optional) Grafana Tempo for distributed tracing
- (Optional) Prometheus + Grafana for metrics dashboards

---

## Project Structure

```
order-cache-worker/
├── app/
│   ├── __init__.py
│   ├── config.py              Environment-based configuration
│   ├── redis_client.py        Connection pool + group setup
│   ├── models.py              DebeziumEvent dataclass
│   ├── indexes.py             Reverse index maintenance
│   ├── event_router.py        Order ID routing per table
│   ├── join_builder.py        3-round-trip join
│   ├── debounce.py            OrderRefreshDebouncer
│   ├── stream_consumer.py     XREADGROUP main loop
│   ├── pending_recovery.py    XAUTOCLAIM recovery
│   ├── main.py                Entrypoint
│   └── observability/
│       ├── __init__.py
│       ├── logger.py          Structured logging
│       ├── metrics.py         Prometheus metrics
│       ├── tracing.py         OpenTelemetry
│       ├── health.py          /health server
│       └── debug_inspector.py /debug server
├── conf/                      Config files
├── scripts/                   Setup + utility scripts
├── systemd/                   Systemd units
├── tests/                     pytest suite
├── docs/                      Documentation
├── requirements.txt
└── requirements-dev.txt
```

---

## Local Development Setup

```bash
# Clone and install
git clone https://github.com/sumodbalachandran/order-cache-worker.git
cd order-cache-worker
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt

# Start Redis locally
redis-server conf/redis.conf &

# Run the worker (streams will be empty until you seed data)
python -m app.main

# In another terminal — seed test CDC events
python scripts/seed_test_data.py
```

### Environment Variables

Copy `conf/worker.env` and export or use a `.env` loader:

```bash
export $(grep -v '^#' conf/worker.env | xargs)
```

---

## Configuration Reference

All config lives in `app/config.py` — every setting is an environment variable with a sensible default.

| Variable | Default | Type | Description |
|---|---|---|---|
| `REDIS_HOST` | `127.0.0.1` | str | Redis host |
| `REDIS_PORT` | `6379` | int | Redis port |
| `REDIS_PASSWORD` | _(empty)_ | str | Redis password |
| `REDIS_DB` | `0` | int | Redis DB index |
| `REDIS_MAX_CONNECTIONS` | `50` | int | Connection pool size |
| `CONSUMER_GROUP` | `join_workers` | str | Stream consumer group name |
| `CONSUMER_NAME` | `worker-1` | str | This worker's name |
| `BATCH_SIZE` | `100` | int | Max messages per XREADGROUP call |
| `BLOCK_MS` | `2000` | int | XREADGROUP blocking timeout (ms) |
| `MAX_RETRIES` | `3` | int | Max retries for transient errors |
| `PENDING_CHECK_INTERVAL` | `30` | int | Seconds between recovery sweeps |
| `CACHE_TTL_SECONDS` | `86400` | int | cache:order:* TTL (24 h) |
| `DEBOUNCE_ENABLED` | `true` | bool | Enable debounce |
| `DEBOUNCE_INTERVAL_MS` | `50` | int | Flush interval for debouncer |
| `LOG_LEVEL` | `INFO` | str | Python logging level |
| `LOG_FORMAT` | `json` | str | `json` or `text` |
| `LOG_FILE` | _(empty)_ | str | Path for rotating file log |
| `OTEL_ENABLED` | `true` | bool | Enable OpenTelemetry tracing |
| `OTEL_SERVICE_NAME` | `order-cache-worker` | str | OTel service name |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://127.0.0.1:4317` | str | OTLP gRPC endpoint |
| `OTEL_SAMPLE_RATE` | `0.1` | float | Trace sampling rate (0–1) |
| `METRICS_ENABLED` | `true` | bool | Enable Prometheus metrics |
| `METRICS_PORT` | `8000` | int | Prometheus scrape port |
| `HEALTH_PORT` | `8001` | int | Health endpoint port |
| `DEBUG_MODE` | `false` | bool | Enable /debug endpoints |
| `DEBUG_PORT` | `8002` | int | Debug server port |

---

## Code Walkthrough

### Data Flow (step by step)

1. `main.py` bootstraps logging, tracing, metrics, health server, Redis, consumer groups, debug server, and lag monitor.
2. `StreamConsumer.run()` loops on `XREADGROUP`, reading up to `BATCH_SIZE` messages from all 5 streams.
3. For each message, `process_event()` is called:
   - Parses the message into a `DebeziumEvent` (full envelope or flattened SMT).
   - Writes a `raw:{table}:{pk}` hash.
   - Updates reverse indexes via `update_indexes()`.
   - Routes to affected order IDs via `get_affected_order_ids()`.
   - Schedules each order for refresh via `OrderRefreshDebouncer.schedule_refresh()`.
4. The debounce flush thread calls `refresh_order()` for each deduplicated order ID.
5. `refresh_order()` calls `build_order_view()`, which does 3 pipeline round-trips:
   - RT1: Fetch order hash + index lookups (item_ids, shipping_ids).
   - RT2: Fetch customer + all items + all shipments.
   - RT3: Fetch product hashes.
6. The assembled view is written atomically via `write_order_view()` (`DEL + HSET + EXPIRE`).

### Redis Key Schema

| Key Pattern | Type | Content |
|---|---|---|
| `raw:orders:{order_id}` | Hash | Order row fields |
| `raw:customers:{customer_id}` | Hash | Customer row fields |
| `raw:products:{product_id}` | Hash | Product row fields |
| `raw:order_items:{item_id}` | Hash | Order item row fields |
| `raw:shipping:{shipping_id}` | Hash | Shipping row fields |
| `cache:order:{order_id}` | Hash | Denormalized order view |
| `idx:customer_orders:{customer_id}` | Set | order_ids for customer |
| `idx:order_items:{order_id}` | Set | item_ids for order |
| `idx:item_order:{item_id}` | String | order_id for item |
| `idx:product_items:{product_id}` | Set | item_ids using product |
| `idx:order_shipping:{order_id}` | Set | shipping_ids for order |
| `idx:shipping_order:{shipping_id}` | String | order_id for shipment |

### Join Pipeline (3 round-trips)

```python
# RT1 — order + index lookups
pipe1 = r.pipeline(transaction=False)
pipe1.hgetall(f"raw:orders:{order_id}")
pipe1.smembers(f"idx:order_items:{order_id}")
pipe1.smembers(f"idx:order_shipping:{order_id}")
order_hash, item_ids, shipping_ids = pipe1.execute()

# RT2 — customer + items + shipments
pipe2 = r.pipeline(transaction=False)
pipe2.hgetall(f"raw:customers:{customer_id}")
for iid in item_ids: pipe2.hgetall(f"raw:order_items:{iid}")
for sid in shipping_ids: pipe2.hgetall(f"raw:shipping:{sid}")
results2 = pipe2.execute()

# RT3 — products
pipe3 = r.pipeline(transaction=False)
for pid in product_ids: pipe3.hgetall(f"raw:products:{pid}")
results3 = pipe3.execute()
```

### Debounce

The `OrderRefreshDebouncer` uses a background thread that sleeps for `DEBOUNCE_INTERVAL_MS` ms, then flushes the pending `set[str]` of order IDs in one batch. If `schedule_refresh()` is called for an order that is already pending, a `debounce_coalesced_total` counter is incremented and the duplicate is ignored.

### Consumer Loop

`StreamConsumer.run()` continuously calls `XREADGROUP` with `">"` (new messages only). Acknowledged messages are batched and sent via pipeline `XACK`. On `ConnectionError`, the worker sleeps 5 seconds before retrying.

### Pending Recovery

`PendingRecovery` runs `XAUTOCLAIM` every `PENDING_CHECK_INTERVAL` seconds, claiming messages idle for more than 60 s, and re-processing them.

---

## Observability

### Logging

- Format: JSON (default) or human-readable text (`LOG_FORMAT=text`).
- Each log line includes `trace_id`, `span_id`, and active context vars (`stream`, `table`, `message_id`, `order_id`, `operation`).
- Use `logger.info("message", extra_fields={"key": "value"})` for structured fields.

### Metrics (Prometheus)

Scrape endpoint: `http://localhost:8000/metrics`

**Key PromQL examples:**

```promql
# Event throughput (events/s)
rate(cdc_events_processed_total[1m])

# Error rate
rate(cdc_events_failed_total[1m])

# p99 processing latency
histogram_quantile(0.99, rate(cdc_event_processing_duration_seconds_bucket[5m]))

# Debounce efficiency
rate(debounce_coalesced_total[1m]) / rate(orders_refreshed_total[1m])

# Stream lag
consumer_stream_lag{stream="stream:orders"}
```

### Tracing (OpenTelemetry)

Span tree for a single event:

```
stream_consumer.process_event
  └─ join_builder.refresh_order
       └─ join_builder.build_order_view
```

Configure `OTEL_EXPORTER_OTLP_ENDPOINT` to point at Grafana Tempo.

### Health Endpoints

| Path | Method | Description |
|---|---|---|
| `/health` | GET | Liveness — always 200 |
| `/health/ready` | GET | Readiness — 200 if Redis + group ready + errors < 10 |
| `/health/details` | GET | Full state JSON |

### Debug Inspector (`DEBUG_MODE=true`)

| Path | Description |
|---|---|
| `/debug/order/{id}` | Raw hash, cached view, TTL, staleness |
| `/debug/stream/{name}` | Stream info, groups, last 5 messages |
| `/debug/indexes/{id}` | All indexes + bidirectional check |
| `/debug/raw/{table}/{pk}` | Raw hash contents |
| `/debug/rebuild/{id}` | Force rebuild with timing |
| `/debug/pipeline/stats` | Redis info + stream stats |

---

## Testing

```bash
# Unit tests (no Redis required)
pytest tests/ -v --ignore=tests/test_integration.py

# Coverage
pytest tests/ --cov=app --cov-report=term-missing --ignore=tests/test_integration.py

# Integration tests (requires Redis on localhost:6379)
pytest tests/test_integration.py -v -m integration
```

---

## Extending the System

### Adding a New Table

1. Add the stream to `Config.STREAMS`.
2. Add index maintenance in `indexes.py` (`update_indexes()`).
3. Add routing in `event_router.py` (`get_affected_order_ids()`).
4. Update `join_builder.py` if the new table's data should appear in the cache view.
5. Add tests in `tests/test_indexes.py` and `tests/test_event_router.py`.

### Changing the Cache Schema

Edit `join_builder.build_order_view()`. Add/remove fields from the `view` dict. Update `tests/test_join_builder.py` and `tests/test_integration.py`.

---

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| `BUSYGROUP Consumer Group name already exists` | Normal on restart | Ignored automatically |
| High `consumer_stream_lag` | Worker too slow or dead | Scale workers or increase `BATCH_SIZE` |
| Cache views not updating | Debouncer not flushing | Check `DEBOUNCE_ENABLED`, logs from debounce thread |
| `XAUTOCLAIM` errors | Redis < 7 | Upgrade to Redis 7+ |
| Missing product names in cache | Product hash missing | Check `raw:products:{pid}` exists |

---

## Coding Conventions

- Python 3.11+; use `dict | None`, `set[str]` type hints.
- All imports are absolute (`from app.config import Config`).
- Use `get_logger(__name__)` and `logger.info("msg", extra_fields={...})`.
- No bare `except:` — always catch specific exceptions.
- Use pipelines (`r.pipeline(transaction=False)`) for multi-key reads.
- Keep functions short and focused; large modules split into helpers.
