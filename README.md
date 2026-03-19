# Order Cache Worker

A production-ready **CDC Stream Join Pipeline** that consumes real-time change events from 5 SQL Server tables via Debezium into Redis Streams, performs application-side joins, and writes denormalized cached order views back to Redis.

Supports **10,000+ changes/second** with full observability (OpenTelemetry tracing, Prometheus metrics, structured JSON logging, health/debug HTTP endpoints).

---

## Quick Start

```bash
# 1. Install dependencies
python3.11 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# 2. Start Redis
redis-server conf/redis.conf

# 3. Seed test data
python scripts/seed_test_data.py

# 4. Run the worker
python -m app.main
```

---

## Architecture

```
SQL Server tables (5)
      │
      ▼  (Debezium CDC)
Redis Streams ──► StreamConsumer (XREADGROUP)
                       │
                       ├─► Write raw:* hashes
                       ├─► Update reverse indexes
                       └─► Debouncer ──► JoinBuilder ──► cache:order:* hash
```

### Data Flow

1. **Debezium** captures row-level changes on `orders`, `customers`, `products`, `order_items`, and `shipping` tables and publishes them to five Redis Streams.
2. **StreamConsumer** reads batches via `XREADGROUP`, writes raw hashes, updates reverse indexes, and routes affected order IDs.
3. **OrderRefreshDebouncer** coalesces rapid updates for the same order, then calls **JoinBuilder**.
4. **JoinBuilder** performs a 3-round-trip pipeline join and writes a denormalized `cache:order:{id}` hash with TTL.
5. **PendingRecovery** uses `XAUTOCLAIM` to reprocess stale unacknowledged messages.

---

## Project Structure

```
app/                    Python package
  config.py             Central configuration (env vars)
  redis_client.py       Connection pool + consumer group setup
  models.py             DebeziumEvent dataclass
  indexes.py            Reverse index maintenance
  event_router.py       Order ID routing per table
  join_builder.py       3-round-trip join + cache write
  debounce.py           OrderRefreshDebouncer
  stream_consumer.py    XREADGROUP loop
  pending_recovery.py   XAUTOCLAIM recovery thread
  main.py               Entry point
  observability/
    logger.py           JSON/text structured logging
    metrics.py          Prometheus metrics + lag monitor
    tracing.py          OpenTelemetry tracing
    health.py           /health HTTP server
    debug_inspector.py  /debug HTTP server (DEBUG_MODE=true)
conf/                   Configuration files
scripts/                Shell scripts + seed data
systemd/                Systemd unit files
tests/                  pytest test suite
docs/                   Developer and functional docs
```

---

## Configuration

All configuration is via environment variables (see `conf/worker.env`):

| Variable | Default | Description |
|---|---|---|
| `REDIS_HOST` | `127.0.0.1` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
| `CONSUMER_GROUP` | `join_workers` | Stream consumer group |
| `CONSUMER_NAME` | `worker-1` | Instance name |
| `BATCH_SIZE` | `100` | Messages per XREADGROUP |
| `DEBOUNCE_INTERVAL_MS` | `50` | Debounce flush interval |
| `CACHE_TTL_SECONDS` | `86400` | Cached view TTL |
| `OTEL_ENABLED` | `true` | Enable OpenTelemetry |
| `METRICS_PORT` | `8000` | Prometheus scrape port |
| `HEALTH_PORT` | `8001` | Health endpoint port |
| `DEBUG_MODE` | `false` | Enable debug inspector |

---

## Scaling

```bash
# Run 4 workers on the same server
./scripts/scale_workers.sh 4
```

Each worker gets offset ports (metrics: 8000, 8010, 8020, 8030…).

---

## Observability

| Endpoint | Description |
|---|---|
| `http://localhost:8000/metrics` | Prometheus metrics |
| `http://localhost:8001/health` | Liveness |
| `http://localhost:8001/health/ready` | Readiness |
| `http://localhost:8001/health/details` | Full health state |
| `http://localhost:8002/debug/order/{id}` | Debug a specific order (DEBUG_MODE=true) |

---

## Testing

```bash
pip install -r requirements-dev.txt
pytest tests/ -v --ignore=tests/test_integration.py   # unit tests
pytest tests/ -v -m integration                        # requires Redis
```

---

## Docs

- [Developer Guide](docs/DEVELOPER.md)
- [Functional Specification](docs/FUNCTIONAL.md)

---

## License

MIT
