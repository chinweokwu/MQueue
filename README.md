# MQueue (Distributed, Sharded, Priority Message Queue)

**mqueue** is a high-performance, distributed, sharded priority message queue written in **Go**. It is designed for **massive write throughput**, **strict priority ordering**, and **at-least-once delivery**.

mqueue is modeled after **Facebook's FOQS** (Facebook Ordered Queueing Service). It combines the durability of **PostgreSQL** with the speed of **Redis** and the safety of a **Write-Ahead Log (WAL)**.

---

## 🚀 Key Features

### ⚡ Async Enqueue & High Throughput
- **Non-Blocking Writes**: Enqueue requests are written to Redis (buffer) and WAL (durability) and acknowledged *immediately*.
- **Snowflake IDs**: Generates unique, time-ordered 64-bit IDs application-side, avoiding database round-trips for ID generation.
- **Bulk Flushing**: Background flushers batch-insert items into PostgreSQL, maximizing database throughput.

### 🛡️ Reliability & Durability
- **WAL-Based Recovery**: Every message is persisted to a local Write-Ahead Log before ack. If the server crashes, items are recovered on startup.
- **Graceful Shutdown**: Ensures all in-flight memory buffers are drained to the database before the application exits.
- **Circuit Breakers**: Automatically stops writing to a database shard if it becomes unhealthy, preventing cascading failures.
- **Strict Sharding**: Routes writes based on `(namespace, topic)` hash. Fails fast if the target shard is unavailable to prevent data inconsistency.

### 🎯 Priority & Scheduling
- **Priority Queues**: Items are ordered by `priority` (asc) and then `deliver_after` timestamp.
- **Delayed Delivery**: Native support for scheduling tasks in the future.
- **Prefetching**: Background prefetchers move high-priority items from disk (Postgres) to memory (Redis) to ensure low-latency dequeue.

### 📦 Multi-Tenancy
- **Namespaces**: Logical isolation for different teams or customers.
- **Quotas**: Configurable rate limits (enqueues/minute) per namespace.
- **Dynamic Topics**: Topics are created on-the-fly; no manual provisioning required.

### 🔍 Observability
- **Structured Logging**: Configurable JSON/Console logging via Zap.
- **Prometheus Metrics**: Built-in `/metrics` endpoint with counters for enqueue, dequeue, ack, nack, and queue depth.
- **Tracing Context**: Ready for OpenTelemetry integration.

---

## Architecture

![Architecture](https://github.com/user-attachments/assets/beb639a6-04b4-4221-8c02-81740a1be6d3)

1.  **Enqueue**: 
    - Client -> HTTP API -> Snowflake Generator -> Redis Buffer + WAL.
2.  **Persist**: 
    - Flusher Daemon -> Drains Redis Buffer -> Bulk Insert to PostgreSQL Shard.
3.  **Prepare**: 
    - Prefetcher Daemon -> Scans PostgreSQL for highest priority ready items -> Pushes to Redis Ready Queue.
4.  **Dequeue**: 
    - Client -> HTTP API -> Pop from Redis Ready Queue.
5.  **Clean Up**: 
    - Client -> HTTP API -> Ack (Delete from Postgres) / Nack (Retry later).

---

## 🛠️ Configuration

Configuration is loaded from `.env` or environment variables.

| Variable | Description | Default |
| :--- | :--- | :--- |
| `DATABASE_URLS` | Comma-separated Postgres connection strings (Shards) | **Required** |
| `REDIS_ADDRS` | Comma-separated Redis addresses | **Required** |
| `REDIS_PASSWORD`| Redis password | `""` |
| `WAL_DIR` | Directory for Write-Ahead Log files | **Required** |
| `MQUEUE_NODE_ID`| Unique integer ID (0-1023) for Snowflake generator | `1` |
| `LOG_LEVEL` | `debug`, `info`, `warn`, `error` | `info` |
| `LOG_ENCODING` | `json` (prod), `console` (dev) | `json` |
| `JWT_SECRET` | Secret for verifying auth tokens | **Required** |
| `NAMESPACE_QUOTAS`| Rate limits (e.g. `tenant-a:1000,tenant-b:500`) | `""` |

---

## 🏃 Quick Start

### 1. Run with Docker Compose
```bash
docker-compose up --build -d
```

### 2. Verify Health
```bash
curl -k https://localhost:8080/health
# Output: OK
```

### 3. Enqueue a Message
```bash
curl -k -X POST https://localhost:8080/enqueue \
  -H "Authorization: Bearer <YOUR_JWT>" \
  -H "Content-Type: application/json" \
  -d '[{
    "namespace": "default",
    "topic": "test-topic",
    "priority": 1,
    "payload": "SGVsbG8gV29ybGQ=", 
    "deliver_after": "2024-01-01T12:00:00Z"
  }]'
```

### 4. Dequeue a Message
```bash
curl -k "https://localhost:8080/dequeue?namespace=default&topic=test-topic&limit=1" \
  -H "Authorization: Bearer <YOUR_JWT>"
```

### 5. Acknowledge (ACK)
```bash
curl -k -X POST https://localhost:8080/ack \
  -H "Authorization: Bearer <YOUR_JWT>" \
  -d '{
    "id": 174568912344,
    "namespace": "default",
    "topic": "test-topic"
  }'
```

---

## ⚠️ "Production Ready" Notes

*   **Sharding**: MQueue uses `hash(namespace + topic)` to pick a shard. This allows high throughput but means a single topic cannot exceed the write capacity of one Postgres instance.
*   **Failover**: If a shard goes down, writes to topics on that shard will **fail** (return 500) to preserve data consistency. We do not support "spillover" writes to avoid orphaned data.
*   **WAL**: Ensure `WAL_DIR` is mounted on a persistent volume in production.

## License

MIT License
