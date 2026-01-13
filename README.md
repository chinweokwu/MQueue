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
```mermaid
graph TD
    Client[Client App]
    LB[Load Balancer]
    API[MQueue API Nodes]
    Redis[Redis Cluster]
    PG[PostgreSQL Shards]

    subgraph "MQueue Node"
        Router[HTTP Router]
        IDGen[Snowflake ID Gen]
        Buffer[Redis Buffer]
        WAL[WAL Manager]
        Flusher[Flusher Daemon]
        Prefetch[Prefetch Daemon]
    end

    Client -->|HTTPS| LB
    LB --> API
    API --> Router

    %% Write Path (High Throughput)
    Router --> IDGen
    Router -->|1. Enqueue| Buffer
    Buffer -->|2. Buffer Write| Redis
    Router -->|3. Log| WAL
    flusherLoop[Async Loop] -.-> Flusher
    Flusher -->|4. Drain| Redis
    Flusher -->|5. Bulk Insert| PG

    %% Read Path (Low Latency)
    prefetchLoop[Async Loop] -.-> Prefetch
    Prefetch -->|6. Lease Ready Items| PG
    Prefetch -->|7. Push to Ready Queue| Redis
    Router -->|8. Dequeue (LPOP)| Redis
    
    %% Ack Path
    Router -->|9. Ack (DELETE)| PG
```

### How It Works
1.  **Enqueue (Write Fast)**: Requests are immediately written to a **Redis Buffer** and a local **WAL** (Write-Ahead Log) for durability. The client receives an ID immediately, without waiting for a database transaction.
2.  **Persist (Flush)**: Background flushers drain the Redis buffer and performs efficient **Bulk Inserts** into the appropriate **PostgreSQL Shard**.
3.  **Prepare (Prefetch)**: Background prefetchers scan the database for high-priority, ready-to-process items and push them into a **Redis Ready Queue**.
4.  **Dequeue (Read Fast)**: Consumers read directly from the **Redis Ready Queue** (FIFO), ensuring sub-millisecond latency.
5.  **Clean Up (Ack)**: When a consumer finishes a job, it sends an ACK, which deletes the record from PostgreSQL.

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
| `BUFFER_TTL` | Time to keep items in Redis Buffer (e.g. `1m`, `1h`) | `1m` |

---

## 🏃 Quick Start

### 1. Run with Docker Compose
```bash
docker-compose up --build -d
```

### 2. Run Integration Tests (Dockerized)
Run the full integration suite in a clean, isolated environment:
```bash
make docker-test
```

### 3. Verify Health
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

### 6. Go Client Example
You can easily integrate MQueue into your Go application:

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type Item struct {
	Namespace    string    `json:"namespace"`
	Topic        string    `json:"topic"`
	Priority     int       `json:"priority"`
	Payload      []byte    `json:"payload"`
	DeliverAfter time.Time `json:"deliver_after"`
}

func main() {
	// Enqueue
	item := Item{
		Namespace:    "default",
		Topic:        "orders",
		Priority:     1,
		Payload:      []byte("order-123"),
		DeliverAfter: time.Now(),
	}
	body, _ := json.Marshal([]Item{item})
	req, _ := http.NewRequest("POST", "http://localhost:8080/enqueue", bytes.NewBuffer(body))
	req.Header.Set("Authorization", "Bearer <JWT>")
	req.Header.Set("Content-Type", "application/json")
	
	resp, _ := http.DefaultClient.Do(req)
	defer resp.Body.Close()
	fmt.Println("Enqueued:", resp.Status)
}
```

---

## ⚠️ "Production Ready" Notes

*   **Sharding**: MQueue uses `hash(namespace + topic)` to pick a shard. This allows high throughput but means a single topic cannot exceed the write capacity of one Postgres instance.
*   **Failover**: If a shard goes down, writes to topics on that shard will **fail** (return 500) to preserve data consistency. We do not support "spillover" writes to avoid orphaned data.
*   **WAL**: Ensure `WAL_DIR` is mounted on a persistent volume in production.

## License

MIT License
