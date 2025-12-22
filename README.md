# mqueue — Distributed, Sharded, Priority Message Queue

mqueue is a high-performance, distributed, sharded priority message queue written in **Go**, designed for **at-least-once delivery**, **low-latency dequeue**, **durability**, and **multi-tenancy**.

Inspired by systems like **Facebook’s FOQS**, mqueue combines:
- **PostgreSQL** — durable, sharded persistence
- **Redis** — buffering and fast-path serving
- **Write-Ahead Log (WAL)** — crash recovery

It is ideal for background job processing, lead enrichment pipelines, task scheduling, and any workflow requiring reliable, ordered, delayed message delivery.

---

## Key Features

### Core Capabilities
- **Horizontal Sharding**  
  Automatic sharding across multiple PostgreSQL instances using an FNV-32 hash of `namespace + topic`.

- **Low-Latency Dequeue**  
  Hot topics are served directly from Redis ready queues. Cold topics fall back to PostgreSQL leasing.

- **Strict Priority & Delayed Delivery**  
  Messages are dequeued by priority (lower number = higher priority) and `deliver_after` timestamp.

- **At-Least-Once Delivery**  
  Lease-based visibility timeout with automatic renewal and redelivery on failure.

- **Retries & Dead Letter Queue (DLQ)**  
  Configurable retries with exponential backoff. Messages exceeding max retries are moved to DLQ.

- **Idempotency Support**  
  Per-namespace idempotency keys with deduplication across Redis and PostgreSQL.

- **Multi-Tenancy & Rate Limiting**  
  Isolated namespaces with configurable per-minute enqueue quotas.

### Reliability & Operations
- **Durability & Crash Recovery**  
  Per-shard WAL with automatic recovery on startup.

- **Observability**  
  Native Prometheus metrics: enqueue/dequeue/ack/nack rates, buffer depth, ready queue depth, shard health.

- **Security**  
  JWT authentication, TLS support, and IP-based rate limiting.

- **High Availability Ready**  
  Redis replication and Sentinel support.

- **Dynamic Topics**  
  Topics are created automatically on first use.

---

## When to Use mqueue

Use **mqueue** when you need:

### ✅ Strict Priority Processing
- Jobs **must** be processed in priority order
- Higher-priority tasks must always preempt lower-priority ones

### ✅ Delayed & Scheduled Jobs
- Native `deliver_after` support
- No need for cron hacks or polling schedulers

### ✅ At-Least-Once Semantics
- You can tolerate duplicate delivery
- Consumers are idempotent or use idempotency keys

### ✅ Database-Backed Durability
- Messages must survive crashes and restarts
- PostgreSQL-backed persistence is preferred over in-memory brokers

### ✅ Multi-Tenant Queues
- Isolated namespaces for different customers or teams
- Per-namespace rate limits and quotas

### ✅ Operational Simplicity
- You want **Postgres + Redis**, not a large distributed streaming platform
- Easier debugging with SQL visibility into queue state

### Common Use Cases
- Background job processing
- Lead ingestion & enrichment pipelines
- Payment or billing workflows
- Email, SMS, and notification dispatch
- Task scheduling with retries and DLQ
- SaaS internal queues per customer/tenant

---

## When NOT to Use mqueue

Avoid **mqueue** if your problem looks like any of the following:

### ❌ Event Streaming / Analytics Pipelines
- You need to replay events from weeks or months ago
- Multiple consumer groups need independent offsets  
➡️ Use **Kafka / Pulsar**

### ❌ Exactly-Once Delivery Required
- Your system **cannot tolerate duplicates**
- Exactly-once semantics are mandatory  
➡️ Use transactional systems or idempotent consumers

### ❌ Ultra-High Throughput (Millions/sec)
- You need extreme throughput over ordering guarantees
- Disk-backed SQL writes become a bottleneck  
➡️ Use Kafka or log-based systems

### ❌ Fan-Out Broadcast Messaging
- Every message must go to many consumers
- Pub/Sub-style distribution  
➡️ Use Kafka, NATS, or cloud Pub/Sub

### ❌ Ephemeral / Fire-and-Forget Tasks
- Messages don’t matter if lost
- No retries, no durability needed  
➡️ Use Redis lists, in-memory queues, or task runners

---

## Architecture Overview

mqueue blends durability, speed, and recoverability by combining PostgreSQL, Redis, and WAL-based recovery.

(<img width="1414" height="919" alt="image" src="https://github.com/user-attachments/assets/beb639a6-04b4-4221-8c02-81740a1be6d3" />)

**Background Daemons**:
- **Flusher** → Drains Redis buffer → PostgreSQL (with circuit breaker)
- **Prefetcher** → Leases items → Priority heap merge → Redis Ready Queue
- **Lease Daemon** → Renews active leases in Redis + PostgreSQL

## Installation & Quick Start (Docker Compose)

### Prerequisites
- Docker & Docker Compose
- Go 1.24 (optional for local development)
---

## Installation & Quick Start (Docker Compose)

### Prerequisites
- Docker & Docker Compose
- Go 1.24+ (optional, for development)

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/mqueue.git
cd mqueue
````
### 2. Generate TLS Certificates (Recommended for Production)
`````Bash
mkdir -p mqueue-certs
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout mqueue-certs/key.pem \
  -out mqueue-certs/cert.pem \
  -subj "/CN=localhost"
`````
#### 3. Create .env File (Optional but Recommended)
`````Bash
envDATABASE_URLS=
REDIS_ADDRS=
REDIS_PASSWORD=
WAL_DIR=
JWT_SECRET=
NAMESPACE_QUOTAS=
TLS_CERT_FILE=/app/mqueue-certs/cert.pem
TLS_KEY_FILE=/app/mqueue-certs/key.pem
`````
### 4. Start the System
```Bash
docker-compose up --build -d
Wait ~60 seconds for services to initialise.
````
### 5. Verify It's Running
````Bash
##Public health check
curl -k https://localhost:8080/health
# → OK
`````
### Metrics
```Bash
curl -k https://localhost:2112/metrics | grep mqueue_shard_health
# should show 1 for all shards
````

#### Authentication
All endpoints except /health require a valid JWT token in the header:
````text
Authorisation: Bearer <token>
Generate a JWT Token
Use https://jwt.io

Algorithm: HS256
Secret: fYXSb6rYIpxCKOvIv5hKWKph+2B//Wie9kV7sj6Ot44=
Payload (example):

JSON{
  "sub": "test-user",
  "exp": 1930000000
}
`````
Copy the generated token.
API Examples
Replace YOUR_JWT with your generated token.
### Enqueue Messages
````Bash
curl -k -X POST https://localhost:8080/enqueue \
  -H "Authorisation: Bearer YOUR_JWT" \
  -H "Content-Type: application/json" \
  -d '[{
    "namespace": "default",
    "topic": "leads",
    "priority": 1,
    "payload": "eyJuYW1lIjoiSm9obiBEb2UifQ==",  // base64: {"name":"John Doe"}
    "deliver_after": "2025-12-25T10:00:00Z"
  }]'
 # Returns an array of assigned message IDs.
````
### Dequeue Messages
````Bash
curl -k "https://localhost:8080/dequeue?namespace=default&topic=leads&limit=10" \
  -H "Authorisation: Bearer YOUR_JWT"
# Returns up to limit messages.
`````
### Ack (Successful Processing)
`````Bash
curl -k -X POST https://localhost:8080/ack \
  -H "Authorisation: Bearer YOUR_JWT" \
  -H "Content-Type: application/json" \
  -d '{
    "id": 123456789012345678,
    "namespace": "default",
    "topic": "leads"
  }'
```````
### Nack (Failure → Retry)
````Bash
curl -k -X POST https://localhost:8080/nack \
  -H "Authorisation: Bearer YOUR_JWT" \
  -H "Content-Type: application/json" \
  -d '{
    "id": 123456789012345678,
    "namespace": "default",
    "topic": "leads",
    "error": "Processing failed"
  }'
After MaxRetries (default 3), the message moves to DLQ.
`````
### View Dead Letter Queue
```Bash
curl -k "https://localhost:8080/dlq?namespace=default&topic=leads&limit=10" \
  -H "Authorisation: Bearer YOUR_JWT"
````
### Delete from DLQ
```Bash
curl -k -X POST https://localhost:8080/dlq/delete \
  -H "Authorisation: Bearer YOUR_JWT" \
  -H "Content-Type: application/json" \
  -d '{
    "id": 123456789012345678,
    "namespace": "default",
    "topic": "leads"
  }'
````
### Monitoring
```bash
Metrics End
point: https://localhost:2112/metrics (TLS)
Key metrics:
mqueue_buffer_queue_depth — items waiting to be flushed
mqueue_ready_queue_depth — items in fast path
mqueue_enqueue_total, mqueue_dequeue_total
mqueue_shard_health — 1 = healthy
```
### Development
```Bash
make build              # Build binary
make run                # Run locally
make test               # Unit tests
make integration-test   # Integration tests (requires Docker)
````
## License
MIT License


