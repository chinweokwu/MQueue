# MQueue (Distributed, Sharded, Priority Message Queue)

**mqueue** is a high-performance, distributed, sharded priority message queue written in **Go**. Modeled after **Facebook's FOQS** (Facebook Ordered Queueing Service), it is built to support massive write throughput, strict priority scheduling, and at-least-once delivery.

It combines the speed of **Redis** for fast-path enqueueing and prefetch dequeueing with the durability of **PostgreSQL** for persistence, wrapped in a local **Write-Ahead Log (WAL)** for crash recovery.

---

## 📖 Table of Contents
1. [Core Mechanics (How It Works)](#-core-mechanics-how-it-works)
2. [Dashboard & Telemetry](#-dashboard--telemetry)
3. [Performance & Benchmark Capacity](#-performance--benchmark-capacity)
4. [Quick Start (How to Run)](#-quick-start-how-to-run)
5. [Connecting to Microservices (API & Integration)](#-connecting-to-microservices-api--integration)
6. [Configuration Schema](#-configuration-schema)

---

## 🎯 Core Mechanics (How It Works)

MQueue splits its operations into decoupled write, persistence, and read pipelines:

![MQueue Architecture Diagram](./assets/architecture_diagram.png)

1. **Enqueue (Write Fast)**:
   Producers call `/enqueue`. The server generates globally unique, time-ordered 64-bit **Snowflake IDs** application-side (no database round-trip). It then writes the messages to an in-memory **Redis Buffer** list and appends them to a local **Write-Ahead Log (WAL)** file. Once safely written to WAL, the request is immediately acknowledged (latency is sub-millisecond).
2. **Flush (Durable Persistence)**:
   A background **Flusher** daemon periodically drains the Redis Buffer and executes high-speed **Bulk Inserts** into PostgreSQL shards, sharding data based on a hash of the `(namespace + topic)`.
3. **Prefetch (Read Smart)**:
   To avoid executing expensive sorting queries on PostgreSQL database shards for every read request, a background **Prefetcher** daemon polls PostgreSQL for upcoming, ready tasks. It locks/leases these items and pre-loads them into the **Redis Ready Queue**.
4. **Dequeue (Memory Speed)**:
   Consumers call `/dequeue`. They pull directly from the in-memory **Redis Ready Queue** using `LPOP`, achieving sub-millisecond dequeue speeds. If the Redis queue is empty, the server automatically falls back to leasing directly from PostgreSQL (slow-path).
5. **Acknowledge (Lifecycle End)**:
   Once the task is finished, the worker sends a `POST /ack` request, deleting the item from the PostgreSQL database shard. If the job fails, `POST /nack` puts the item back in the Postgres retry loop with exponential backoff.

---

## 📊 Dashboard & Telemetry

MQueue includes built-in interactive HTML5 dashboards for real-time visualization of queue metrics, distributed shard state, and background daemon cycles. These dashboards allow operators to inspect system health, verify data routing, and even interactively simulate cluster failovers.

### 💳 Ledger Engine Cluster Dashboard
**Access Endpoint**: `/dashboard` (or the default root `/`)

The Ledger Engine Dashboard provides a high-level view of the entire cluster's health and throughput. It maps client requests flowing through the system's load balancers down to individual Redis buffers and PostgreSQL sharded databases.

* **Key Features**:
  * **Dynamic Flow Pipeline**: Animates task ingestion from the client, fast-path writes to Redis memory buffers, and database flusher sweeps to Postgres.
  * **Live Shard Clusters**: Displays connection pool saturation, WAL replays, and successful/rejected transaction rates in real time.
  * **Interactive Outage Simulation**: Operators can click on any individual **Redis Shard** to trigger an artificial outage. The pipeline path automatically adapts, showing data logging on local worker WAL disks instead of Redis. When the shard is clicked again, it triggers the background `RecoveryDaemon` to replay WAL logs and restore shard health.
  * **Log Terminal**: Stream real-time cluster operations and heartbeat events.

![MQueue Shard Cluster Dashboard](./assets/dashboard_status.png)

*The status dashboard showing normal distributed write operations under stress testing.*

---

### 🎯 FOQS Core Scheduling Engine Dashboard
**Access Endpoint**: `/dashboard/foqs`

The FOQS Core Dashboard focuses on the task scheduler's core queuing algorithms, demonstrating exactly how priority scheduling, delayed delivery, and background prefetcher loops function.

* **Key Features**:
  * **Visual Prefetcher Loop**: View how the prefetcher pulls upcoming tasks from sharded PostgreSQL databases and loads them into memory-speed Redis ready queues.
  * **Interactive Scheduling Actions**: Users can trigger money transfers ($50) or suspicious AML transfers ($15K) to visually trace priority execution, or schedule items with delivery delays to watch them wait until their delivery timestamp is reached.
  * **Dead Letter Queue (DLQ) Integration**: Visually monitor tasks that have failed processing multiple times. Users can inspect the exact payload in the DLQ box and manually trigger a re-ingestion retry.

![FOQS Core Scheduling Engine Dashboard](./assets/dashboard_foqs.png)

*The FOQS scheduling visualizer showing priority-based queues and DLQ replay controls.*

---

## ⚡ Performance & Benchmark Capacity

MQueue was benchmarked on a standard local containerized development environment (concurrency level of 50, 1 database shard, 1 Redis instance) using the built-in benchmark utility.

### Measured Throughput Metrics:
* **Enqueue (Redis buffer + WAL disk write)**: **~150+ operations/sec** (includes writing and fsyncing to the Write-Ahead Log).
* **Database Flush (Persistence to PostgreSQL)**: **~2,900+ operations/sec** (leveraging batch bulk inserts).
* **Dequeue (Prefetched Redis Read)**: **~87,000+ operations/sec** (pure memory speed).

> [!NOTE]
> In production environments with multiple PostgreSQL shards and SSD-backed WAL storage, enqueue throughput scales horizontally and matches Redis limits.

### Running the Benchmarks
To run the benchmark utility on your local setup:
```bash
make benchmark
```
*(This starts a test PostgreSQL and Redis container via Docker Compose, creates temporary folders, and runs the Go benchmark suite).*

---

## 🏃 Quick Start (How to Run)

### Requirements
* **Go** 1.22 or higher (if running outside Docker)
* **Docker & Docker Compose** (recommended)

### Option A: Run via Docker Compose (Recommended)

> [!NOTE]
> Local development and testing run entirely inside standard **Docker Containers** using Docker Compose. **Kubernetes (K8s) is NOT required to run MQueue locally.** 
> The "Pods" displayed on the dashboard are simply the 3 local Docker container replicas managed by Docker Compose.

1. **Start the services**:
   ```bash
   docker-compose up --build -d
   ```
   This spins up:
   * 3 API load-balanced replicas of the Go MQueue service (mapped as Pods `mqueue-0`, `mqueue-1`, and `mqueue-2` on the dashboard)
   * PostgreSQL instances configured as database shards
   * Redis instances configured for rate-limiting, buffers, and prefetch queues

2. **Verify Server Health**:
   ```bash
   curl -k https://localhost:8080/health
   # Expected Output: OK
   ```

3. **Run the integration test suite**:
   ```bash
   make docker-test
   ```

### Option B: Run Locally (Bare Metal)
1. **Provision Dependencies**:
   Ensure PostgreSQL (shards) and Redis are running.
2. **Apply Database Schema**:
   Import `create_tables.sql` into your Postgres databases.
3. **Configure Environment Variables**:
   Create a `.env` file based on the config section below.
4. **Setup WAL folders**:
   ```bash
   make setup-wal
   ```
5. **Run the application**:
   ```bash
   go run main.go
   ```

---

## 🔌 Connecting to Microservices (API & Integration)

Microservices communicate with MQueue over HTTP/HTTPS. All endpoints (except `/health` and visual dashboards) require a valid **JSON Web Token (JWT)** in the authorization header.

### 🔑 Authentication & Scopes
MQueue uses HMAC-SHA256 tokens signed with `JWT_SECRET`. To connect, your microservice must provide the header:
```http
Authorization: Bearer <JWT_TOKEN>
```

Your JWT token claims must grant permission to your target namespace. The token must contain either:
1. A **`scopes`** array mapping `namespace:action` (actions: `read`, `write`, or `*`):
   ```json
   {
     "sub": "microservice-payments",
     "scopes": ["payments:write", "payments:read"]
   }
   ```
2. Or an **`allowed_namespaces`** claim:
   ```json
   {
     "allowed_namespaces": ["payments", "auth"]
   }
   ```

---

### 📥 Enqueueing Messages (Write Path)
**Endpoint**: `POST /enqueue`

Send a JSON array containing the tasks to enqueue. 

```bash
curl -k -X POST https://localhost:8080/enqueue \
  -H "Authorization: Bearer <YOUR_JWT>" \
  -H "Content-Type: application/json" \
  -d '[{
    "namespace": "payments",
    "topic": "process-refund",
    "priority": 1,
    "payload": "eyJrZXkiOiAidmFsdWUiLCJhbW91bnQiOiAxMC41MH0=", 
    "deliver_after": "2026-06-06T12:00:00Z"
  }]'
```
* **`payload`**: Must be a **Base64 encoded** string.
* **`priority`**: Integer priority weight. **Lower numbers are processed first** (0 is highest priority).
* **`deliver_after`**: UTC timestamp. The task remains hidden in the queue until this time is reached (for delayed scheduling).

**Response** (HTTP 200):
```json
[269543218222202880]
```
Returns a list of generated Snowflake IDs corresponding to your batch.

---

### 📤 Dequeueing Messages (Read Path)
**Endpoint**: `GET /dequeue`

Microservice workers poll this endpoint to receive pending messages.

```bash
curl -k "https://localhost:8080/dequeue?namespace=payments&topic=process-refund&limit=5" \
  -H "Authorization: Bearer <YOUR_JWT>"
```
* **`namespace`**: Isolating tenant workspace (required).
* **`topic`**: Task queue identifier (required).
* **`limit`**: Maximum number of items to return in the batch (defaults to 10).

**Response** (HTTP 200):
```json
[
  {
    "id": 269543218222202880,
    "namespace": "payments",
    "topic": "process-refund",
    "payload": "eyJrZXkiOiAidmFsdWUiLCJhbW91bnQiOiAxMC41MH0=",
    "priority": 1,
    "deliver_after": "2026-06-06T12:00:00Z",
    "lease_expires_at": "2026-06-06T12:30:00Z"
  }
]
```

---

### ✅ Acknowledging Messages (Completion)
**Endpoint**: `POST /ack`

Once a microservice worker successfully processes a message, it must acknowledge it to remove it from the system. If it is not acknowledged before `lease_expires_at`, it will be redelivered.

```bash
curl -k -X POST https://localhost:8080/ack \
  -H "Authorization: Bearer <YOUR_JWT>" \
  -H "Content-Type: application/json" \
  -d '{
    "id": 269543218222202880,
    "namespace": "payments",
    "topic": "process-refund"
  }'
```

**Response**: HTTP 200 `OK`.

---

### ❌ Negative Acknowledging (Failure / Retry)
**Endpoint**: `POST /nack`

If a microservice fails to process a message, it should notify the queue. MQueue will re-schedule the task with exponential backoff.

```bash
curl -k -X POST https://localhost:8080/nack \
  -H "Authorization: Bearer <YOUR_JWT>" \
  -H "Content-Type: application/json" \
  -d '{
    "id": 269543218222202880,
    "namespace": "payments",
    "topic": "process-refund",
    "error": "Payment service connection timeout"
  }'
```

**Response**: HTTP 200 `OK`.

---

## 🛠️ Configuration Schema

Configuration is loaded from environment variables or a `.env` file:

| Variable | Description | Example / Default |
| :--- | :--- | :--- |
| `DATABASE_URLS` | Comma-separated Postgres shard connections | `postgres://user:pass@host:5432/mqueue?sslmode=disable` |
| `REDIS_ADDRS` | Comma-separated Redis addresses | `localhost:6379,localhost:6380` |
| `REDIS_PASSWORD`| Redis password | `""` |
| `WAL_DIR` | Directory path for Write-Ahead Log files | `./wal` |
| `MQUEUE_NODE_ID`| Snowflake generator Worker Node ID (0-1023) | `1` |
| `LOG_LEVEL` | Logging level (`debug`, `info`, `warn`, `error`) | `info` |
| `LOG_ENCODING` | Output format (`json` or `console`) | `json` |
| `JWT_SECRET` | Secret key used to sign and verify auth tokens | **Required** |
| `NAMESPACE_QUOTAS`| Rate limits per namespace (enqueues/minute) | `default:10000,payments:5000` |
| `BUFFER_TTL` | Time to live for data in Redis Buffer | `1m` |
