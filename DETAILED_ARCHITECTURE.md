# MQueue Detailed System Architecture & Design Specification

This document provides a comprehensive analysis of the internal mechanics, design patterns, and components that make up **MQueue** (Distributed, Sharded, Priority Message Queue).

---

## 1. Architectural Blueprint

MQueue utilizes a hybrid architecture combining the high throughput of in-memory caching with the durability of relational shards and local Write-Ahead Logs (WAL).

```
                  ┌──────────────────────────────────┐
                  │        Client Application        │
                  └────────────────┬─────────────────┘
                                   │
                  1. Enqueue (Red) │  4. Dequeue (Green)
                                   ▼
                  ┌──────────────────────────────────┐
                  │         MQueue API Nodes         │
                  └────┬────────────────────────┬────┘
                       │                        │
        ┌──────────────┘                        └──────────────┐
        │                                                      │
        ▼                                                      ▼
┌───────────────┐                                      ┌───────────────┐
│ Redis Buffer  │                                      │  Redis Queue  │
│  (Fast-Path)  │                                      │ (Prefetched)  │
└───────┬───────┘                                      └───────▲───────┘
        │                                                      │
        │ 2. Flusher Daemon                                    │ 3. Prefetcher Daemon
        ▼                                                      │
┌──────────────────────────────────────────────────────────────┴───────┐
│                          PostgreSQL Shards                           │
│                          (Durable Storage)                           │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 2. Core Components

### 2.1. API Cluster Nodes
The API layer is built in **Go** and designed to scale horizontally behind a load balancer. 
* **State Management**: The API nodes are stateless regarding message storage but manage local Write-Ahead Log (WAL) files.
* **Snowflake ID Generation**: Each node embeds a custom 64-bit Snowflake ID generator, creating time-ordered, globally unique IDs locally to avoid network round-trips:
  `[Timestamp (41 bits) | Node ID (10 bits) | Sequence (12 bits)]`

### 2.2. Write Buffer (Redis)
* **Purpose**: Serves as a high-speed write cache to absorb sudden bursts of enqueue operations.
* **Storage Key**: `mqueue:buffer:{namespace}:{topic}` (implemented as a Redis List).

### 2.3. Write-Ahead Log (WAL)
* **Purpose**: Ensures immediate write durability.
* **Mechanism**: Every incoming batch is serialized and appended to an active log file on local SSD storage using `fsync` before acknowledging the request. If the in-memory Redis buffer crashes, the WAL maintains the data.

### 2.4. PostgreSQL Shards
* **Purpose**: Serves as the database of record (durable ground truth).
* **Sharding Scheme**: Database operations are routed across multiple PostgreSQL instances. Shards are selected using an FNV-1a hash of the combined `(namespace + topic)` string.
* **Schema Indexes**:
  * `idx_items_ready`: For picking up unprocessed tasks (`status = 'ready'`) ordered by priority and delivery time.
  * `idx_items_namespace_topic`: For namespace isolation and metadata polling.

### 2.5. Ready Queue (Redis Prefetch Cache)
* **Purpose**: Eliminates the latency of sorting database queries during reads.
* **Storage Key**: `mqueue:ready:{namespace}:{topic}`.

---

## 3. The Write Path (Enqueue) - Deep Dive Design

```
[ Client Request ] 
       │
       ▼
[ POST /enqueue ] ──► [ JWT Claims Verification (allowed_namespaces/scopes) ]
       │
       ▼
[ Snowflake ID Generation ] (Local thread-safe node ID sequencing)
       │
       ├────────────────────────────────────────┐ (Concurrent Async Write)
       ▼                                        ▼
[ Redis Buffer Cache ]                   [ Local WAL Manager ]
* LPUSH mqueue:buffer                    * Append serialized JSON log line
* RAM-speed (sub-ms)                     * Execute physical fsync() to SSD
       │                                        │
       └───────────────────┬────────────────────┘
                           ▼
                    [ HTTP 200 OK ] (Returns generated Snowflake IDs)
```

### 3.1. Ingress and Authorization
1. The client sends a payload to `POST /enqueue` containing a JSON array of messages.
2. The router validates the HMAC-SHA256 signed **JWT** in the HTTP request header.
3. The auth middleware extracts claims:
   * Checks **`allowed_namespaces`** matching the target namespace, OR
   * Evaluates the **`scopes`** array containing `namespace:write` or `namespace:*`.

### 3.2. Snowflake ID Generation
To scale writes globally without database round-trips or locks, MQueue generates thread-safe **Snowflake IDs** application-side:
* **Timestamp**: 41 bits (millisecond precision, providing 69 years of unique IDs relative to custom epoch).
* **Worker ID**: 10 bits (supports up to 1024 concurrent running MQueue API container nodes).
* **Sequence**: 12 bits (supports up to 4096 messages per millisecond per node).

### 3.3. Dual-Write Durability Protocol (WAL & Redis)
Once IDs are generated, the API node triggers a concurrent write flow:
1. **In-Memory Cache (Fast-Path)**: Pushes serialized items to Redis via `LPUSH mqueue:buffer:{namespace}:{topic}`.
2. **Local WAL (Append-Only Log)**: Appends the serialized payload with its Snowflake ID to `/app/wal/shard{id}/wal.log`. A blocking `f.Sync()` is performed, forcing the kernel filesystem buffer to flush dirty pages to the underlying SSD block storage.
3. **Response Guarantee**: The API server returns success **only** after both the Redis Buffer memory write and the local WAL disk sync succeed. If either fails, the transaction is rejected, guaranteeing at-least-once reliability.

---

## 4. The Persistence Path (Flusher Daemon)

The **Flusher** daemon runs asynchronously in the background on each API node to persist buffered writes to database shards:

```
[ Ticker Trigger (FlushInterval) ]
               │
               ▼
[ Drain Redis Buffer ] ──► (RPOP/LPOP batch of items from mqueue:buffer)
               │
               ▼
[ Shard Resolution ] ──► (Hash namespace + topic using FNV-1a modulo shard pool size)
               │
               ▼
[ Bulk DB Transaction ] ──► (INSERT INTO items (...) ON CONFLICT (id) DO NOTHING)
               │
        ┌──────┴──────┐
        │ Success?    ├─► [ No ] ─► [ Circuit Breaker Trips ] (Retry buffer sweep later)
        └──────┬──────┘
               │ [ Yes ]
               ▼
[ Purge Redis Buffer & Rotated WAL ]
```

### 4.1. Shard Routing Mechanics
To locate the target database instance, the Flusher resolves the database shard dynamically:
1. Formulates the sharding key: `namespace + ":" + topic`.
2. Computes the hash using the **FNV-1a 32-bit algorithm**:
   ```go
   h := fnv.New32a()
   h.Write([]byte(namespace + ":" + topic))
   shardIndex := int(h.Sum32() % uint32(len(pgPools)))
   ```
3. FNV-1a ensures uniform key distribution across database instances while guaranteeing all messages for the same namespace/topic logical queue reside on the same database node.

### 4.2. Database Bulk Insertion
Instead of writing records row-by-row (which incurs high network and transaction overhead), the Flusher aggregates messages into batches and issues a single transaction:
```sql
INSERT INTO items (id, namespace, topic, status, priority, payload, deliver_after, created_at, updated_at) 
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) 
ON CONFLICT (id) DO NOTHING;
```
* The `ON CONFLICT (id) DO NOTHING` clause handles retries and WAL replays idempotently, preventing duplicate record creation.

### 4.3. Circuit Breakers
The Flusher integrates a circuit breaker (`gobreaker` library) per database pool:
* **Consecutive Failures**: If the database shard fails more than 3 consecutive times, the breaker trips to **Open**.
* **Action**: Flusher sweeps are suspended for that shard, protecting Postgres from load cascades.
* **Cooldown**: After a 10-second timeout, the breaker goes to **Half-Open**, permitting a single batch probe to test shard recovery.

---

## 5. The Read Path (Dequeue & Prefetch) - Deep Dive Design

To deliver sub-millisecond read times without overloading database instances with `ORDER BY` and lock operations on every request, MQueue separates read queues into Postgres and Redis layers:

```
        [ PostgreSQL Shard ] (Contains persistent, sorted tasks)
                 │
                 │ 1. SELECT ... FOR UPDATE SKIP LOCKED
                 ▼
     [ Background Prefetcher ] (Runs loop to lock ready tasks)
                 │
                 │ 2. RPUSH mqueue:queue:{namespace}:{topic}
                 ▼
       [ Redis Ready Queue ] (In-memory sorted lease cache)
                 ▲
                 │ 3. LPOP (Memory-speed read)
                 ▼
           [ GET /dequeue ] ──► [ Worker Client ]
                 │
                 └───────────────► Process Task ───► [ POST /ack ] ──► (Delete from Postgres)
```

### 5.1. The Prefetcher Daemon
The background **Prefetcher** daemon continually query-sweeps PostgreSQL shards to load ready messages into memory:
1. **Query Scan**: It executes a query to lock unprocessed, ready tasks:
   ```sql
   SELECT id, namespace, topic, payload, priority, deliver_after 
   FROM items 
   WHERE namespace = $1 AND topic = $2 AND status = 'ready' AND deliver_after <= NOW() 
   ORDER BY priority ASC, deliver_after ASC 
   LIMIT $3 
   FOR UPDATE SKIP LOCKED;
   ```
   * **`FOR UPDATE SKIP LOCKED`**: This SQL construct is critical; it locks the matching rows and immediately returns them, skipping any rows already leased by concurrent prefetchers. This avoids lock waits and resource contention.
2. **Lease Setting**: The prefetcher sets the lease state on the locked database rows:
   ```sql
   UPDATE items SET status = 'leased', lease_expires_at = $1, lease_owner = $2 WHERE id = ANY($3);
   ```
3. **Queue Cache Feeding**: Pushes the message payloads into the Redis Ready Queue:
   `RPUSH mqueue:queue:{namespace}:{topic}`.

### 5.2. Worker Dequeue
When a worker consumer requests a message (`GET /dequeue?namespace=X&topic=Y`):
1. The API node pops the next message from memory: `LPOP mqueue:queue:{namespace}:{topic}`.
2. If Redis pops a message, it is returned to the client immediately ($<1\text{ms}$).
3. **Slow-Path Fallback**: If the Redis queue is empty (e.g. during prefetch lags or heavy load), the API node falls back to query the database directly:
   * It performs a lock and lease scan directly on PostgreSQL, updates the lease, and returns the result, maintaining at-least-once availability.

### 5.3. Completion and Visibility Timeout
1. **Success (`POST /ack`)**: If the worker processes the item successfully, it calls `/ack`. The API node issues a `DELETE FROM items WHERE id = $1` on the PostgreSQL shard, removing the message permanently.
2. **Failure (`POST /nack`)**: If the worker fails, it calls `/nack`. The API node increments `retries`, updates status to `ready`, applies exponential backoff with jitter to `deliver_after`, and clears the lease.
3. **Visibility Timeout Redelivery**: If a worker node crashes and fails to call `/ack` before `lease_expires_at` is reached:
   * The Prefetcher sweeps expired leases during its scan (`lease_expires_at < NOW()`).
   * It resets their status to `ready`, clearing the lease owner and scheduling them for redelivery.

---

## 6. Recovery Protocol (The Recovery Daemon)

If Redis encounters a cluster partition or database node failure, MQueue activates a recovery system to protect data integrity:

1. **Degraded Path**: Write operations skip Redis and write items strictly to local disk WAL files.
2. **Outage Recovery**: When the Redis master recovers, the **Recovery Daemon** initializes.
3. **WAL Rotation**: It isolates active writes by forcing a WAL file rotation:
   * Renames the active `wal.log` to `wal-[timestamp].log`.
   * Creates a fresh, empty `wal.log` for incoming writes.
4. **Log Replay**: It reads the rotated WAL log line-by-line, deserializing the message payloads.
5. **Database Sync**: It replays the items into the target PostgreSQL shards via idempotent upserts.
6. **Cleanup**: On successful replay, it deletes the rotated log files from disk.
