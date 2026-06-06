# MQueue Design Decisions & Future Roadmap

This document outlines the architectural trade-offs, rationale behind specific technology choices, and the future roadmap of the MQueue project.

---

## 1. Modulo Sharding vs. Consistent Hashing

### Current Implementation: Modulo Sharding
MQueue routes topic-specific payloads using FNV-1a modulo sharding:
```go
shardIndex := int(h.Sum32() % uint32(len(pgPools)))
```

* **Why we chose it**: 
  * **Strict Queue Co-location**: All messages for a unique `(namespace, topic)` are guaranteed to live on the exact same PostgreSQL shard instance. This is required to maintain strict priority ordering (`ORDER BY priority, deliver_after`) and to execute transactional locks (`FOR UPDATE SKIP LOCKED`) without expensive cross-node database coordination.
  * **Simplicity**: Modulo hashing has zero run-time overhead, requires no coordinator node, and does not require maintaining a dynamic hash ring.
* **Trade-off**: Changing the shard count (e.g. from 2 nodes to 3 nodes) shifts the routing target for most topics, requiring a managed drainage/migration window (as documented in `OPERATIONAL_PLAYBOOK.md`).

### Future Roadmap: Consistent Hash Ring
For massive, multi-tenant deployments that scale dynamically:
* We plan to implement a **Consistent Hash Ring with Virtual Nodes**.
* A migration/rebalancing worker daemon will be introduced to automatically move leased and ready tasks between PostgreSQL shard instances when new shards are added or removed, achieving zero-downtime, fully automated scale-outs.

---

## 2. Hybrid Storage (Redis + PostgreSQL) vs. Single Store

### Current Implementation: Hybrid Cache/DB
MQueue combines Redis (in-memory fast-path) and PostgreSQL (durable relational storage).

* **Why we chose it**:
  * **Throughput**: Writing enqueues to a Redis buffer list and reading dequeues from a Redis ready queue achieves sub-millisecond, memory-speed latency.
  * **Durability**: Using PostgreSQL as the backend shard database guarantees ACID transaction safety, complex priority queries, and persistence over system restarts.
* **Trade-off**: Managing connection pools, failovers, and synchronizations for two distinct storage systems increases operational complexity.

### Future Roadmap: Pluggable Storage Providers
* We plan to decouple the storage interface, allowing operators to swap PostgreSQL for other distributed databases (like CockroachDB, YugabyteDB, or ScyllaDB) depending on latency and replication requirements.

---

## 3. Snowflake IDs vs. Database UUIDs/Sequences

### Current Implementation: Application-side Snowflake IDs
MQueue generates unique 64-bit Snowflake IDs within the API layer.

* **Why we chose it**:
  * **No Central Coordination**: Database sequences require contacting a primary database node to fetch the next ID, creating a major write bottleneck. Snowflake IDs are generated locally on the API nodes using thread-safe bit shifts.
  * **Time-Orderable**: The leading 41 bits represent a millisecond timestamp, meaning IDs are naturally sorted by creation time. This allows fast, index-friendly range queries in PostgreSQL.
* **Trade-off**: Requires coordinating worker node IDs (0-1023) to avoid ID collisions if two API nodes share the same ID.

### Future Roadmap: Automated Worker ID Leasing
* We plan to implement automated worker node ID leasing using Redis lock sets on startup, eliminating the need to manually assign `MQUEUE_NODE_ID` to API container replicas.

---

## 4. Write-Ahead Logging (WAL) vs. Direct DB Sync

### Current Implementation: Sync disk WAL with Async DB Flush
Enqueues write to a local WAL log and return immediately, while database flusher daemons persist them asynchronously.

* **Why we chose it**:
  * **Low Latency**: Directly writing batches to PostgreSQL shards takes 10ms–100ms due to relational database engine overhead. Writing to disk-level WAL logs with `fsync` takes $< 1\text{ms}$.
  * **Reliability**: If PostgreSQL becomes temporarily overloaded, the local WAL prevents the queue from rejecting incoming traffic.
* **Trade-off**: Requires allocating persistent volumes (PVCs) with high disk write speeds (SSDs/NVMe) to each API node.

### Future Roadmap: WAL Compaction
* We plan to introduce a background WAL compaction daemon that automatically truncates log segments as soon as they are fully persisted to PostgreSQL, minimizing local disk usage.

---

## 5. Security & Authorization: HMAC-SHA256 JWTs vs. mTLS/OAuth2

### Current Implementation: Stateless HMAC-SHA256 JWT
API endpoints authenticate incoming requests using HMAC-SHA256 signatures validated against a cluster-wide `JWT_SECRET`.

* **Why we chose it**:
  * **Stateless Speed**: API nodes validate signatures locally in memory. There is no external network round-trip to an Identity Provider (IdP) or OAuth2 server during request processing, preserving MQueue's sub-millisecond latencies.
  * **Granular Namespaces**: Claims like `allowed_namespaces` and `scopes` (e.g. `payments:write`) are embedded inside the token payload, allowing instant namespace-level tenant isolation.
* **Trade-off**: Requires secure propagation and periodic rotation of the shared `JWT_SECRET` across all API replicas.

### Future Roadmap: SPIFFE/SPIRE & mTLS
* We plan to support **Mutual TLS (mTLS)** for microservice authentication, utilizing **SPIFFE/SPIRE** to assign dynamic, cryptographically verifiable identities to containers. This removes the need for shared secrets and handles rotation transparently.

---

## 6. Rate Limiting: Redis-Backed Sliding Window vs. Token Bucket

### Current Implementation: Sliding Window Counter
MQueue uses namespace-level sliding window rate limits stored as sorted sets (`ZSET`) or counters inside Redis.

* **Why we chose it**:
  * **Accuracy**: Prevents token-limit resets at window boundaries (unlike Fixed Window algorithms), blocking consumers from double-dipping quotas during boundary switches.
  * **Distributed Synchronization**: Multiple API container instances share the same Redis rate-limiting state, ensuring that rate enforcement is unified across the entire cluster.
* **Trade-off**: Sorting and clearing old keys from Redis ZSETs incurs a slight memory and CPU write cost.

### Future Roadmap: Adaptive Backpressure Rate Limiting
* We plan to implement **Adaptive Rate Limiting**. Instead of relying on hardcoded static quotas (e.g., 5000 requests/minute), the system will monitor database shard connection health, latencies, and CPU levels, automatically throttling producers during database congestion.

---

## 7. Delayed Scheduling: SQL Filtering vs. Hierarchical Timer Wheels

### Current Implementation: Index-Backed SQL Polling
Delayed messages are stored in the PostgreSQL database shard alongside active tasks, with a `deliver_after` timestamp.

* **Why we chose it**:
  * **Simplicity**: No separate delay queue infrastructure is required. The `deliver_after` column is indexed via B-Tree, and the Prefetcher queries it efficiently:
    `WHERE status = 'ready' AND deliver_after <= NOW()`
  * **Crash Durability**: If the server restarts, scheduled items are safe in the database and will resume delivery when their time arrives.
* **Trade-off**: The database index must be constantly updated on writes, creating slight index write overhead.

### Future Roadmap: Memory-Speed Hierarchical Timer Wheels
* For sub-second scheduled tasks, database polling can be too slow. We plan to integrate an in-memory **Hierarchical Timer Wheel** cache on the API nodes to buffer near-future delayed tasks, executing triggers at millisecond precision before routing them back into the Redis ready queues.

