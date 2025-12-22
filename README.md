# mqueue - A Distributed, Sharded, Priority Message Queue
mqueue is a high-performance, distributed, sharded priority message queue built in Go, designed for at least once delivery with low-latency dequeue, durability, and multi-tenancy support. Inspired by systems like Facebook's FOQS, it combines PostgreSQL for persistence, Redis for buffering and fast-path serving, and WAL for crash recovery.
Perfect for background job processing, lead enrichment pipelines, task scheduling, and any workflow requiring reliable, ordered, delayed message delivery.

Inspired by systems like Facebook's FOQS, mqueue combines:
- **PostgreSQL** for sharded persistence
- **Redis** for buffering and fast-path serving
- **WAL** for crash recovery

## Features

- **Horizontal Sharding**  
  Automatic sharding across multiple PostgreSQL instances using FNV-32 hash on `namespace + topic`.

- **Low-Latency Dequeue**  
  Hot topics served from in-memory Redis ready queue (prefetched with correct priority order). Cold topics fallback to PostgreSQL lease.

- **Strict Priority & Delayed Delivery**  
  Messages are dequeued by priority (lower numbers indicate higher priorities) and `deliver_after` timestamp.

- **At-Least-Once Delivery**  
  Lease-based visibility timeout with automatic renewal and redelivery on failure.

- **Retries with Exponential Backoff + DLQ**  
  Configurable max retries. Failed messages are automatically moved to the Dead Letter Queue.

- **Idempotency Support**  
  Per-namespace idempotency keys with deduplication in Redis buffer and PostgreSQL.

- **Multi-Tenancy & Rate Limiting**  
  Isolated namespaces with configurable per-minute enqueue quotas.

- **Durability & Crash Recovery**  
  Per-shard Write-Ahead Log (WAL) with automatic recovery on startup.

- **Observability**  
  Full Prometheus metrics: enqueue/dequeue/ack/nack rates, buffer & ready queue depths, shard health.

- **Security**  
  JWT authentication, TLS support, and IP-based rate limiting.

- **High Availability Ready**  
  Redis replication and Sentinel support included.

- **Dynamic Topics**  
  No pre-creation is needed — topics are automatically discovered.

## Architecture Overview
It combines the best of PostgreSQL (durability & sharding), Redis (speed & buffering), and Write-Ahead Logging (crash recovery) to deliver a robust, scalable system.

<img width="1416" height="4028" alt="image" src="https://github.com/user-attachments/assets/dcd406b4-0726-4bf1-becb-bb2e9b3cb6ad" />

Background Daemons:
  • Flusher       → Drains Redis buffer → PostgreSQL (with circuit breaker)
  • Prefetcher    → Leases items → Priority heap merge → Redis Ready Queue
  • Lease Daemon  → Renews active leases in Redis + PostgreSQL
Background Daemons:
  • Flusher       → Drains Redis buffer → PostgreSQL (with circuit breaker)
  • Prefetcher    → Leases items → Priority heap merge → Redis Ready Queue
  • Lease Daemon  → Renews active leases in Redis + PostgreSQL

