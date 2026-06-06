# MQueue Troubleshooting Guide

This guide helps cluster operators diagnose and resolve common failure modes, lag, and operational issues in the MQueue cluster.

---

## 1. Issue: Prefetch Lag (Ready Queues Are Empty)

### Symptoms:
* Clients calling `GET /dequeue` receive empty arrays (`[]`).
* Consumer worker CPU load is low, but database size is growing.
* Latency on database queries is high.

### Root Cause Analysis:
1. **Prefetcher Sleep Time**: The prefetcher loop is sleeping too long or is blocked.
2. **Missing Shard Index**: The PostgreSQL shard is executing a full-table scan on the `items` table instead of using indexes.
3. **Connection Exhaustion**: The prefetcher cannot acquire a database connection to query the shard.

### Solutions:
* **Verify Indexes**: Check if the shard index is applied:
  ```sql
  SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'items';
  ```
  If missing, recreate the index:
  ```sql
  CREATE INDEX IF NOT EXISTS idx_items_ready 
  ON items (namespace, topic, priority, deliver_after) 
  WHERE status = 'ready';
  ```
* **Monitor DB Connections**: Ensure PostgreSQL isn't rejecting connections. Check max connection limits in Postgres configuration.

---

## 2. Issue: Circuit Breaker Trips (Status "Open")

### Symptoms:
* Logs show: `gobreaker: flusher circuit breaker is open`.
* Messages remain stuck in the Redis Buffer and do not persist to PostgreSQL.
* Local WAL log files are growing rapidly.

### Root Cause Analysis:
The database shard is unresponsive or failing transactions. After 3 consecutive failures, the API's circuit breaker blocks all further bulk inserts to prevent thread congestion.

### Solutions:
1. **Check Database Health**: Ping the PostgreSQL instance and inspect error logs.
2. **Check Connection Pools**: Verify if connection pools are saturated. If Postgres is healthy but slow, increase connection pool sizes in config.
3. **Manual Reset**: Once the database is verified healthy, the circuit breaker will transition to **Half-Open** on the next flush attempt and automatically close when a batch successfully completes.

---

## 3. Issue: JWT Authentication Failures (HTTP 401/403)

### Symptoms:
* Microservices receive `401 Unauthorized` or `403 Forbidden` responses.
* Logs show: `Forbidden namespace access in enqueue` or `token is expired`.

### Root Cause Analysis:
1. The client JWT signature is invalid (wrong `JWT_SECRET`).
2. The token has expired (`exp` claim is in the past).
3. The token claims do not authorize the requested namespace.

### Solutions:
* **Validate Token Payload**: Decode the JWT at [jwt.io](https://jwt.io) or using CLI tools:
  * Verify the payload contains `allowed_namespaces` with the target namespace or `"*"` (wildcard).
  * Or verify the `scopes` array contains `namespace:read` or `namespace:*`.
* **Sync Clocks**: Ensure the clocks on the client servers and the MQueue API nodes are synced via NTP. If clocks drift, tokens may be rejected as expired.

---

## 4. Issue: Write-Ahead Log (WAL) Disk Bloat

### Symptoms:
* Disk utilization on API containers/nodes is approaching 100%.
* Large files named `wal-*.log` are accumulating in the `/wal` folder.

### Root Cause Analysis:
The background cleanup cron is disabled, or database shard outages are preventing the Recovery Daemon from replaying and deleting rotated log files.

### Solutions:
* **Verify Recovery Daemon**: Check the logs to ensure the `RecoveryDaemon` is active. If a database shard is down, recover the database first. The daemon will automatically sweep and process the accumulated WAL logs once connection is restored.
* **Configure WAL Retention**: Verify that the cleanup interval is set correctly in your config (the default retention keeps last 5 logs or up to 10GB).
