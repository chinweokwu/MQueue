# MQueue Operations & Administration Playbook

This playbook provides step-by-step instructions for cluster operators to scale, maintain, and troubleshoot the MQueue distributed cluster in staging and production environments.

---

## 1. Shard Cluster Scaling (Scale-Out Runbook)

Because MQueue shards data based on an FNV-1a hash modulo the number of database connections (`hash(namespace + topic) % NumShards`), adding a new shard changes the modulo denominator and shifts the routing destination for existing queues.

### 1.1. Resharding Strategy (Zero-Downtime Migration)
When provisioning a new PostgreSQL database shard (e.g., adding Shard 2 to a Shard 0 & 1 cluster):

1. **Provision the New Shard Instance**:
   Deploy the new PostgreSQL shard container or cloud instance and apply the schema:
   ```bash
   psql -h pg-shard2 -U morah_paul -d mqueue -f create_table.sql
   ```

2. **Phase 1: Read-Only Mode for Affected Topics (Recommended)**:
   * Do not change the API configurations immediately.
   * If possible, briefly pause enqueuing on the active topics, allowing the Flusher daemon to drain the existing Redis buffers into the old shards.

3. **Phase 2: Scale Modulo Config**:
   Update the `DATABASE_URLS` environment variable across the API servers to include the new shard string.
   * **Old Config**: `DATABASE_URLS="postgres://...shard0,postgres://...shard1"`
   * **New Config**: `DATABASE_URLS="postgres://...shard0,postgres://...shard1,postgres://...shard2"`

4. **Phase 3: Rollout & Auto-Backfill**:
   Perform a rolling restart of the MQueue API containers.
   * New enqueued items will immediately shard across the three databases.
   * If a dequeue request maps a topic to the new shard but the message is still residing on an old shard (due to pre-existing leases), the Prefetcher will automatically catch up as it drains the old shard databases.

---

## 2. Dead Letter Queue (DLQ) Management

A task is moved to the DLQ table (`dead_letter`) on its PostgreSQL shard when it fails execution `MaxRetries` times.

### 2.1. Inspecting DLQ Items
To inspect dead-lettered items on a specific database shard, run:
```sql
SELECT id, namespace, topic, error_reason, failed_at, payload 
FROM dead_letter 
ORDER BY failed_at DESC 
LIMIT 50;
```

### 2.2. Replaying DLQ Items
Operators can replay dead items back into the active queue once the downstream consumer issue (e.g., database timeout, API outage) has been resolved.

#### Method A: Via the FOQS Core Dashboard
1. Open the dashboard at `https://localhost:8080/dashboard/foqs`.
2. Scroll to the **Dead Letter Queue (DLQ)** component on the right.
3. Click the **Replay** button next to the relevant transaction.

#### Method B: Via CLI/API (Bulk Replay)
Run a script to update the items back into the active `items` table on the PostgreSQL shard:
```sql
-- Move a specific item back to ready
INSERT INTO items (id, namespace, topic, status, priority, payload, deliver_after, created_at, updated_at)
SELECT id, namespace, topic, 'ready', 1, payload, NOW(), NOW(), NOW()
FROM dead_letter
WHERE id = 321611565323255808;

-- Delete from DLQ table
DELETE FROM dead_letter WHERE id = 321611565323255808;
```

---

## 3. Secret & Credential Rotation

### 3.1. Rotating JWT Signing Key (`JWT_SECRET`)
To rotate the HMAC signing key without causing client microservices to reject valid tokens immediately:

1. **Transition Phase**: Update the API nodes' configuration to accept tokens signed with *both* the old and new secrets (if customized in your middleware) or perform a rolling deployment.
2. **Issue New Tokens**: Have your authorization service begin signing new JWTs with the **New Key**.
3. **Rollout Key**: Update the `JWT_SECRET` environment variable or the Kubernetes Secret file:
   ```bash
   kubectl create secret generic mqueue-secrets \
     --from-literal=jwt-secret="NEW_SUPER_SECRET_KEY" \
     --dry-run=client -o yaml | kubectl apply -f -
   ```
4. **Restart Replicas**: Perform a rolling restart (`kubectl rollout restart statefulset mqueue`) of the API nodes.

### 3.2. Rotating Redis Password
1. Update the Redis password on the primary and replica instances.
2. Update the `REDIS_PASSWORD` environment variable or mount configuration in the API nodes.
3. Perform a rolling restart of the API cluster nodes. The Sentinel cluster will re-authenticate clients automatically.

---

## 4. Manual WAL Log Recovery

If an API node crashes and fails to run its automatic WAL replay on startup, or if a log file becomes corrupt:

### 4.1. Force Replay Log File
Locate the rotated WAL logs inside the node volume (`/wal/shard[id]/wal-[timestamp].log`). 

If you need to manually force a replay of rotated files, trigger a REST call or run the recovery utility:
```bash
# Force rotate the current WAL to isolate active writes
# (Creates a rotated log for replay)
curl -k -X POST https://localhost:8080/api/wal/rotate \
  -H "Authorization: Bearer <ADMIN_JWT>"
```

### 4.2. Handling Corrupted WAL Files
If the server reports parsing errors (e.g., `invalid JSON character` or EOF in log), the WAL file may contain partially written lines due to a sudden hardware power cut.
1. **Back up the corrupted file**:
   ```bash
   cp /wal/shard0/wal.log /wal/shard0/wal.log.bak
   ```
2. **Sanitize the log**:
   Use a text editor or utility to remove any incomplete line at the end of the file.
3. **Restart the API service**:
   The `RecoveryDaemon` will parse the file, restore the sanitized items to the database, and safely purge the recovery log.
