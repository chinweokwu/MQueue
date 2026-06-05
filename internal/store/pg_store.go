package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"mqueue/internal/config"
	"mqueue/internal/log"
	"mqueue/internal/wal"
	"sort"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type PGStore struct {
	dbs           []*sql.DB
	redis         []redis.UniversalClient
	leaseCache    map[int64]string
	cacheMu       sync.RWMutex
	config        *config.Config
	logger        *log.Logger
	healthyShards map[int]bool
	healthyMu     sync.RWMutex
}

func NewPGStore(dbURLs []string, redisClients []redis.UniversalClient, cfg *config.Config) (*PGStore, error) {
	var dbs []*sql.DB
	for _, url := range dbURLs {
		db, err := sql.Open("postgres", url)
		if err != nil {
			return nil, fmt.Errorf("open postgres %s: %w", url, err)
		}
		db.SetMaxOpenConns(20)
		db.SetMaxIdleConns(10)
		dbs = append(dbs, db)
	}
	s := &PGStore{
		dbs:           dbs,
		redis:         redisClients,
		leaseCache:    make(map[int64]string),
		config:        cfg,
		logger:        log.NewLogger(),
		healthyShards: make(map[int]bool),
	}
	for i := range dbURLs {
		s.healthyShards[i] = true
	}
	go s.monitorShards()
	return s, nil
}

func (s *PGStore) Config() *config.Config {
	return s.config
}

func (s *PGStore) GetDBs() []*sql.DB {
	return s.dbs
}

// SetShardHealth sets the health state of a specific shard. Useful for tests.
func (s *PGStore) SetShardHealth(shardID int, healthy bool) {
	s.healthyMu.Lock()
	defer s.healthyMu.Unlock()
	s.healthyShards[shardID] = healthy
}

// IsShardHealthy returns true if the specified database shard is healthy.
func (s *PGStore) IsShardHealthy(shardID int) bool {
	s.healthyMu.RLock()
	defer s.healthyMu.RUnlock()
	return s.healthyShards[shardID]
}


func (s *PGStore) GetRedisClients() []redis.UniversalClient {
	return s.redis
}

func (s *PGStore) GetShardID(namespace, topic string) int {
	h := fnv.New32a()
	h.Write([]byte(namespace + topic))
	return int(h.Sum32() % uint32(len(s.dbs)))
}

func (s *PGStore) getDBForShard(namespace, topic string) (*sql.DB, error) {
	shardID := s.GetShardID(namespace, topic)
	s.healthyMu.RLock()
	isHealthy := s.healthyShards[shardID]
	s.healthyMu.RUnlock()

	if !isHealthy {
		return nil, fmt.Errorf("shard %d is unhealthy", shardID)
	}
	return s.dbs[shardID], nil
}

// GetHealthyDBsForShard returns a slice of healthy *sql.DB connections starting from the primary shard.
func (s *PGStore) GetHealthyDBsForShard(namespace, topic string) []*sql.DB {
	primaryShardID := s.GetShardID(namespace, topic)
	numShards := len(s.dbs)

	s.healthyMu.RLock()
	defer s.healthyMu.RUnlock()

	var healthyDBs []*sql.DB
	for i := 0; i < numShards; i++ {
		shardID := (primaryShardID + i) % numShards
		if s.healthyShards[shardID] {
			healthyDBs = append(healthyDBs, s.dbs[shardID])
		}
	}
	return healthyDBs
}

func (s *PGStore) GetRedisForShard(namespace, topic string) redis.UniversalClient {
	shardID := s.GetShardID(namespace, topic)
	// Redis doesn't have the same strict data ownership issues as DB,
	// but for consistency, we could stick to the primary.
	// However, keeping fallback here might be acceptable if Redis is just a cache.
	// But given the "Async Enqueue" ensures strictly one path, let's keep it simple
	// and stick to the primary for now to avoid confusion.
	return s.redis[shardID]
}

func (s *PGStore) monitorShards() {
	ticker := time.NewTicker(10 * time.Second)
	for range ticker.C {
		for i, db := range s.dbs {
			if err := db.Ping(); err != nil {
				s.healthyMu.Lock()
				s.healthyShards[i] = false
				s.healthyMu.Unlock()
				s.logger.Error("Shard unhealthy", zap.Int("shard", i), zap.Error(err))
			} else {
				s.healthyMu.Lock()
				s.healthyShards[i] = true
				s.healthyMu.Unlock()
			}
		}
	}
}

func (s *PGStore) UpsertItems(ctx context.Context, items []Item) ([]int64, error) {
	if len(items) == 0 {
		return nil, nil
	}
	namespace := items[0].Namespace
	if quota, ok := s.config.NamespaceQuotas[namespace]; ok {
		count, err := s.countEnqueuesLastMinute(ctx, namespace)
		if err != nil {
			return nil, err
		}
		if count >= quota {
			return nil, fmt.Errorf("namespace %s exceeded quota of %d enqueues/minute", namespace, quota)
		}
	}

	dbs := s.GetHealthyDBsForShard(items[0].Namespace, items[0].Topic)
	if len(dbs) == 0 {
		return nil, fmt.Errorf("no healthy shards available to upsert items")
	}
	db := dbs[0]

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	var ids []int64
	for _, item := range items {
		var rawID int64
		metaBytes, err := json.Marshal(item.Metadata)
		if err != nil {
			return nil, fmt.Errorf("marshal metadata: %w", err)
		}

		query := `
               INSERT INTO items (id, namespace, topic, idempotency_key, priority, payload, metadata, deliver_after, lease_expires_at, lease_owner, status, retries, created_at, updated_at, first_failed_at, last_error)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
               ON CONFLICT (id) DO UPDATE
               SET priority = EXCLUDED.priority,
                   payload = EXCLUDED.payload,
                   metadata = EXCLUDED.metadata,
                   deliver_after = EXCLUDED.deliver_after,
                   lease_expires_at = EXCLUDED.lease_expires_at,
                   lease_owner = EXCLUDED.lease_owner,
                   status = EXCLUDED.status,
                   retries = EXCLUDED.retries,
                   updated_at = EXCLUDED.updated_at,
                   first_failed_at = EXCLUDED.first_failed_at,
                   last_error = EXCLUDED.last_error
               RETURNING id
		`
		if item.IdempotencyKey != nil && *item.IdempotencyKey != "" {
			query = `
               INSERT INTO items (id, namespace, topic, idempotency_key, priority, payload, metadata, deliver_after, lease_expires_at, lease_owner, status, retries, created_at, updated_at, first_failed_at, last_error)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
               ON CONFLICT (namespace, idempotency_key) DO UPDATE
               SET priority = EXCLUDED.priority,
                   payload = EXCLUDED.payload,
                   metadata = EXCLUDED.metadata,
                   deliver_after = EXCLUDED.deliver_after,
                   lease_expires_at = EXCLUDED.lease_expires_at,
                   lease_owner = EXCLUDED.lease_owner,
                   status = EXCLUDED.status,
                   retries = EXCLUDED.retries,
                   updated_at = EXCLUDED.updated_at,
                   first_failed_at = EXCLUDED.first_failed_at,
                   last_error = EXCLUDED.last_error
               RETURNING id
			`
		}

		err = tx.QueryRowContext(ctx, query, item.ID, item.Namespace, item.Topic, item.IdempotencyKey, item.Priority, item.Payload, metaBytes,
			item.DeliverAfter, item.LeaseExpiresAt, item.LeaseOwner, item.Status, item.Retries, item.CreatedAt, item.UpdatedAt, item.FirstFailedAt, item.LastError).Scan(&rawID)
		if err != nil {
			return nil, fmt.Errorf("upsert item: %w", err)
		}
		ids = append(ids, rawID)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}
	return ids, nil
}

func (s *PGStore) countEnqueuesLastMinute(ctx context.Context, namespace string) (int, error) {
	var count int
	for i, db := range s.dbs {
		s.healthyMu.RLock()
		isHealthy := s.healthyShards[i]
		s.healthyMu.RUnlock()
		if !isHealthy {
			s.logger.Warn("Skipping unhealthy shard in countEnqueuesLastMinute", zap.Int("shard", i))
			continue
		}
		var shardCount int
		err := db.QueryRowContext(ctx, `
               SELECT COUNT(*) FROM items
               WHERE namespace = $1 AND created_at >= $2
           `, namespace, time.Now().Add(-1*time.Minute)).Scan(&shardCount)
		if err != nil {
			return 0, err
		}
		count += shardCount
	}
	return count, nil
}

func (s *PGStore) LeaseItems(ctx context.Context, namespace, topic, leaseOwner string, limit int, leaseDuration time.Duration) ([]Item, error) {
	dbs := s.GetHealthyDBsForShard(namespace, topic)
	if len(dbs) == 0 {
		return nil, fmt.Errorf("no healthy shards available to lease items")
	}

	client := s.GetRedisForShard(namespace, topic)

	var items []Item
	remaining := limit
	leaseTime := time.Now().Add(leaseDuration)
	now := time.Now()

	for _, db := range dbs {
		if remaining <= 0 {
			break
		}
		rows, err := db.QueryContext(ctx, `
               SELECT id, namespace, topic, idempotency_key, priority, payload, metadata, deliver_after, lease_expires_at, lease_owner, ttl, status, retries, created_at, updated_at, first_failed_at, last_error
               FROM items
               WHERE namespace = $1 AND topic = $2
               AND status = 'ready'
               AND (deliver_after <= $3)
               ORDER BY priority, deliver_after
               LIMIT $4
           `, namespace, topic, now, remaining*3)
		if err != nil {
			s.logger.Error("Lease items select failed on a shard, trying fallback", zap.Error(err))
			continue
		}

		func() {
			defer rows.Close()
			for rows.Next() {
				if remaining <= 0 {
					return
				}
				var item Item
				var metaBytes []byte
				err := rows.Scan(&item.ID, &item.Namespace, &item.Topic, &item.IdempotencyKey, &item.Priority, &item.Payload, &metaBytes,
					&item.DeliverAfter, &item.LeaseExpiresAt, &item.LeaseOwner, &item.TTL, &item.Status, &item.Retries, &item.CreatedAt,
					&item.UpdatedAt, &item.FirstFailedAt, &item.LastError)
				if err != nil {
					s.logger.Error("Scan candidate item failed", zap.Error(err))
					continue
				}
				if len(metaBytes) > 0 {
					if err := json.Unmarshal(metaBytes, &item.Metadata); err != nil {
						s.logger.Error("Unmarshal metadata failed", zap.Error(err))
						continue
					}
				}

				// Acquire distributed lock in Redis
				lockKey := fmt.Sprintf("mqueue:lease_lock:%d", item.ID)
				ok, err := client.SetNX(ctx, lockKey, leaseOwner, leaseDuration).Result()
				if err != nil || !ok {
					continue // Already leased or Redis connection error
				}

				// Set lease details in memory for downstream consumer
				item.LeaseExpiresAt = &leaseTime
				item.LeaseOwner = &leaseOwner

				items = append(items, item)
				remaining--
			}
		}()
	}

	sort.Slice(items, func(i, j int) bool {
		if items[i].Priority != items[j].Priority {
			return items[i].Priority < items[j].Priority
		}
		return items[i].DeliverAfter.Before(items[j].DeliverAfter)
	})

	return items, nil
}

func (s *PGStore) AckItem(ctx context.Context, namespace, topic string, itemID int64) error {
	dbs := s.GetHealthyDBsForShard(namespace, topic)
	if len(dbs) == 0 {
		return fmt.Errorf("no healthy shards available to ack item")
	}

	var lastErr error
	deleted := false
	for _, db := range dbs {
		res, err := db.ExecContext(ctx, `
               DELETE FROM items WHERE id = $1
           `, itemID)
		if err != nil {
			lastErr = err
			continue
		}
		rows, err := res.RowsAffected()
		if err == nil && rows > 0 {
			deleted = true
			break
		}
	}

	s.cacheMu.Lock()
	delete(s.leaseCache, itemID)
	s.cacheMu.Unlock()

	// Clean up Redis lease keys
	client := s.GetRedisForShard(namespace, topic)
	client.Del(ctx, fmt.Sprintf("mqueue:lease_lock:%d", itemID))
	client.Del(ctx, fmt.Sprintf("mqueue:lease:%d", itemID))
	client.SRem(ctx, fmt.Sprintf("mqueue:leases:%s:%s", namespace, topic), fmt.Sprintf("mqueue:lease:%d", itemID))

	if !deleted && lastErr != nil {
		return fmt.Errorf("ack item on shards: %w", lastErr)
	}
	return nil
}

func (s *PGStore) ReleaseLease(ctx context.Context, namespace, topic string, itemID int64) error {
	client := s.GetRedisForShard(namespace, topic)
	client.Del(ctx, fmt.Sprintf("mqueue:lease_lock:%d", itemID))
	client.Del(ctx, fmt.Sprintf("mqueue:lease:%d", itemID))
	client.SRem(ctx, fmt.Sprintf("mqueue:leases:%s:%s", namespace, topic), fmt.Sprintf("mqueue:lease:%d", itemID))
	return nil
}

func (s *PGStore) GetItem(ctx context.Context, namespace, topic string, itemID int64) (Item, error) {
	dbs := s.GetHealthyDBsForShard(namespace, topic)
	if len(dbs) == 0 {
		return Item{}, fmt.Errorf("no healthy shards available to get item")
	}

	var lastErr error
	for _, db := range dbs {
		var item Item
		var metaBytes []byte
		err := db.QueryRowContext(ctx, `
               SELECT id, namespace, topic, idempotency_key, priority, payload, metadata, deliver_after, lease_expires_at, lease_owner, ttl, status, retries, created_at, updated_at, first_failed_at, last_error
               FROM items WHERE id = $1
           `, itemID).Scan(&item.ID, &item.Namespace, &item.Topic, &item.IdempotencyKey, &item.Priority, &item.Payload, &metaBytes,
			&item.DeliverAfter, &item.LeaseExpiresAt, &item.LeaseOwner, &item.TTL, &item.Status, &item.Retries, &item.CreatedAt, &item.UpdatedAt, &item.FirstFailedAt, &item.LastError)
		if err == nil {
			if len(metaBytes) > 0 {
				_ = json.Unmarshal(metaBytes, &item.Metadata)
			}
			return item, nil
		}
		if err != sql.ErrNoRows {
			lastErr = err
		}
	}
	if lastErr != nil {
		return Item{}, fmt.Errorf("get item: %w", lastErr)
	}
	return Item{}, sql.ErrNoRows
}

func (s *PGStore) MoveToDLQ(ctx context.Context, item Item, lastError string) error {
	dbs := s.GetHealthyDBsForShard(item.Namespace, item.Topic)
	if len(dbs) == 0 {
		return fmt.Errorf("no healthy shards available to move to DLQ")
	}

	metaBytes, err := json.Marshal(item.Metadata)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}

	var lastErr error
	moved := false
	for _, db := range dbs {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			lastErr = err
			continue
		}

		res, err := tx.ExecContext(ctx, `
               DELETE FROM items WHERE id = $1
           `, item.ID)
		if err != nil {
			tx.Rollback()
			lastErr = err
			continue
		}
		rows, err := res.RowsAffected()
		if err != nil || rows == 0 {
			tx.Rollback()
			if err != nil {
				lastErr = err
			}
			continue
		}

		_, err = tx.ExecContext(ctx, `
               INSERT INTO dead_letter (original_id, namespace, topic, idempotency_key, priority, payload, metadata, last_error, retries, first_failed_at, moved_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
           `, item.ID, item.Namespace, item.Topic, item.IdempotencyKey, item.Priority, item.Payload, metaBytes, lastError, item.Retries, item.FirstFailedAt, time.Now())
		if err != nil {
			tx.Rollback()
			lastErr = err
			continue
		}

		if err := tx.Commit(); err != nil {
			lastErr = err
			continue
		}
		moved = true
		break
	}

	if !moved {
		if lastErr != nil {
			return fmt.Errorf("move to DLQ on shards: %w", lastErr)
		}
		return fmt.Errorf("item not found on any shard to move to DLQ")
	}
	return nil
}

func (s *PGStore) GetActiveTopics(ctx context.Context, namespace string) ([]string, error) {
	var topics []string
	seen := make(map[string]bool)
	for i, db := range s.dbs {
		s.healthyMu.RLock()
		isHealthy := s.healthyShards[i]
		s.healthyMu.RUnlock()
		if !isHealthy {
			s.logger.Warn("Skipping unhealthy shard in GetActiveTopics", zap.Int("shard", i))
			continue
		}

		rows, err := db.QueryContext(ctx, `
               SELECT DISTINCT topic FROM items WHERE namespace = $1 AND status = 'ready'
               UNION
               SELECT DISTINCT topic FROM dead_letter WHERE namespace = $1
           `, namespace)
		if err != nil {
			s.logger.Error("Query active topics failed on a shard", zap.Int("shard", i), zap.Error(err))
			continue
		}
		defer rows.Close()
		for rows.Next() {
			var topic string
			if err := rows.Scan(&topic); err != nil {
				return nil, err
			}
			if !seen[topic] {
				topics = append(topics, topic)
				seen[topic] = true
			}
		}
	}
	
	// Also check Redis for buffered topics
	for _, client := range s.redis {
		topicSetKey := fmt.Sprintf("mqueue:active_topics:%s", namespace)
		redisTopics, err := client.SMembers(ctx, topicSetKey).Result()
		if err == nil {
			for _, topic := range redisTopics {
				if !seen[topic] {
					topics = append(topics, topic)
					seen[topic] = true
				}
			}
		}
	}
	return topics, nil
}

func (s *PGStore) CleanEmptyTopics(ctx context.Context, namespace string) error {
	for i, db := range s.dbs {
		s.healthyMu.RLock()
		isHealthy := s.healthyShards[i]
		s.healthyMu.RUnlock()
		if !isHealthy {
			continue
		}

		_, err := db.ExecContext(ctx, `
               DELETE FROM items WHERE namespace = $1 AND status = 'done'
           `, namespace)
		if err != nil {
			return fmt.Errorf("clean topics: %w", err)
		}
	}
	return nil
}

func (s *PGStore) Recover(ctx context.Context, walManager *wal.WALManager) error {
	for shardID := 0; shardID < len(s.dbs); shardID++ {
		s.healthyMu.RLock()
		isHealthy := s.healthyShards[shardID]
		s.healthyMu.RUnlock()
		if !isHealthy {
			s.logger.Warn("Skipping WAL recovery for unhealthy shard", zap.Int("shard", shardID))
			continue
		}

		entries, err := walManager.ReadWAL(shardID)
		if err != nil {
			return fmt.Errorf("read WAL for shard %d: %w", shardID, err)
		}
		for _, entry := range entries {
			var items []Item
			if err := json.Unmarshal(entry, &items); err != nil {
				return fmt.Errorf("unmarshal WAL entry for shard %d: %w", shardID, err)
			}
			if _, err := s.UpsertItems(ctx, items); err != nil {
				return fmt.Errorf("recover items for shard %d: %w", shardID, err)
			}
		}
	}
	return nil
}

// IsPoolSaturated checks if any PostgreSQL shard's connection pool is saturated (e.g. 90% or more connections are in-use).
func (s *PGStore) IsPoolSaturated() bool {
	for _, db := range s.dbs {
		stats := db.Stats()
		if stats.MaxOpenConnections > 0 {
			// If in-use connections reach 90% of max open connections, consider it saturated
			if float64(stats.InUse)/float64(stats.MaxOpenConnections) >= 0.9 {
				return true
			}
		}
	}
	return false
}
