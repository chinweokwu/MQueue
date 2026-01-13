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
	redis         []*redis.Client
	leaseCache    map[int64]string
	cacheMu       sync.RWMutex
	config        *config.Config
	logger        *log.Logger
	healthyShards map[int]bool
	healthyMu     sync.RWMutex
}

func NewPGStore(dbURLs []string, redisClients []*redis.Client, cfg *config.Config) (*PGStore, error) {
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

func (s *PGStore) GetRedisClients() []*redis.Client {
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

func (s *PGStore) GetRedisForShard(namespace, topic string) *redis.Client {
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

	db, err := s.getDBForShard(items[0].Namespace, items[0].Topic)
	if err != nil {
		return nil, fmt.Errorf("get db: %w", err)
	}
	// shardID := s.GetShardID(items[0].Namespace, items[0].Topic) // Not needed for encoding
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
		// Insert with explicit ID
		err = tx.QueryRowContext(ctx, `
               INSERT INTO items (id, namespace, topic, idempotency_key, priority, payload, metadata, deliver_after, status, retries, created_at, updated_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
               ON CONFLICT (id) DO UPDATE
               SET priority = EXCLUDED.priority,
                   payload = EXCLUDED.payload,
                   metadata = EXCLUDED.metadata,
                   deliver_after = EXCLUDED.deliver_after,
                   status = EXCLUDED.status,
                   updated_at = EXCLUDED.updated_at
               RETURNING id
           `, item.ID, item.Namespace, item.Topic, item.IdempotencyKey, item.Priority, item.Payload, metaBytes,
			item.DeliverAfter, item.Status, item.Retries, item.CreatedAt, item.UpdatedAt).Scan(&rawID)
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
	for _, db := range s.dbs {
		var shardCount int
		err := db.QueryRowContext(ctx, `
               SELECT COUNT(*) FROM items
               WHERE namespace = $1 AND created_at >= $2
           `, namespace, time.Now().Add(-1*time.Minute)).Scan(&shardCount)
		// If a shard is down, we might fail or partial count.
		// For quotas, failing open (ignoring down shard) or failing closed?
		// Let's just log error and continue if we want robustness, or fail strict.
		// For now, fail strict as per request to avoid inconsistencies.
		if err != nil {
			return 0, err
		}
		count += shardCount
	}
	return count, nil
}

func (s *PGStore) LeaseItems(ctx context.Context, namespace, topic, leaseOwner string, limit int, leaseDuration time.Duration) ([]Item, error) {
	db, err := s.getDBForShard(namespace, topic)
	if err != nil {
		return nil, fmt.Errorf("get db: %w", err)
	}
	rows, err := db.QueryContext(ctx, `
           UPDATE items
           SET lease_expires_at = $1, lease_owner = $2
           WHERE id IN (
               SELECT id FROM items
               WHERE namespace = $3 AND topic = $4
               AND status = 'ready'
               AND (deliver_after <= $5)
               AND (lease_expires_at IS NULL OR lease_expires_at < $5)
               ORDER BY priority, deliver_after
               LIMIT $6
               FOR UPDATE SKIP LOCKED
           )
           RETURNING id, namespace, topic, idempotency_key, priority, payload, metadata, deliver_after, lease_expires_at, lease_owner, ttl, status, retries, created_at, updated_at, first_failed_at
       `, time.Now().Add(leaseDuration), leaseOwner, namespace, topic, time.Now(), limit)
	if err != nil {
		return nil, fmt.Errorf("lease items: %w", err)
	}
	defer rows.Close()

	var items []Item
	for rows.Next() {
		var item Item
		var metaBytes []byte
		err := rows.Scan(&item.ID, &item.Namespace, &item.Topic, &item.IdempotencyKey, &item.Priority, &item.Payload, &metaBytes,
			&item.DeliverAfter, &item.LeaseExpiresAt, &item.LeaseOwner, &item.TTL, &item.Status, &item.Retries, &item.CreatedAt,
			&item.UpdatedAt, &item.FirstFailedAt)
		if err != nil {
			return nil, fmt.Errorf("scan item: %w", err)
		}
		if len(metaBytes) > 0 {
			if err := json.Unmarshal(metaBytes, &item.Metadata); err != nil {
				return nil, fmt.Errorf("unmarshal metadata: %w", err)
			}
		}
		// IDs are global, no encoding
		items = append(items, item)
	}
	// Sort items by priority (ASC) then deliver_after (ASC) as RETURNING order is not guaranteed
	sort.Slice(items, func(i, j int) bool {
		if items[i].Priority != items[j].Priority {
			// Lower priority value = Higher priority (usually)
			// But check expectation: 1 is High, 10 is Low.
			// Postgres ORDER BY priority ASC -> 1 first.
			return items[i].Priority < items[j].Priority
		}
		return items[i].DeliverAfter.Before(items[j].DeliverAfter)
	})

	return items, nil
}

func (s *PGStore) AckItem(ctx context.Context, namespace, topic string, itemID int64) error {
	db, err := s.getDBForShard(namespace, topic)
	if err != nil {
		return fmt.Errorf("get db: %w", err)
	}
	_, err = db.ExecContext(ctx, `
           DELETE FROM items WHERE id = $1
       `, itemID)
	if err != nil {
		return fmt.Errorf("ack item: %w", err)
	}
	s.cacheMu.Lock()
	delete(s.leaseCache, itemID)
	s.cacheMu.Unlock()
	return nil
}

func (s *PGStore) GetItem(ctx context.Context, namespace, topic string, itemID int64) (Item, error) {
	db, err := s.getDBForShard(namespace, topic)
	if err != nil {
		return Item{}, fmt.Errorf("get db: %w", err)
	}
	var item Item
	var metaBytes []byte
	err = db.QueryRowContext(ctx, `
           SELECT id, namespace, topic, idempotency_key, priority, payload, metadata, deliver_after, lease_expires_at, lease_owner, ttl, status, retries, created_at, updated_at, first_failed_at
           FROM items WHERE id = $1
       `, itemID).Scan(&item.ID, &item.Namespace, &item.Topic, &item.IdempotencyKey, &item.Priority, &item.Payload, &metaBytes, &item.DeliverAfter, &item.LeaseExpiresAt, &item.LeaseOwner, &item.TTL, &item.Status, &item.Retries, &item.CreatedAt, &item.UpdatedAt, &item.FirstFailedAt)
	if err == nil && len(metaBytes) > 0 {
		_ = json.Unmarshal(metaBytes, &item.Metadata)
	}
	if err != nil {
		return Item{}, fmt.Errorf("get item: %w", err)
	}
	return item, nil
}

func (s *PGStore) MoveToDLQ(ctx context.Context, item Item, lastError string) error {
	db, err := s.getDBForShard(item.Namespace, item.Topic)
	if err != nil {
		return fmt.Errorf("get db: %w", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	metaBytes, _ := json.Marshal(item.Metadata)
	_, err = tx.ExecContext(ctx, `
           INSERT INTO dead_letter (original_id, namespace, topic, idempotency_key, priority, payload, metadata, last_error, retries, first_failed_at, moved_at)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
       `, item.ID, item.Namespace, item.Topic, item.IdempotencyKey, item.Priority, item.Payload, metaBytes, lastError, item.Retries, item.FirstFailedAt, time.Now())
	if err != nil {
		return fmt.Errorf("insert into DLQ: %w", err)
	}

	_, err = tx.ExecContext(ctx, `
           DELETE FROM items WHERE id = $1
       `, item.ID)
	if err != nil {
		return fmt.Errorf("delete from items: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

func (s *PGStore) GetActiveTopics(ctx context.Context, namespace string) ([]string, error) {
	var topics []string
	seen := make(map[string]bool)
	for _, db := range s.dbs {
		rows, err := db.QueryContext(ctx, `
               SELECT DISTINCT topic FROM items WHERE namespace = $1 AND status = 'ready'
               UNION
               SELECT DISTINCT topic FROM dead_letter WHERE namespace = $1
           `, namespace)
		if err != nil {
			return nil, fmt.Errorf("query active topics: %w", err)
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
	for _, db := range s.dbs {
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
