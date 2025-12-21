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

func (s *PGStore) getDBForShard(namespace, topic string) *sql.DB {
	shardID := s.GetShardID(namespace, topic)
	s.healthyMu.RLock()
	if s.healthyShards[shardID] {
		s.healthyMu.RUnlock()
		return s.dbs[shardID]
	}
	s.healthyMu.RUnlock()
	return s.dbs[(shardID+1)%len(s.dbs)]
}

func (s *PGStore) GetRedisForShard(namespace, topic string) *redis.Client {
	shardID := s.GetShardID(namespace, topic)
	s.healthyMu.RLock()
	if s.healthyShards[shardID] {
		s.healthyMu.RUnlock()
		return s.redis[shardID]
	}
	s.healthyMu.RUnlock()
	return s.redis[(shardID+1)%len(s.redis)]
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

	db := s.getDBForShard(items[0].Namespace, items[0].Topic)
	shardID := s.GetShardID(items[0].Namespace, items[0].Topic)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	var ids []int64
	for _, item := range items {
		var rawID int64
		err := tx.QueryRowContext(ctx, `
               INSERT INTO items (namespace, topic, idempotency_key, priority, payload, metadata, deliver_after, status, retries, created_at, updated_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
               ON CONFLICT (namespace, idempotency_key) DO UPDATE
               SET priority = EXCLUDED.priority,
                   payload = EXCLUDED.payload,
                   metadata = EXCLUDED.metadata,
                   deliver_after = EXCLUDED.deliver_after,
                   status = EXCLUDED.status,
                   updated_at = EXCLUDED.updated_at
               RETURNING id
           `, item.Namespace, item.Topic, item.IdempotencyKey, item.Priority, item.Payload, item.Metadata,
			item.DeliverAfter, item.Status, item.Retries, item.CreatedAt, item.UpdatedAt).Scan(&rawID)
		if err != nil {
			return nil, fmt.Errorf("upsert item: %w", err)
		}
		encodedID := int64(shardID)<<56 | rawID
		ids = append(ids, encodedID)
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
		if err != nil {
			return 0, err
		}
		count += shardCount
	}
	return count, nil
}

func (s *PGStore) LeaseItems(ctx context.Context, namespace, topic, leaseOwner string, limit int, leaseDuration time.Duration) ([]Item, error) {
	db := s.getDBForShard(namespace, topic)
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

	shardID := s.GetShardID(namespace, topic)
	var items []Item
	for rows.Next() {
		var item Item
		err := rows.Scan(&item.ID, &item.Namespace, &item.Topic, &item.IdempotencyKey, &item.Priority, &item.Payload, &item.Metadata,
			&item.DeliverAfter, &item.LeaseExpiresAt, &item.LeaseOwner, &item.TTL, &item.Status, &item.Retries, &item.CreatedAt,
			&item.UpdatedAt, &item.FirstFailedAt)
		if err != nil {
			return nil, fmt.Errorf("scan item: %w", err)
		}
		item.ID = int64(shardID)<<56 | item.ID
		items = append(items, item)
	}
	return items, nil
}

func (s *PGStore) AckItem(ctx context.Context, itemID int64) error {
	shardID := int(itemID >> 56)
	if shardID >= len(s.dbs) {
		return fmt.Errorf("invalid shard ID in item ID %d", itemID)
	}
	db := s.dbs[shardID]
	_, err := db.ExecContext(ctx, `
           DELETE FROM items WHERE id = $1
       `, itemID&0x00FFFFFFFFFFFFFF)
	if err != nil {
		return fmt.Errorf("ack item: %w", err)
	}
	s.cacheMu.Lock()
	delete(s.leaseCache, itemID)
	s.cacheMu.Unlock()
	return nil
}

func (s *PGStore) GetItem(ctx context.Context, itemID int64) (Item, error) {
	shardID := int(itemID >> 56)
	if shardID >= len(s.dbs) {
		return Item{}, fmt.Errorf("invalid shard ID in item ID %d", itemID)
	}
	db := s.dbs[shardID]
	var item Item
	err := db.QueryRowContext(ctx, `
           SELECT id, namespace, topic, idempotency_key, priority, payload, metadata, deliver_after, lease_expires_at, lease_owner, ttl, status, retries, created_at, updated_at, first_failed_at
           FROM items WHERE id = $1
       `, itemID&0x00FFFFFFFFFFFFFF).Scan(&item.ID, &item.Namespace, &item.Topic, &item.IdempotencyKey, &item.Priority, &item.Payload, &item.Metadata, &item.DeliverAfter, &item.LeaseExpiresAt, &item.LeaseOwner, &item.TTL, &item.Status, &item.Retries, &item.CreatedAt, &item.UpdatedAt, &item.FirstFailedAt)
	if err != nil {
		return Item{}, fmt.Errorf("get item: %w", err)
	}
	item.ID = itemID
	return item, nil
}

func (s *PGStore) MoveToDLQ(ctx context.Context, item Item, lastError string) error {
	db := s.getDBForShard(item.Namespace, item.Topic)
	_, err := db.ExecContext(ctx, `
           INSERT INTO dead_letter (original_id, namespace, topic, idempotency_key, priority, payload, metadata, last_error, retries, first_failed_at, moved_at)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
       `, item.ID, item.Namespace, item.Topic, item.IdempotencyKey, item.Priority, item.Payload, item.Metadata, lastError, item.Retries, item.FirstFailedAt, time.Now())
	if err != nil {
		return fmt.Errorf("insert into DLQ: %w", err)
	}
	_, err = db.ExecContext(ctx, `
           DELETE FROM items WHERE id = $1
       `, item.ID&0x00FFFFFFFFFFFFFF)
	if err != nil {
		return fmt.Errorf("delete from items: %w", err)
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
