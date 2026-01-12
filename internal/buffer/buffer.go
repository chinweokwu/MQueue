package buffer

import (
	"context"
	"encoding/json"
	"fmt"

	"mqueue/internal/config"
	"mqueue/internal/id"
	"mqueue/internal/log"
	"mqueue/internal/store"
	"mqueue/internal/wal"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type RedisBuffer struct {
	clients []*redis.Client
	cfg     *config.Config
	store   *store.PGStore
	logger  *log.Logger
	node    *id.Node
}

func NewRedisBuffer(clients []*redis.Client, cfg *config.Config, store *store.PGStore, logger *log.Logger) *RedisBuffer {
	node, err := id.NewNode(cfg.NodeID)
	if err != nil {
		logger.Fatal("Failed to initialize ID generator", zap.Error(err))
	}
	return &RedisBuffer{
		clients: clients,
		cfg:     cfg,
		store:   store,
		logger:  logger,
		node:    node,
	}
}

func (b *RedisBuffer) Enqueue(ctx context.Context, items []store.Item, wals *wal.WALManager) ([]int64, error) {
	if len(items) == 0 {
		return nil, nil
	}

	// Group items by shard (namespace + topic)
	type shardKey struct {
		namespace, topic string
	}
	groupedItems := make(map[shardKey][]store.Item)
	for _, item := range items {
		key := shardKey{item.Namespace, item.Topic}
		groupedItems[key] = append(groupedItems[key], item)
	}

	var allIDs []int64
	for key, shardItems := range groupedItems {
		shardID := b.store.GetShardID(key.namespace, key.topic)
		client := b.store.GetRedisForShard(key.namespace, key.topic)
		listKey := fmt.Sprintf("mqueue:buffer:%s:%s", key.namespace, key.topic)

		pipe := client.Pipeline()

		// In-memory deduplication for items in the same request batch
		seenInBatch := make(map[string]bool)

		var ids []int64

		for i := range shardItems {
			// Pointer to item allows modification
			item := &shardItems[i]

			// Generate ID immediately (Async Enqueue Magic)
			item.ID = b.node.Generate()
			ids = append(ids, item.ID)

			// Non-idempotent items: enqueue
			if item.IdempotencyKey == nil {
				data, err := json.Marshal(item)
				if err != nil {
					return nil, fmt.Errorf("marshal non-idempotent item: %w", err)
				}
				pipe.LPush(ctx, listKey, data)
				continue
			}

			idempKey := *item.IdempotencyKey

			// 1. Skip if already seen in this batch
			if seenInBatch[idempKey] {
				b.logger.Info("Skipping in-batch duplicate", zap.String("idempotency_key", idempKey))
				continue
			}
			seenInBatch[idempKey] = true

			// 2. Check Redis for cross-request deduplication
			dedupKey := fmt.Sprintf("mqueue:dedup:%s:%s:%s", key.namespace, key.topic, idempKey)
			exists, err := client.Exists(ctx, dedupKey).Result()
			if err != nil {
				b.logger.Error("Failed to check dedup key in Redis", zap.Error(err))
				return nil, err
			}
			if exists > 0 {
				b.logger.Info("Skipping cross-request duplicate", zap.String("idempotency_key", idempKey))
				continue
			}

			// Mark as seen in Redis with TTL
			pipe.Set(ctx, dedupKey, "1", b.cfg.BufferTTL)

			// Enqueue the item
			data, err := json.Marshal(item)
			if err != nil {
				return nil, fmt.Errorf("marshal item: %w", err)
			}
			pipe.LPush(ctx, listKey, data)
		}

		// Ensure list TTL
		pipe.Expire(ctx, listKey, b.cfg.BufferTTL)

		if _, err := pipe.Exec(ctx); err != nil {
			b.logger.Error("Failed to enqueue to Redis buffer", zap.Error(err))
			return nil, fmt.Errorf("enqueue to redis: %w", err)
		}

		// Write full batch to WAL (for crash recovery)
		walData, err := json.Marshal(shardItems)
		if err != nil {
			b.logger.Error("Failed to marshal items for WAL", zap.Error(err))
			return nil, fmt.Errorf("marshal items for WAL: %w", err)
		}
		if err := wals.WriteWAL(shardID, walData); err != nil {
			b.logger.Error("Failed to write to WAL", zap.Error(err))
			return nil, fmt.Errorf("write to WAL: %w", err)
		}

		// ASYNC ENQUEUE: We do NOT call store.UpsertItems here.
		// The Flusher will pick up items from Redis/WAL and insert them into Postgres.

		allIDs = append(allIDs, ids...)
	}

	return allIDs, nil
}

func (b *RedisBuffer) GetItems(ctx context.Context, namespace, topic string) ([]store.Item, error) {
	client := b.store.GetRedisForShard(namespace, topic)
	listKey := fmt.Sprintf("mqueue:buffer:%s:%s", namespace, topic)

	data, err := client.LRange(ctx, listKey, 0, -1).Result()
	if err != nil {
		b.logger.Error("Failed to get items from Redis list", zap.Error(err))
		return nil, fmt.Errorf("get items from redis list: %w", err)
	}

	var items []store.Item
	for _, itemData := range data {
		var item store.Item
		if err := json.Unmarshal([]byte(itemData), &item); err != nil {
			b.logger.Error("Failed to unmarshal item", zap.Error(err))
			return nil, fmt.Errorf("unmarshal item: %w", err)
		}
		items = append(items, item)
	}
	return items, nil
}

func (b *RedisBuffer) DeleteItems(ctx context.Context, namespace, topic string, ids []int64) error {
	client := b.store.GetRedisForShard(namespace, topic)
	listKey := fmt.Sprintf("mqueue:buffer:%s:%s", namespace, topic)

	data, err := client.LRange(ctx, listKey, 0, -1).Result()
	if err != nil {
		b.logger.Error("Failed to get items for deletion", zap.Error(err))
		return fmt.Errorf("get items for deletion: %w", err)
	}

	keepItems := []string{}
	idSet := make(map[int64]bool)
	for _, id := range ids {
		idSet[id] = true
	}

	for _, itemData := range data {
		var item store.Item
		if err := json.Unmarshal([]byte(itemData), &item); err != nil {
			b.logger.Error("Failed to unmarshal item for deletion", zap.Error(err))
			continue
		}
		if !idSet[item.ID] {
			keepItems = append(keepItems, itemData)
		}
	}

	pipe := client.Pipeline()
	pipe.Del(ctx, listKey)
	for _, itemData := range keepItems {
		pipe.LPush(ctx, listKey, itemData)
	}
	if len(keepItems) > 0 {
		pipe.Expire(ctx, listKey, b.cfg.BufferTTL)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		b.logger.Error("Failed to update Redis list after deletion", zap.Error(err))
		return fmt.Errorf("update redis list: %w", err)
	}

	return nil
}
