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

		// 1. Resolve existing IDs for idempotent items
		var idKeys []string
		var idempIndices []int
		seenInBatch := make(map[string]int64)

		for i, item := range shardItems {
			if item.IdempotencyKey != nil {
				idKey := fmt.Sprintf("mqueue:dedup:%s:%s:%s", key.namespace, key.topic, *item.IdempotencyKey)
				idKeys = append(idKeys, idKey)
				idempIndices = append(idempIndices, i)
			}
		}

		existingIDs := make(map[string]int64)
		if len(idKeys) > 0 {
			results, err := client.MGet(ctx, idKeys...).Result()
			if err != nil {
				b.logger.Error("Failed to check dedup keys in Redis", zap.Error(err))
				return nil, err
			}
			for i, result := range results {
				if result != nil {
					if idStr, ok := result.(string); ok {
						// Assuming simple integer stored as string
						var idVal int64
						if _, err := fmt.Sscanf(idStr, "%d", &idVal); err == nil {
							idx := idempIndices[i]
							k := *shardItems[idx].IdempotencyKey
							existingIDs[k] = idVal
						}
					}
				}
			}
		}

		pipe := client.Pipeline()
		var ids []int64

		for i := range shardItems {
			// Pointer to item allows modification
			item := &shardItems[i]

			if item.IdempotencyKey == nil {
				// Non-idempotent: New ID
				item.ID = b.node.Generate()
			} else {
				k := *item.IdempotencyKey
				if id, ok := seenInBatch[k]; ok {
					item.ID = id
				} else if id, ok := existingIDs[k]; ok {
					item.ID = id
					seenInBatch[k] = id
				} else {
					item.ID = b.node.Generate()
					seenInBatch[k] = item.ID
				}
				
				// Update Redis Deduplication Key
				dedupKey := fmt.Sprintf("mqueue:dedup:%s:%s:%s", key.namespace, key.topic, k)
				pipe.Set(ctx, dedupKey, fmt.Sprintf("%d", item.ID), b.cfg.BufferTTL)
			}
			
			ids = append(ids, item.ID)

			// Enqueue the item (Always enqueue to allow Upsert logic in Store)
			data, err := json.Marshal(item)
			if err != nil {
				return nil, fmt.Errorf("marshal item: %w", err)
			}
			pipe.LPush(ctx, listKey, data)
		}

		// Ensure list TTL
		pipe.Expire(ctx, listKey, b.cfg.BufferTTL)
		
		// Track active topic
		topicSetKey := fmt.Sprintf("mqueue:active_topics:%s", key.namespace)
		pipe.SAdd(ctx, topicSetKey, key.topic)

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
