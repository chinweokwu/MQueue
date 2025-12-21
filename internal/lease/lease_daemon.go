package lease

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"mqueue/internal/config"
	"mqueue/internal/log"
	"mqueue/internal/store"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type LeaseDaemon struct {
	clients []*redis.Client
	store   *store.PGStore
	cfg     *config.Config
	logger  *log.Logger
}

func NewLeaseDaemon(clients []*redis.Client, store *store.PGStore, cfg *config.Config, logger *log.Logger) *LeaseDaemon {
	return &LeaseDaemon{
		clients: clients,
		store:   store,
		cfg:     cfg,
		logger:  logger,
	}
}

func (f *LeaseDaemon) Run(ctx context.Context) {
	ticker := time.NewTicker(f.cfg.LeaseRenewPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			f.logger.Info("Lease daemon shutting down")
			return
		case <-ticker.C:
			if err := f.renewLeases(ctx); err != nil {
				f.logger.Error("Failed to renew leases", zap.Error(err))
			}
		}
	}
}

func (f *LeaseDaemon) renewLeases(ctx context.Context) error {
	for namespace := range f.cfg.NamespaceQuotas {
		topics, err := f.store.GetActiveTopics(ctx, namespace)
		if err != nil {
			f.logger.Error("Failed to get active topics", zap.Error(err))
			return fmt.Errorf("get active topics: %w", err)
		}
		for _, topic := range topics {
			client := f.store.GetRedisForShard(namespace, topic)
			leaseKeys, err := client.SMembers(ctx, fmt.Sprintf("mqueue:leases:%s:%s", namespace, topic)).Result()
			if err != nil {
				f.logger.Error("Failed to get lease keys", zap.Error(err))
				return fmt.Errorf("get lease keys: %w", err)
			}
			for _, key := range leaseKeys {
				data, err := client.Get(ctx, key).Bytes()
				if err == redis.Nil {
					client.SRem(ctx, fmt.Sprintf("mqueue:leases:%s:%s", namespace, topic), key)
					continue
				}
				if err != nil {
					f.logger.Error("Failed to get lease data", zap.Error(err), zap.String("key", key))
					return fmt.Errorf("get lease data: %w", err)
				}
				var item store.Item
				if err := json.Unmarshal(data, &item); err != nil {
					f.logger.Error("Failed to unmarshal lease", zap.Error(err), zap.String("key", key))
					return fmt.Errorf("unmarshal lease: %w", err)
				}
				if item.LeaseExpiresAt == nil || item.LeaseExpiresAt.Before(time.Now()) {
					client.SRem(ctx, fmt.Sprintf("mqueue:leases:%s:%s", namespace, topic), key)
					continue
				}
				newExpiresAt := time.Now().Add(f.cfg.LeaseTTL)
				item.LeaseExpiresAt = &newExpiresAt
				item.UpdatedAt = time.Now()
				if _, err := f.store.UpsertItems(ctx, []store.Item{item}); err != nil {
					f.logger.Error("Failed to renew lease", zap.Error(err), zap.Int64("item_id", item.ID))
					return fmt.Errorf("renew lease: %w", err)
				}
				data, err = json.Marshal(item)
				if err != nil {
					f.logger.Error("Failed to marshal item for lease update", zap.Error(err))
					return fmt.Errorf("marshal item: %w", err)
				}
				if err := client.Set(ctx, key, data, f.cfg.LeaseTTL).Err(); err != nil {
					f.logger.Error("Failed to update lease in Redis", zap.Error(err), zap.String("key", key))
					return fmt.Errorf("update lease in redis: %w", err)
				}
			}
		}
	}
	return nil
}
