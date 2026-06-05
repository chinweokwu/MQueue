package recovery

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"mqueue/internal/config"
	"mqueue/internal/log"
	"mqueue/internal/store"
	"mqueue/internal/wal"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type RecoveryDaemon struct {
	redisClients []redis.UniversalClient
	pgStore      *store.PGStore
	walManager   *wal.WALManager
	cfg          *config.Config
	logger       *log.Logger
	healthy      map[int]bool
	Interval     time.Duration
}

func NewRecoveryDaemon(redisClients []redis.UniversalClient, pgStore *store.PGStore, walManager *wal.WALManager, cfg *config.Config, logger *log.Logger) *RecoveryDaemon {
	healthy := make(map[int]bool)
	for i := range redisClients {
		healthy[i] = true // Assume healthy on startup
	}
	return &RecoveryDaemon{
		redisClients: redisClients,
		pgStore:      pgStore,
		walManager:   walManager,
		cfg:          cfg,
		logger:       logger,
		healthy:      healthy,
		Interval:     5 * time.Second,
	}
}

func (r *RecoveryDaemon) Run(ctx context.Context) {
	ticker := time.NewTicker(r.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for i, client := range r.redisClients {
				err := client.Ping(ctx).Err()
				if err != nil {
					if r.healthy[i] {
						r.logger.Warn("Redis shard went offline", zap.Int("shard", i), zap.Error(err))
						r.healthy[i] = false
					}
					continue
				}

				// If it was unhealthy and is now healthy, perform recovery
				if !r.healthy[i] {
					r.logger.Info("Redis shard recovered, starting WAL replay...", zap.Int("shard", i))
					r.healthy[i] = true

					// Force rotation of active WAL first so we can read and clean safely
					if err := r.walManager.RotateForce(i); err != nil {
						r.logger.Error("Failed to force rotate WAL for recovery", zap.Int("shard", i), zap.Error(err))
						continue
					}

					// Try to acquire distributed lock for recovery on this shard
					lockKey := fmt.Sprintf("mqueue:lock:recovery:%d", i)
					lockValue := r.randomString()
					
					// Set lock with 30 second expiration to prevent deadlock
					acquired, err := client.SetNX(ctx, lockKey, lockValue, 30*time.Second).Result()
					if err != nil || !acquired {
						r.logger.Info("Another node is already running recovery on this shard", zap.Int("shard", i))
						continue
					}

					// Perform WAL replay for this shard
					go func(shardID int, key string, val string, cl redis.UniversalClient) {
						defer cl.Del(context.Background(), key)
						
						r.logger.Info("Replaying WAL for shard", zap.Int("shard", shardID))
						entries, err := r.walManager.ReadAndCleanRotatedWALs(shardID)
						if err != nil {
							r.logger.Error("Failed to read rotated WALs for recovered shard", zap.Int("shard", shardID), zap.Error(err))
							return
						}
						
						count := 0
						for _, entry := range entries {
							var items []store.Item
							if err := json.Unmarshal(entry, &items); err != nil {
								r.logger.Error("Failed to unmarshal WAL entry", zap.Error(err))
								continue
							}
							if _, err := r.pgStore.UpsertItems(ctx, items); err != nil {
								r.logger.Error("Failed to persist WAL items to DB", zap.Error(err))
								continue
							}
							count += len(items)
						}
						r.logger.Info("Successfully recovered items from WAL to PostgreSQL", zap.Int("shard", shardID), zap.Int("count", count))
					}(i, lockKey, lockValue, client)
				}
			}
		}
	}
}

func (r *RecoveryDaemon) randomString() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
