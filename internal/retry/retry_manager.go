package retry

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"mqueue/internal/config"
	"mqueue/internal/log"
	"mqueue/internal/store"

	"go.uber.org/zap"
)

type RetryManager struct {
	pgStore  *store.PGStore
	dlqStore *store.DLQStore
	cfg      *config.Config
	logger   *log.Logger
}

func NewRetryManager(pgStore *store.PGStore, dlqStore *store.DLQStore, cfg *config.Config, logger *log.Logger) *RetryManager {
	return &RetryManager{
		pgStore:  pgStore,
		dlqStore: dlqStore,
		cfg:      cfg,
		logger:   logger,
	}
}

func (r *RetryManager) RetryItem(ctx context.Context, item store.Item, lastError string) error {
	item.Retries++
	item.LastError = &lastError
	item.UpdatedAt = time.Now()
	if item.FirstFailedAt == nil {
		now := time.Now()
		item.FirstFailedAt = &now
	}

	if item.Retries >= r.cfg.MaxRetries {
		// Move to DLQ
		if err := r.pgStore.MoveToDLQ(ctx, item, lastError); err != nil {
			return fmt.Errorf("move to DLQ: %w", err)
		}
		r.logger.Info("Moved item to DLQ", zap.Int64("id", item.ID), zap.Int("retries", item.Retries))
		return nil
	}

	// Apply exponential backoff with jitter
	// Base: 2^retries * initial (e.g. 1s, 2s, 4s...)
	baseBackoff := r.cfg.RetryBackoff * time.Duration(1<<item.Retries)

	// Jitter: +/- 20% to prevent thundering herd
	// rand.Float64() returns [0.0, 1.0)
	jitterFactor := 0.8 + (rand.Float64() * 0.4) // Result: [0.8, 1.2)
	backoff := time.Duration(float64(baseBackoff) * jitterFactor)

	item.DeliverAfter = time.Now().Add(backoff)
	item.Status = "ready"
	item.LeaseExpiresAt = nil
	item.LeaseOwner = nil

	// Update item in Postgres
	_, err := r.pgStore.UpsertItems(ctx, []store.Item{item})
	if err != nil {
		return fmt.Errorf("update item for retry: %w", err)
	}
	r.logger.Info("Retried item", zap.Int64("id", item.ID), zap.Int("retries", item.Retries), zap.Duration("backoff", backoff))
	return nil
}
