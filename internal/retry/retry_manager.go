package retry

import (
	"context"
	"fmt"
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

	// Apply exponential backoff
	backoff := r.cfg.RetryBackoff * time.Duration(1<<item.Retries)
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
