package flusher

import (
	"context"
	"fmt"
	"time"

	"mqueue/internal/buffer"
	"mqueue/internal/config"
	"mqueue/internal/log"
	"mqueue/internal/store"

	"github.com/sony/gobreaker"
	"go.uber.org/zap"
)

type Flusher struct {
	buffer *buffer.RedisBuffer
	store  *store.PGStore
	cfg    *config.Config
	logger *log.Logger
	cb     *gobreaker.CircuitBreaker
}

func NewFlusher(buffer *buffer.RedisBuffer, store *store.PGStore, cfg *config.Config, logger *log.Logger) *Flusher {
	cb := gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "flusher",
		MaxRequests: 5,
		Interval:    60 * time.Second,
		Timeout:     10 * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures > 3
		},
	})
	return &Flusher{
		buffer: buffer,
		store:  store,
		cfg:    cfg,
		logger: logger,
		cb:     cb,
	}
}

func (f *Flusher) Run(ctx context.Context) {
	ticker := time.NewTicker(f.cfg.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			f.logger.Info("Flusher shutting down, performing final flush...")
			f.flushAll(context.Background()) // Use background context as parent is canceled
			f.logger.Info("Flusher shutdown complete")
			return
		case <-ticker.C:
			f.flushAll(ctx)
		}
	}
}

func (f *Flusher) flushAll(ctx context.Context) {
	for namespace := range f.cfg.NamespaceQuotas {
		topics, err := f.store.GetActiveTopics(ctx, namespace)
		if err != nil {
			f.logger.Error("Failed to get active topics", zap.Error(err), zap.String("namespace", namespace))
			continue
		}
		for _, topic := range topics {
			if err := f.flush(ctx, namespace, topic); err != nil {
				f.logger.Error("Flush failed", zap.Error(err), zap.String("namespace", namespace), zap.String("topic", topic))
			}
		}
	}
}

func (f *Flusher) flush(ctx context.Context, namespace, topic string) error {
	items, err := f.buffer.GetItems(ctx, namespace, topic)
	if err != nil {
		f.logger.Error("Failed to get items from buffer", zap.Error(err))
		return fmt.Errorf("get items from buffer: %w", err)
	}
	if len(items) == 0 {
		return nil
	}

	ids, err := f.cb.Execute(func() (interface{}, error) {
		return f.store.UpsertItems(ctx, items)
	})
	if err != nil {
		f.logger.Error("Failed to upsert items", zap.Error(err))
		return fmt.Errorf("upsert items: %w", err)
	}

	// Collect item IDs for deletion
	itemIDs := make([]int64, len(items))
	for i, item := range items {
		itemIDs[i] = item.ID
	}
	if err := f.buffer.DeleteItems(ctx, namespace, topic, itemIDs); err != nil {
		f.logger.Error("Failed to delete items from buffer", zap.Error(err))
		return fmt.Errorf("delete items from buffer: %w", err)
	}

	f.logger.Info("Flushed items", zap.Int("count", len(ids.([]int64))))
	return nil
}
