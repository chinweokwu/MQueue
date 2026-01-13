package prefetch

import (
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"mqueue/internal/config"
	"mqueue/internal/log"
	"mqueue/internal/store"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"golang.org/x/exp/constraints"
)

// RedisPrefetcher manages prefetching items from PostgreSQL and caching them in Redis.
type RedisPrefetcher struct {
	clients     []*redis.Client
	pgStore     *store.PGStore
	cfg         *config.Config
	logger      *log.Logger
	demand      map[string]int
	demandMu    sync.RWMutex
	leaseChan   chan store.Item
	triggerChan chan struct{} // Signal to wake up prefetcher immediately
}

// NewRedisPrefetcher initializes a new RedisPrefetcher.
func NewRedisPrefetcher(clients []*redis.Client, pgStore *store.PGStore, cfg *config.Config, logger *log.Logger) *RedisPrefetcher {
	return &RedisPrefetcher{
		clients:     clients,
		pgStore:     pgStore,
		cfg:         cfg,
		logger:      logger,
		demand:      make(map[string]int),
		leaseChan:   make(chan store.Item, cfg.PrefetchBatchSize),
		triggerChan: make(chan struct{}, 1),
	}
}

// Run starts the prefetch and merge routines.
func (p *RedisPrefetcher) Run(ctx context.Context) {
	go p.prefetch(ctx)
	go p.merge(ctx)
	<-ctx.Done()
	p.logger.Info("Prefetcher shutting down")
}

// Trigger wakes up the prefetcher immediately.
// It is non-blocking; if already triggered, it does nothing.
func (p *RedisPrefetcher) Trigger() {
	select {
	case p.triggerChan <- struct{}{}:
	default:
	}
}

// UpdateDemand updates the demand rate for a specific topic.
func (p *RedisPrefetcher) UpdateDemand(topic string, rate int) {
	p.demandMu.Lock()
	defer p.demandMu.Unlock()
	p.demand[topic] = rate
}

// prefetch leases items from PostgreSQL and stores them temporarily in Redis.
func (p *RedisPrefetcher) prefetch(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.PrefetchInterval)
	defer ticker.Stop()

	runPrefetch := func() {
		for namespace := range p.cfg.NamespaceQuotas {
			topics, err := p.pgStore.GetActiveTopics(ctx, namespace)
			if err != nil {
				p.logger.Error("Failed to get active topics", zap.Error(err), zap.String("namespace", namespace))
				continue
			}
			for _, topic := range topics {
				fullKey := namespace + ":" + topic
				p.demandMu.RLock()
				batchSize := p.demand[fullKey]
				if batchSize == 0 {
					batchSize = p.cfg.PrefetchBatchSize
				}
				p.demandMu.RUnlock()

				items, err := p.pgStore.LeaseItems(ctx, namespace, topic, p.cfg.WorkerID, batchSize, p.cfg.LeaseTTL)
				if err != nil {
					p.logger.Error("Failed to lease items", zap.Error(err))
					continue
				}

				client := p.pgStore.GetRedisForShard(namespace, topic)
				for _, item := range items {
					data, err := json.Marshal(item)
					if err != nil {
						p.logger.Error("Failed to marshal item", zap.Error(err))
						continue
					}

					key := fmt.Sprintf("mqueue:lease:%d", item.ID)
					if err := client.Set(ctx, key, data, p.cfg.LeaseTTL).Err(); err != nil {
						p.logger.Error("Failed to set lease in Redis", zap.Error(err))
						continue
					}
					if err := client.SAdd(ctx, fmt.Sprintf("mqueue:leases:%s:%s", namespace, topic), key).Err(); err != nil {
						p.logger.Error("Failed to add lease to set", zap.Error(err))
						continue
					}
					p.leaseChan <- item
				}
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runPrefetch()
		case <-p.triggerChan:
			// reset ticker to prevent immediate double-run
			ticker.Reset(p.cfg.PrefetchInterval)
			runPrefetch()
		}
	}
}

// merge consumes leased items and merges them back into the processing queue in Redis.
func (p *RedisPrefetcher) merge(ctx context.Context) {
	h := &itemHeap{}
	heap.Init(h)

	for {
		select {
		case <-ctx.Done():
			return
		case item := <-p.leaseChan:
			heap.Push(h, item)
		default:
			if h.Len() == 0 {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			item := heap.Pop(h).(store.Item)
			client := p.pgStore.GetRedisForShard(item.Namespace, item.Topic)

			data, err := json.Marshal(item)
			if err != nil {
				p.logger.Error("Failed to marshal merged item", zap.Error(err))
				continue
			}

			if err := client.RPush(ctx, fmt.Sprintf("mqueue:queue:%s:%s", item.Namespace, item.Topic), data).Err(); err != nil {
				p.logger.Error("Failed to push to queue", zap.Error(err))
				continue
			}
		}
	}
}

// itemHeap implements a priority heap for store.Item.
type itemHeap []store.Item

func (h itemHeap) Len() int { return len(h) }

func (h itemHeap) Less(i, j int) bool {
	if h[i].Priority != h[j].Priority {
		return h[i].Priority > h[j].Priority
	}
	return h[i].DeliverAfter.Before(h[j].DeliverAfter)
}

func (h itemHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *itemHeap) Push(x interface{}) { *h = append(*h, x.(store.Item)) }
func (h *itemHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// min returns the smaller of two ordered values.
func min[T constraints.Ordered](a, b T) T {
	if a < b {
		return a
	}
	return b
}
