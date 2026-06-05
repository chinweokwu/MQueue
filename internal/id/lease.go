package id

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type LeaseManager struct {
	rdb         redis.UniversalClient
	workerID    string
	leaseTTL    time.Duration
	renewPeriod time.Duration
	nodeID      int64
	cancel      context.CancelFunc
}

func NewLeaseManager(rdb redis.UniversalClient, workerID string) *LeaseManager {
	return &LeaseManager{
		rdb:         rdb,
		workerID:    workerID,
		leaseTTL:    10 * time.Second,
		renewPeriod: 3 * time.Second,
		nodeID:      -1,
	}
}

func (lm *LeaseManager) Acquire(ctx context.Context) (int64, error) {
	for i := int64(0); i <= nodeMax; i++ {
		key := fmt.Sprintf("mqueue:node_lease:%d", i)
		ok, err := lm.rdb.SetNX(ctx, key, lm.workerID, lm.leaseTTL).Result()
		if err != nil {
			return -1, fmt.Errorf("redis error during lease check: %w", err)
		}
		if ok {
			lm.nodeID = i
			return i, nil
		}
	}
	return -1, fmt.Errorf("no available node IDs (0-%d) found in Redis", nodeMax)
}

func (lm *LeaseManager) StartRenewalLoop(ctx context.Context, onLeaseLost func()) {
	renewCtx, cancel := context.WithCancel(ctx)
	lm.cancel = cancel

	go func() {
		ticker := time.NewTicker(lm.renewPeriod)
		defer ticker.Stop()

		script := `
			if redis.call("get", KEYS[1]) == ARGV[1] then
				return redis.call("pexpire", KEYS[1], ARGV[2])
			else
				return 0
			end
		`

		for {
			select {
			case <-renewCtx.Done():
				// Release lease cleanly on shutdown
				releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 2*time.Second)
				key := fmt.Sprintf("mqueue:node_lease:%d", lm.nodeID)
				lm.rdb.Del(releaseCtx, key)
				releaseCancel()
				return
			case <-ticker.C:
				key := fmt.Sprintf("mqueue:node_lease:%d", lm.nodeID)
				ttlMs := int64(lm.leaseTTL / time.Millisecond)
				res, err := lm.rdb.Eval(renewCtx, script, []string{key}, lm.workerID, ttlMs).Result()
				if err != nil || res.(int64) == 0 {
					onLeaseLost()
					return
				}
			}
		}
	}()
}

func (lm *LeaseManager) Close() {
	if lm.cancel != nil {
		lm.cancel()
	}
}
