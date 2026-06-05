package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"mqueue/internal/buffer"
	"mqueue/internal/config"
	"mqueue/internal/flusher"
	"mqueue/internal/log"
	"mqueue/internal/prefetch"
	"mqueue/internal/store"
	"mqueue/internal/wal"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"strings"
)

func main() {
	ctx := context.Background()
	logger := log.NewLogger()

	cfg, err := config.Load()
	if err != nil {
		logger.Fatal("Failed to load config", zap.Error(err))
	}

	// Configure for raw benchmark speed
	cfg.MaxBufferLength = 0 // Disable backpressure
	cfg.PrefetchBatchSize = 1000
	cfg.FlushBatchSize = 1000
	cfg.NamespaceQuotas["benchmark"] = 1000000

	// Initialize Redis
	fmt.Printf("Configured Redis Addrs: %v\n", cfg.RedisAddrs)
	var redisClients []redis.UniversalClient
	redisClients = make([]redis.UniversalClient, len(cfg.RedisAddrs))
	for i, addr := range cfg.RedisAddrs {
		var opt *redis.Options
		if strings.HasPrefix(addr, "redis://") || strings.HasPrefix(addr, "rediss://") {
			var err error
			opt, err = redis.ParseURL(addr)
			if err != nil {
				logger.Fatal("Failed to parse Redis URL", zap.Error(err), zap.String("addr", addr))
			}
		} else {
			opt = &redis.Options{
				Addr:     addr,
				Password: cfg.RedisPassword,
			}
		}
		redisClients[i] = redis.NewClient(opt)
	}

	// Initialize Store
	pgStore, err := store.NewPGStore(cfg.DatabaseURLs, redisClients, cfg)
	if err != nil {
		logger.Fatal("Failed to init pgStore", zap.Error(err))
	}
	defer func() {
		for _, db := range pgStore.GetDBs() {
			db.Close()
		}
		for _, rdb := range redisClients {
			rdb.Close()
		}
	}()

	// Flush Redis to start clean
	for _, rdb := range redisClients {
		rdb.FlushDB(ctx)
	}

	// Clean PG database tables
	for _, db := range pgStore.GetDBs() {
		_, _ = db.Exec("TRUNCATE TABLE items RESTART IDENTITY CASCADE;")
		_, _ = db.Exec("TRUNCATE TABLE dead_letter RESTART IDENTITY CASCADE;")
	}

	walDir, err := os.MkdirTemp("", "mqueue-bench-wal")
	if err != nil {
		logger.Fatal("Failed to create temp WAL dir", zap.Error(err))
	}
	defer os.RemoveAll(walDir)

	walManager, err := wal.NewWALManager(len(cfg.DatabaseURLs), walDir)
	if err != nil {
		logger.Fatal("Failed to init WAL", zap.Error(err))
	}
	defer walManager.Close()

	redisBuffer := buffer.NewRedisBuffer(redisClients, cfg, pgStore, logger)

	totalRequests := int64(10000)
	concurrency := int64(50)
	if envReq := os.Getenv("BENCH_REQUESTS"); envReq != "" {
		fmt.Sscanf(envReq, "%d", &totalRequests)
	}
	if envCon := os.Getenv("BENCH_CONCURRENCY"); envCon != "" {
		fmt.Sscanf(envCon, "%d", &concurrency)
	}

	cfg.PrefetchBatchSize = int(totalRequests)
	cfg.PrefetchInterval = 100 * time.Millisecond

	fmt.Printf("===================================================\n")
	fmt.Printf("          MQUEUE SYSTEM BENCHMARK UTILITY          \n")
	fmt.Printf("===================================================\n")
	fmt.Printf(" Concurrency Level: %d\n", concurrency)
	fmt.Printf(" Total Operations:  %d\n", totalRequests)
	fmt.Printf(" Database Shards:   %d\n", len(cfg.DatabaseURLs))
	fmt.Printf(" Redis Instances:   %d\n", len(cfg.RedisAddrs))
	fmt.Printf("===================================================\n\n")

	// 1. Benchmark Enqueue (Redis Write + WAL disk write)
	fmt.Printf("Starting Enqueue Benchmark...\n")
	enqueueStart := time.Now()
	var wg sync.WaitGroup
	var enqueueSuccess int64
	var enqueueErr int64

	requestsPerWorker := totalRequests / concurrency

	for i := int64(0); i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int64) {
			defer wg.Done()
			for j := int64(0); j < requestsPerWorker; j++ {
				item := store.Item{
					Namespace:    "benchmark",
					Topic:        "test",
					Priority:     int(rand.Int31n(10)),
					Payload:      []byte(fmt.Sprintf("benchmark-payload-%d-%d", workerID, j)),
					Status:       "ready",
					DeliverAfter: time.Now(),
					CreatedAt:    time.Now(),
					UpdatedAt:    time.Now(),
				}
				_, err := redisBuffer.Enqueue(ctx, []store.Item{item}, walManager)
				if err != nil {
					atomic.AddInt64(&enqueueErr, 1)
				} else {
					atomic.AddInt64(&enqueueSuccess, 1)
				}
			}
		}(i)
	}
	wg.Wait()
	enqueueDur := time.Since(enqueueStart)

	fmt.Printf("Enqueue Complete:\n")
	fmt.Printf("  Success:    %d\n", enqueueSuccess)
	fmt.Printf("  Errors:     %d\n", enqueueErr)
	fmt.Printf("  Duration:   %v\n", enqueueDur)
	fmt.Printf("  Throughput: %.2f ops/sec\n\n", float64(enqueueSuccess)/enqueueDur.Seconds())

	// 2. Benchmark Flush (Move from Redis Buffer to PostgreSQL)
	fmt.Printf("Starting Flush (Persistence) Benchmark...\n")
	flushStart := time.Now()
	flusherInstance := flusher.NewFlusher(redisBuffer, pgStore, cfg, logger)
	
	// Trigger flush cycle manually until buffer list is empty
	for {
		client := pgStore.GetRedisForShard("benchmark", "test")
		lenVal, err := client.LLen(ctx, "mqueue:buffer:benchmark:test").Result()
		if err != nil || lenVal == 0 {
			break
		}
		flusherInstance.FlushAll(ctx)
		time.Sleep(10 * time.Millisecond)
	}
	flushDur := time.Since(flushStart)

	fmt.Printf("Flush Complete:\n")
	fmt.Printf("  Duration:   %v\n", flushDur)
	fmt.Printf("  Throughput: %.2f ops/sec\n\n", float64(enqueueSuccess)/flushDur.Seconds())

	// 3. Benchmark Dequeue (Direct fast-path from Redis ready queue)
	fmt.Printf("Prefetching ready items to Redis...\n")
	prefetcher := prefetch.NewRedisPrefetcher(redisClients, pgStore, cfg, logger)
	
	// Start prefetcher run loop in background
	go prefetcher.Run(ctx)
	
	// Wake prefetcher once to populate Redis ready queue
	prefetcher.Trigger()
	// Wait brief moment for prefetch to load
	time.Sleep(1 * time.Second)

	fmt.Printf("Starting Dequeue Benchmark...\n")
	dequeueStart := time.Now()
	var dequeueSuccess int64
	var dequeueErr int64

	readyKey := "mqueue:queue:benchmark:test"
	client := pgStore.GetRedisForShard("benchmark", "test")

	for i := int64(0); i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				count := atomic.LoadInt64(&dequeueSuccess)
				if count >= totalRequests {
					break
				}
				// Dequeue items in batches
				redisData, err := client.LRange(ctx, readyKey, 0, 9).Result()
				if err != nil || len(redisData) == 0 {
					time.Sleep(10 * time.Millisecond)
					continue
				}
				client.LTrim(ctx, readyKey, int64(len(redisData)), -1)
				
				for _, data := range redisData {
					var item store.Item
					if json.Unmarshal([]byte(data), &item) == nil {
						atomic.AddInt64(&dequeueSuccess, 1)
					} else {
						atomic.AddInt64(&dequeueErr, 1)
					}
				}
			}
		}()
	}
	wg.Wait()
	dequeueDur := time.Since(dequeueStart)

	fmt.Printf("Dequeue Complete:\n")
	fmt.Printf("  Success:    %d\n", dequeueSuccess)
	fmt.Printf("  Errors:     %d\n", dequeueErr)
	fmt.Printf("  Duration:   %v\n", dequeueDur)
	fmt.Printf("  Throughput: %.2f ops/sec\n\n", float64(dequeueSuccess)/dequeueDur.Seconds())
	fmt.Printf("===================================================\n")
}
