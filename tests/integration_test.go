//go:build integration
// +build integration

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
	"database/sql"
	_ "github.com/lib/pq"

	"mqueue/internal/buffer"
	"mqueue/internal/config"
	"mqueue/internal/flusher"
	"mqueue/internal/log"
	"mqueue/internal/prefetch"
	"mqueue/internal/retry"
	"mqueue/internal/store"
	"mqueue/internal/wal"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	tcRedis "github.com/testcontainers/testcontainers-go/modules/redis"
)

func setupTestDB(ctx context.Context) (string, func(), error) {
	if url := os.Getenv("TEST_DB_URL"); url != "" {
		return url, func() {}, nil
	}
	pgContainer, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:15"),
		postgres.WithDatabase("mqueue"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("securepassword"),
	)
	if err != nil {
		return "", nil, fmt.Errorf("failed to start postgres container: %w", err)
	}

	dbURL, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return "", nil, fmt.Errorf("failed to get connection string for postgres: %w", err)
	}

	cleanup := func() {
		pgContainer.Terminate(ctx)
	}

	return dbURL, cleanup, nil
}

func setupTestRedis(ctx context.Context) (string, func(), error) {
	if addr := os.Getenv("TEST_REDIS_ADDR"); addr != "" {
		return addr, func() {}, nil
	}
	redisContainer, err := tcRedis.RunContainer(ctx, testcontainers.WithImage("redis:7"))
	if err != nil {
		return "", nil, fmt.Errorf("failed to start redis container: %w", err)
	}

	redisAddr, err := redisContainer.Endpoint(ctx, "")
	if err != nil {
		return "", nil, fmt.Errorf("failed to get redis endpoint: %w", err)
	}

	cleanup := func() {
		redisContainer.Terminate(ctx)
	}

	return redisAddr, cleanup, nil
}

func TestStoreIntegration(t *testing.T) {
	ctx := context.Background()

	// Setup DB (Sharded 1 & 2 reuse same DB for Docker test)
	dbURL, cleanupDB, err := setupTestDB(ctx)
	if err != nil {
		t.Fatalf("setup db failed: %s", err)
	}
	// Clean DB before starting (in case previous run didn't clean up)
	db, _ := sql.Open("postgres", dbURL)
	db.Exec("TRUNCATE TABLE items, dead_letter")
	db.Close()
	defer cleanupDB()
	dbURL1 := dbURL
	dbURL2 := dbURL

	// Setup Redis (Sharded 1 & 2 reuse same Redis for Docker test)
	redisAddr, cleanupRedis, err := setupTestRedis(ctx)
	if err != nil {
		t.Fatalf("setup redis failed: %s", err)
	}
	defer cleanupRedis()
	redisAddr1 := redisAddr
	redisAddr2 := redisAddr

	redisClient1 := redis.NewClient(&redis.Options{Addr: redisAddr1})
	redisClient2 := redis.NewClient(&redis.Options{Addr: redisAddr2})

	// Create WAL directories
	walDir := t.TempDir() // Use temp dir to avoid cleanup issues
	cfg := &config.Config{
		DatabaseURLs:      []string{dbURL1, dbURL2},
		RedisAddrs:        []string{redisAddr1, redisAddr2},
		NamespaceQuotas:   map[string]int{"default": 1000},
		MaxRetries:        2,
		WorkerID:          "test-worker",
		LeaseTTL:          5 * time.Second, // Short for testing timeout
		WorkerBatchSize:   10,
		PrefetchBatchSize: 10,
		FlushInterval:     1 * time.Second,
		PrefetchInterval:  1 * time.Second,
		BufferTTL:         1 * time.Minute,
		WALDir:            walDir,
	}

	pgStore, err := store.NewPGStore(cfg.DatabaseURLs, []*redis.Client{redisClient1, redisClient2}, cfg)
	if err != nil {
		t.Fatalf("failed to initialize store: %s", err)
	}
	defer func() {
		for _, db := range pgStore.GetDBs() {
			db.Close()
		}
		redisClient1.Close()
		redisClient2.Close()
	}()

	dlqStore, err := store.NewDLQStore(cfg.DatabaseURLs, cfg)
	if err != nil {
		t.Fatalf("failed to initialize DLQ store: %s", err)
	}

	walManager, err := wal.NewWALManager(len(cfg.DatabaseURLs), cfg.WALDir)
	if err != nil {
		t.Fatalf("failed to initialize WAL: %s", err)
	}
	defer walManager.Close()

	logger := log.NewLogger()
	redisBuffer := buffer.NewRedisBuffer([]*redis.Client{redisClient1, redisClient2}, cfg, pgStore, logger)
	_ = retry.NewRetryManager(pgStore, dlqStore, cfg, logger) // Initialize but ignore if unused in specific tests
	prefetcher := prefetch.NewRedisPrefetcher([]*redis.Client{redisClient1, redisClient2}, pgStore, cfg, logger)

	// Start prefetcher in background AFTER schema is ready
	// go prefetcher.Run(ctx) <- Moved down

	// Initialize schema
	time.Sleep(5 * time.Second) // Wait for DB to be fully ready
	for _, db := range pgStore.GetDBs() {
		_, err := db.Exec(`
			CREATE TABLE IF NOT EXISTS items (
				id BIGSERIAL PRIMARY KEY,
				namespace VARCHAR(255) NOT NULL,
				topic VARCHAR(255) NOT NULL,
				idempotency_key VARCHAR(255),
				priority INTEGER NOT NULL,
				payload BYTEA NOT NULL,
				metadata JSONB,
				deliver_after TIMESTAMP WITH TIME ZONE NOT NULL,
				lease_expires_at TIMESTAMP WITH TIME ZONE,
				lease_owner VARCHAR(255),
				ttl TIMESTAMP WITH TIME ZONE,
				status VARCHAR(50) NOT NULL,
				retries INTEGER NOT NULL,
				created_at TIMESTAMP WITH TIME ZONE NOT NULL,
				updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
				first_failed_at TIMESTAMP WITH TIME ZONE,
				last_error TEXT,
				CONSTRAINT uniq_idempotency UNIQUE (namespace, idempotency_key)
			);
			CREATE TABLE IF NOT EXISTS dead_letter (
				id BIGSERIAL PRIMARY KEY,
				original_id BIGINT NOT NULL,
				namespace VARCHAR(255) NOT NULL,
				topic VARCHAR(255) NOT NULL,
				idempotency_key VARCHAR(255),
				priority INTEGER NOT NULL,
				payload BYTEA NOT NULL,
				metadata JSONB,
				last_error TEXT,
				retries INTEGER NOT NULL,
				first_failed_at TIMESTAMP WITH TIME ZONE,
				moved_at TIMESTAMP WITH TIME ZONE NOT NULL
			);
			CREATE INDEX IF NOT EXISTS idx_items_ready ON items (namespace, topic, priority, deliver_after) WHERE status = 'ready';
			CREATE INDEX IF NOT EXISTS idx_items_namespace_topic ON items (namespace, topic);
			CREATE INDEX IF NOT EXISTS idx_dead_letter_namespace_topic ON dead_letter (namespace, topic);
		`)
		if err != nil {
			t.Fatalf("failed to initialize schema: %s", err)
		}

	flusher := flusher.NewFlusher(redisBuffer, pgStore, cfg, logger)
	go flusher.Run(ctx)
	// Prefetcher started later for relevant tests
	}

	// Your original tests (keep them — they are excellent)
	// ... (EnqueueAndDequeue, AckItem, MoveToDLQ, RetryLogic, MultiTopic, ShardFailover, NamespaceQuota, Flush)

	// === NEW CRITICAL TESTS ===

	t.Run("PriorityOrdering", func(t *testing.T) {
		now := time.Now().Add(-1 * time.Minute)
		items := []store.Item{
			{Namespace: "default", Topic: "prio", Priority: 10, Payload: []byte("low"), DeliverAfter: now, Status: "ready", CreatedAt: now, UpdatedAt: now},
			{Namespace: "default", Topic: "prio", Priority: 1, Payload: []byte("high"), DeliverAfter: now, Status: "ready", CreatedAt: now, UpdatedAt: now},
			{Namespace: "default", Topic: "prio", Priority: 5, Payload: []byte("med"), DeliverAfter: now, Status: "ready", CreatedAt: now, UpdatedAt: now},
		}
		_, err := redisBuffer.Enqueue(ctx, items, walManager)
		if err != nil {
			t.Fatalf("enqueue failed: %s", err)
		}
		time.Sleep(3 * time.Second) // wait for flush + prefetch

		dequeued, err := pgStore.LeaseItems(ctx, "default", "prio", "worker", 3, 30*time.Second)
		if err != nil {
			t.Fatalf("dequeue failed: %s", err)
		}
		if len(dequeued) != 3 {
			t.Fatalf("expected 3 items, got %d", len(dequeued))
		}
		if string(dequeued[0].Payload) != "high" {
			t.Errorf("highest priority not dequeued first: got %s (prio %d)", string(dequeued[0].Payload), dequeued[0].Priority)
			for i, item := range dequeued {
				t.Logf("Item %d: Prio %d Payload %s", i, item.Priority, string(item.Payload))
			}
		}
		if string(dequeued[1].Payload) != "med" {
			t.Errorf("medium priority not second: got %s", string(dequeued[1].Payload))
		}
		if string(dequeued[2].Payload) != "low" {
			t.Errorf("low priority not last: got %s", string(dequeued[2].Payload))
		}
	})

	t.Run("DelayedDelivery", func(t *testing.T) {
		delayed := store.Item{
			Namespace:    "default",
			Topic:        "delay",
			Payload:      []byte("delayed"),
			DeliverAfter: time.Now().Add(3 * time.Second),
			Status:       "ready",
			CreatedAt:    time.Now(),
			UpdatedAt:    time.Now(),
		}
		_, err := redisBuffer.Enqueue(ctx, []store.Item{delayed}, walManager)
		if err != nil {
			t.Fatalf("enqueue failed: %s", err)
		}

		// Immediate dequeue
		items, _ := pgStore.LeaseItems(ctx, "default", "delay", "worker", 1, 30*time.Second)
		if len(items) > 0 {
			t.Error("delayed item was leasable too early")
		}

		time.Sleep(4 * time.Second)
		items, err = pgStore.LeaseItems(ctx, "default", "delay", "worker", 1, 30*time.Second)
		if err != nil {
			t.Fatalf("dequeue failed after delay: %s", err)
		}
		if len(items) == 0 {
			t.Error("delayed item never became available")
		}
	})

	t.Run("LeaseTimeoutRedelivery", func(t *testing.T) {
		item := store.Item{
			Namespace:    "default",
			Topic:        "timeout",
			Payload:      []byte("redeliver"),
			DeliverAfter: time.Now().Add(-1 * time.Minute),
			Status:       "ready",
			CreatedAt:    time.Now(),
			UpdatedAt:    time.Now(),
		}
		ids, err := redisBuffer.Enqueue(ctx, []store.Item{item}, walManager)
		if err != nil {
			t.Fatalf("enqueue failed: %s", err)
		}
		time.Sleep(2 * time.Second) // Wait for flush

		// Lease but don't ack
		items, err := pgStore.LeaseItems(ctx, "default", "timeout", "worker1", 1, 3*time.Second)
		if err != nil || len(items) == 0 {
			t.Fatal("failed to lease item")
		}

		// Wait for lease to expire
		time.Sleep(5 * time.Second)

		// Lease again with different worker
		items2, err := pgStore.LeaseItems(ctx, "default", "timeout", "worker2", 1, 30*time.Second)
		if err != nil || len(items2) == 0 {
			t.Fatal("item not redelivered after lease timeout")
		}
		if items2[0].ID != ids[0] {
			t.Error("redelivered item has different ID")
		}
	})

	t.Run("ConcurrentWorkers", func(t *testing.T) {
		// Enqueue 5 items
		items := make([]store.Item, 5)
		now := time.Now().Add(-1 * time.Minute)
		for i := range items {
			items[i] = store.Item{
				Namespace:    "default",
				Topic:        "concurrent",
				Payload:      []byte(fmt.Sprintf("item-%d", i)),
				Priority:     1,
				DeliverAfter: now,
				Status:       "ready",
				CreatedAt:    now,
				UpdatedAt:    now,
			}
		}
		redisBuffer.Enqueue(ctx, items, walManager)
		time.Sleep(3 * time.Second)

		var wg sync.WaitGroup
		leased := make(map[int64]bool)
		var mu sync.Mutex

		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func(worker string) {
				defer wg.Done()
				dequeued, _ := pgStore.LeaseItems(ctx, "default", "concurrent", worker, 5, 30*time.Second)
				mu.Lock()
				for _, it := range dequeued {
					if leased[it.ID] {
						t.Errorf("duplicate lease of item %d", it.ID)
					}
					leased[it.ID] = true
				}
				mu.Unlock()
			}(fmt.Sprintf("worker-%d", i))
		}
		wg.Wait()

		if len(leased) != 5 {
			t.Errorf("expected 5 unique leases, got %d", len(leased))
		}
	})

	// Start Prefetcher for Redis-integrated tests
	go prefetcher.Run(ctx)
	time.Sleep(2 * time.Second) // Allow prefetcher to initialize

	t.Run("RedisReadyQueueServing", func(t *testing.T) {
		now := time.Now().Add(-1 * time.Minute)
		items := []store.Item{
			{Namespace: "default", Topic: "redisq", Priority: 1, Payload: []byte("fast"), DeliverAfter: now, Status: "ready", CreatedAt: now, UpdatedAt: now},
			{Namespace: "default", Topic: "redisq", Priority: 2, Payload: []byte("slow"), DeliverAfter: now, Status: "ready", CreatedAt: now, UpdatedAt: now},
		}
		_, err := redisBuffer.Enqueue(ctx, items, walManager)
		if err != nil {
			t.Fatalf("enqueue failed: %s", err)
		}
		time.Sleep(4 * time.Second) // wait for prefetcher

		client := pgStore.GetRedisForShard("default", "redisq")
		readyKey := "mqueue:queue:default:redisq"
		length, _ := client.LLen(ctx, readyKey).Result()
		if length == 0 {
			t.Error("prefetcher did not populate Redis ready queue")
		}

		// Simulate dequeue handler logic
		data, _ := client.LRange(ctx, readyKey, 0, 1).Result()
		if len(data) > 0 {
			var item store.Item
			json.Unmarshal([]byte(data[0]), &item)
			if string(item.Payload) != "fast" {
				t.Error("priority order not preserved in Redis queue")
			}
		}
	})



	t.Run("IdempotencyKey", func(t *testing.T) {
		key := "unique-123"
		item1 := store.Item{
			Namespace:      "default",
			Topic:          "idemp",
			IdempotencyKey: &key,
			Priority:       10,
			Payload:        []byte("v1"),
			DeliverAfter:   time.Now().Add(-1 * time.Minute),
			Status:         "ready",
			CreatedAt:      time.Now(),
			UpdatedAt:      time.Now(),
		}
		item2 := item1
		item2.Payload = []byte("v2")

		ids1, _ := redisBuffer.Enqueue(ctx, []store.Item{item1}, walManager)
		time.Sleep(5 * time.Second) // Increased wait
		ids2, _ := redisBuffer.Enqueue(ctx, []store.Item{item2}, walManager)
		time.Sleep(5 * time.Second) // Wait for update flush

		if ids1[0] != ids2[0] {
			t.Error("idempotency key did not prevent duplicate")
		}

		item, _ := pgStore.GetItem(ctx, "default", "idemp", ids1[0])
		if string(item.Payload) != "v2" {
			t.Error("item not updated on conflict")
		}
	})



	t.Run("WALRecovery", func(t *testing.T) {
		// Enqueue items
		items := []store.Item{
			{Namespace: "default", Topic: "recover", Payload: []byte("survive-crash"), DeliverAfter: time.Now(), Status: "ready", CreatedAt: time.Now(), UpdatedAt: time.Now()},
		}
		redisBuffer.Enqueue(ctx, items, walManager)

		// Simulate crash: close store without flush
		for _, db := range pgStore.GetDBs() {
			db.Close()
		}

		// Re-create store with same WAL
		newStore, err := store.NewPGStore(cfg.DatabaseURLs, []*redis.Client{redisClient1, redisClient2}, cfg)
		if err != nil {
			t.Fatalf("failed to recreate store: %s", err)
		}
		defer func() {
			for _, db := range newStore.GetDBs() {
				db.Close()
			}
		}()

		if err := newStore.Recover(ctx, walManager); err != nil {
			t.Fatalf("recovery failed: %s", err)
		}

		// Item should now be in DB
		recovered, err := newStore.LeaseItems(ctx, "default", "recover", "recovery-worker", 1, 30*time.Second)
		if err != nil || len(recovered) == 0 {
			t.Fatal("item not recovered from WAL")
		}
		if string(recovered[0].Payload) != "survive-crash" {
			t.Error("recovered item has wrong payload")
		}
	})
}
