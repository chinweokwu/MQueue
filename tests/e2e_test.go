//go:build integration
// +build integration

package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"mqueue/internal/buffer"
	"mqueue/internal/config"
	"mqueue/internal/flusher"
	"mqueue/internal/log"
	"mqueue/internal/metrics"
	"mqueue/internal/prefetch"
	"mqueue/internal/retry"
	"mqueue/internal/server"
	"mqueue/internal/store"
	"mqueue/internal/wal"

	"github.com/go-chi/chi/v5"
	"github.com/golang-jwt/jwt/v4"
	"github.com/redis/go-redis/v9"

)

// Helper to generate a valid JWT for testing
func generateTestToken(secret, sub string) string {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": sub,
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	tokenString, _ := token.SignedString([]byte(secret))
	return tokenString
}



func TestE2E_HTTP_Flow(t *testing.T) {
	ctx := context.Background()

	// 1. Setup Infrastructure
	dbURL, cleanupDB, err := setupTestDB(ctx)
	if err != nil {
		t.Fatalf("setup db failed: %s", err)
	}
	defer cleanupDB()

	redisAddr, cleanupRedis, err := setupTestRedis(ctx)
	if err != nil {
		t.Fatalf("setup redis failed: %s", err)
	}
	defer cleanupRedis()

	// 2. Setup Application Components
	// -------------------------------------------------------------------------
	logger := log.NewLogger()
	walDir := t.TempDir()

	cfg := &config.Config{
		DatabaseURLs:      []string{dbURL},
		RedisAddrs:        []string{redisAddr},
		NamespaceQuotas:   map[string]int{"default": 1000, "e2e": 1000},
		MaxRetries:        2,
		WorkerID:          "e2e-worker",
		LeaseTTL:          5 * time.Second,
		WorkerBatchSize:   10,
		PrefetchBatchSize: 10,
		FlushInterval:     100 * time.Millisecond,
		PrefetchInterval:  100 * time.Millisecond,
		BufferTTL:         1 * time.Minute,
		WALDir:            walDir,
		JWTSecret:         "super-secret-test-key",
	}

	// Clients
	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer redisClient.Close()

	// Store
	pgStore, err := store.NewPGStore(cfg.DatabaseURLs, []*redis.Client{redisClient}, cfg)
	if err != nil {
		t.Fatalf("failed to init pgStore: %v", err)
	}
	dlqStore, err := store.NewDLQStore(cfg.DatabaseURLs, cfg)
	if err != nil {
		t.Fatalf("failed to init dlqStore: %v", err)
	}

	// Schema Init (Manual for tests)
	time.Sleep(5 * time.Second) // Wait for DB to be fully ready
	db := pgStore.GetDBs()[0]
	_, err = db.Exec(`
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
	`)
	if err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	// Components
	walManager, _ := wal.NewWALManager(1, walDir)
	defer walManager.Close()

	metrics := metrics.NewQueueMetrics(pgStore, cfg, logger)
	redisBuffer := buffer.NewRedisBuffer([]*redis.Client{redisClient}, cfg, pgStore, logger)
	retryManager := retry.NewRetryManager(pgStore, dlqStore, cfg, logger)

	// Start Daemons (Prefetcher is critical for Dequeue to work from Redis)
	prefetcher := prefetch.NewRedisPrefetcher([]*redis.Client{redisClient}, pgStore, cfg, logger)
	flusher := flusher.NewFlusher(redisBuffer, pgStore, cfg, logger)

	ctxDaemons, cancelDaemons := context.WithCancel(ctx)
	defer cancelDaemons()
	go prefetcher.Run(ctxDaemons)
	go flusher.Run(ctxDaemons)

	// 3. Start HTTP Server
	// -------------------------------------------------------------------------
	r := chi.NewRouter()
	server.SetupRouter(r, cfg, pgStore, dlqStore, redisBuffer, retryManager, metrics, walManager, prefetcher)

	ts := httptest.NewServer(r)
	defer ts.Close()

	client := ts.Client()
	baseURL := ts.URL
	userToken := generateTestToken(cfg.JWTSecret, "test-user")
	authHeader := "Bearer " + userToken

	// 4. Execute Test Scenarios
	// -------------------------------------------------------------------------

	t.Run("HealthCheck", func(t *testing.T) {
		resp, err := client.Get(baseURL + "/health")
		if err != nil {
			t.Fatalf("health check failed: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200 OK, got %d", resp.StatusCode)
		}
	})

	t.Run("AuthMiddleware", func(t *testing.T) {
		// No Token
		resp, err := client.Post(baseURL+"/enqueue", "application/json", nil)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("expected 401 for missing token, got %d", resp.StatusCode)
		}
		resp.Body.Close()

		// Invalid Token
		req, _ := http.NewRequest("POST", baseURL+"/enqueue", nil)
		req.Header.Set("Authorization", "Bearer invalid-token")
		resp, err = client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("expected 401 for invalid token, got %d", resp.StatusCode)
		}
		resp.Body.Close()
	})

	t.Run("EnqueueAndDequeue", func(t *testing.T) {
		// Enqueue
		payload := []map[string]interface{}{
			{
				"namespace":     "e2e",
				"topic":         "test-topic",
				"priority":      1,
				"payload":       []byte(`"hello world"`), // JSON string encoded as bytes
				"deliver_after": time.Now(),
			},
		}
		body, _ := json.Marshal(payload)
		req, _ := http.NewRequest("POST", baseURL+"/enqueue", bytes.NewReader(body))
		req.Header.Set("Authorization", authHeader)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("enqueue request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			t.Fatalf("enqueue failed with %d: %s", resp.StatusCode, string(bodyBytes))
		}

		// Wait for prefetcher to move item to Redis (if strictly testing redis path)
		// Or pg fallback might work.
		time.Sleep(5 * time.Second)

		// Dequeue
		req, _ = http.NewRequest("GET", baseURL+"/dequeue?namespace=e2e&topic=test-topic&limit=1", nil)
		req.Header.Set("Authorization", authHeader)

		resp, err = client.Do(req)
		if err != nil {
			t.Fatalf("dequeue request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("dequeue failed with %d", resp.StatusCode)
		}

		var items []store.Item
		if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
			t.Fatalf("failed to decode dequeue response: %v", err)
		}

		if len(items) != 1 {
			t.Fatalf("expected 1 item, got %d", len(items))
		}

		// Ack
		ackPayload := map[string]interface{}{
			"id":        items[0].ID,
			"namespace": items[0].Namespace,
			"topic":     items[0].Topic,
		}
		body, _ = json.Marshal(ackPayload)
		req, _ = http.NewRequest("POST", baseURL+"/ack", bytes.NewReader(body))
		req.Header.Set("Authorization", authHeader)
		req.Header.Set("Content-Type", "application/json")

		resp, err = client.Do(req)
		if err != nil {
			t.Fatalf("ack request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("ack failed with %d", resp.StatusCode)
		}
	})

	t.Run("DLQ_Flow", func(t *testing.T) {
		// Enqueue Item designed to fail
		payload := []map[string]interface{}{
			{
				"namespace":     "e2e",
				"topic":         "dlq-topic",
				"priority":      1,
				"payload":       []byte(`"fail-me"`),
				"deliver_after": time.Now(),
			},
		}
		body, _ := json.Marshal(payload)
		req, _ := http.NewRequest("POST", baseURL+"/enqueue", bytes.NewReader(body))
		req.Header.Set("Authorization", authHeader)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil || resp.StatusCode != 200 {
			t.Fatalf("setup enqueue failed")
		}
		resp.Body.Close()

		time.Sleep(1 * time.Second)

		// Nack Loop (MaxRetries = 2)
		// Try 1:
		itemID := leaseAndNack(t, client, baseURL, authHeader, "dlq-topic")
		// Try 2:
		leaseAndNack(t, client, baseURL, authHeader, "dlq-topic")
		// Try 3 (Should exceed max retries and move to DLQ):
		leaseAndNack(t, client, baseURL, authHeader, "dlq-topic")

		// Verify in DLQ
		req, _ = http.NewRequest("GET", baseURL+"/dlq?namespace=e2e&topic=dlq-topic", nil)
		req.Header.Set("Authorization", authHeader)
		resp, err = client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		var dlqItems []store.DeadLetter
		json.NewDecoder(resp.Body).Decode(&dlqItems)

		found := false
		for _, item := range dlqItems {
			if item.OriginalID == itemID {
				found = true
				break
			}
		}
		if !found {
			t.Error("Item not found in DLQ after max retries")
		}
	})
}

func leaseAndNack(t *testing.T, client *http.Client, baseURL, authHeader, topic string) int64 {
	// Dequeue
	req, _ := http.NewRequest("GET", baseURL+"/dequeue?namespace=e2e&topic="+topic+"&limit=1", nil)
	req.Header.Set("Authorization", authHeader)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("dequeue failed in leaseAndNack loop: %d", resp.StatusCode)
	}

	var items []store.Item
	json.NewDecoder(resp.Body).Decode(&items)
	if len(items) == 0 {
		t.Fatal("expected item to nack, got none")
	}

	// Nack
	nackPayload := map[string]interface{}{
		"id":        items[0].ID,
		"namespace": items[0].Namespace,
		"topic":     items[0].Topic,
		"error":     "simulated processing failure",
	}
	body, _ := json.Marshal(nackPayload)
	req, _ = http.NewRequest("POST", baseURL+"/nack", bytes.NewReader(body))
	req.Header.Set("Authorization", authHeader)
	req.Header.Set("Content-Type", "application/json")

	resp, err = client.Do(req)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("nack failed: %v", err)
	}
	resp.Body.Close()

	return items[0].ID
}
