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
func generateTestToken(secret, sub string, allowedNamespaces []string) string {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": sub,
		"exp": time.Now().Add(time.Hour).Unix(),
		"allowed_namespaces": allowedNamespaces,
	})
	tokenString, _ := token.SignedString([]byte(secret))
	return tokenString
}

func generateScopedTestToken(secret, sub string, scopes []string) string {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": sub,
		"exp": time.Now().Add(time.Hour).Unix(),
		"scopes": scopes,
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
		MaxRetries:        3,
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
	redisClient.FlushDB(ctx)

	// Store
	pgStore, err := store.NewPGStore(cfg.DatabaseURLs, []redis.UniversalClient{redisClient}, cfg)
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
	redisBuffer := buffer.NewRedisBuffer([]redis.UniversalClient{redisClient}, cfg, pgStore, logger)
	retryManager := retry.NewRetryManager(pgStore, dlqStore, cfg, logger)

	// Start Daemons (Prefetcher is critical for Dequeue to work from Redis)
	prefetcher := prefetch.NewRedisPrefetcher([]redis.UniversalClient{redisClient}, pgStore, cfg, logger)
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
	userToken := generateTestToken(cfg.JWTSecret, "test-user", []string{"*"})
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

	t.Run("NamespaceAuthorization", func(t *testing.T) {
		restrictedToken := generateTestToken(cfg.JWTSecret, "tenant-user", []string{"tenant-a"})
		restrictedAuthHeader := "Bearer " + restrictedToken

		req, _ := http.NewRequest("GET", baseURL+"/dequeue?namespace=tenant-a&topic=test-topic&limit=1", nil)
		req.Header.Set("Authorization", restrictedAuthHeader)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusForbidden {
			t.Errorf("expected access to tenant-a to be allowed, got 403")
		}

		reqForbidden, _ := http.NewRequest("GET", baseURL+"/dequeue?namespace=e2e&topic=test-topic&limit=1", nil)
		reqForbidden.Header.Set("Authorization", restrictedAuthHeader)
		respForbidden, err := client.Do(reqForbidden)
		if err != nil {
			t.Fatal(err)
		}
		defer respForbidden.Body.Close()
		if respForbidden.StatusCode != http.StatusForbidden {
			t.Errorf("expected access to e2e namespace to be forbidden, got %d", respForbidden.StatusCode)
		}
	})

	t.Run("GranularScopes", func(t *testing.T) {
		readToken := generateScopedTestToken(cfg.JWTSecret, "tenant-reader", []string{"tenant-b:read"})
		readAuthHeader := "Bearer " + readToken

		writeToken := generateScopedTestToken(cfg.JWTSecret, "tenant-writer", []string{"tenant-b:write"})
		writeAuthHeader := "Bearer " + writeToken

		// 1. Reader tries to dequeue (should succeed / status 200)
		reqRead, _ := http.NewRequest("GET", baseURL+"/dequeue?namespace=tenant-b&topic=test-topic&limit=1", nil)
		reqRead.Header.Set("Authorization", readAuthHeader)
		respRead, err := client.Do(reqRead)
		if err != nil {
			t.Fatal(err)
		}
		defer respRead.Body.Close()
		if respRead.StatusCode == http.StatusForbidden {
			t.Error("expected reader to have access to dequeue, got 403")
		}

		// 2. Reader tries to enqueue (should fail / status 403)
		payload := []map[string]interface{}{
			{
				"namespace": "tenant-b",
				"topic":     "test-topic",
				"payload":   []byte("test"),
			},
		}
		bodyBytes, _ := json.Marshal(payload)
		reqWriteForbidden, _ := http.NewRequest("POST", baseURL+"/enqueue", bytes.NewReader(bodyBytes))
		reqWriteForbidden.Header.Set("Authorization", readAuthHeader)
		respWriteForbidden, err := client.Do(reqWriteForbidden)
		if err != nil {
			t.Fatal(err)
		}
		defer respWriteForbidden.Body.Close()
		if respWriteForbidden.StatusCode != http.StatusForbidden {
			t.Errorf("expected reader to be forbidden from enqueue, got %d", respWriteForbidden.StatusCode)
		}

		// 3. Writer tries to enqueue (should succeed / status 200)
		reqWrite, _ := http.NewRequest("POST", baseURL+"/enqueue", bytes.NewReader(bodyBytes))
		reqWrite.Header.Set("Authorization", writeAuthHeader)
		respWrite, err := client.Do(reqWrite)
		if err != nil {
			t.Fatal(err)
		}
		defer respWrite.Body.Close()
		if respWrite.StatusCode == http.StatusForbidden {
			t.Error("expected writer to have access to enqueue, got 403")
		}

		// 4. Writer tries to dequeue (should fail / status 403)
		reqReadForbidden, _ := http.NewRequest("GET", baseURL+"/dequeue?namespace=tenant-b&topic=test-topic&limit=1", nil)
		reqReadForbidden.Header.Set("Authorization", writeAuthHeader)
		respReadForbidden, err := client.Do(reqReadForbidden)
		if err != nil {
			t.Fatal(err)
		}
		defer respReadForbidden.Body.Close()
		if respReadForbidden.StatusCode != http.StatusForbidden {
			t.Errorf("expected writer to be forbidden from dequeue, got %d", respReadForbidden.StatusCode)
		}
	})
}

func leaseAndNack(t *testing.T, client *http.Client, baseURL, authHeader, topic string) int64 {
	var items []store.Item
	for i := 0; i < 20; i++ {
		req, _ := http.NewRequest("GET", baseURL+"/dequeue?namespace=e2e&topic="+topic+"&limit=1", nil)
		req.Header.Set("Authorization", authHeader)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != 200 {
			resp.Body.Close()
			t.Fatalf("dequeue failed in leaseAndNack loop: %d", resp.StatusCode)
		}
		json.NewDecoder(resp.Body).Decode(&items)
		resp.Body.Close()
		if len(items) > 0 {
			break
		}
		time.Sleep(150 * time.Millisecond)
	}

	if len(items) == 0 {
		t.Fatal("expected item to nack, got none after retrying")
	}

	// Nack
	nackPayload := map[string]interface{}{
		"id":        items[0].ID,
		"namespace": items[0].Namespace,
		"topic":     items[0].Topic,
		"error":     "simulated processing failure",
	}
	body, _ := json.Marshal(nackPayload)
	req, err := http.NewRequest("POST", baseURL+"/nack", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", authHeader)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("nack failed: %v", err)
	}
	resp.Body.Close()

	return items[0].ID
}
