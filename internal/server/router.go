package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"mqueue/internal/buffer"
	"mqueue/internal/config"
	"mqueue/internal/log"
	"mqueue/internal/metrics"
	"mqueue/internal/prefetch"
	"mqueue/internal/retry"
	"mqueue/internal/store"
	"mqueue/internal/wal"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/httprate"
	"github.com/golang-jwt/jwt/v4"
	"go.uber.org/zap"
)

func SetupRouter(r *chi.Mux, cfg *config.Config, pgStore *store.PGStore, dlqStore *store.DLQStore, buffer *buffer.RedisBuffer, retryManager *retry.RetryManager, metrics *metrics.QueueMetrics, walManager *wal.WALManager, prefetcher *prefetch.RedisPrefetcher) {
	logger := log.NewLogger()
	r.Use(httprate.Limit(100, time.Minute, httprate.WithKeyFuncs(httprate.KeyByIP)))
	// r.Use(authMiddleware(cfg.JWTSecret, logger))

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		for i, db := range pgStore.GetDBs() {
			if err := db.PingContext(r.Context()); err != nil {
				logger.Error("Database health check failed", zap.Error(err), zap.Int("shard", i))
				http.Error(w, "Database unhealthy", http.StatusServiceUnavailable)
				return
			}
		}
		for i, client := range pgStore.GetRedisClients() {
			if err := client.Ping(r.Context()).Err(); err != nil {
				logger.Error("Redis health check failed", zap.Error(err), zap.Int("shard", i))
				http.Error(w, "Redis unhealthy", http.StatusServiceUnavailable)
				return
			}
		}
		w.Write([]byte("OK"))
	})
	r.Group(func(r chi.Router) {
		r.Use(authMiddleware(cfg.JWTSecret, logger))
		r.Get("/topics", func(w http.ResponseWriter, r *http.Request) {
			namespace := r.URL.Query().Get("namespace")
			if namespace == "" {
				logger.Error("Missing namespace parameter")
				http.Error(w, "Missing namespace", http.StatusBadRequest)
				return
			}
			topics, err := pgStore.GetActiveTopics(r.Context(), namespace)
			if err != nil {
				logger.Error("Failed to get active topics", zap.Error(err), zap.String("namespace", namespace))
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if err := json.NewEncoder(w).Encode(topics); err != nil {
				logger.Error("Failed to encode topics", zap.Error(err))
				http.Error(w, "Failed to encode response", http.StatusInternalServerError)
			}
		})

		r.Post("/enqueue", func(w http.ResponseWriter, r *http.Request) {
			var reqItems []struct {
				Namespace      string                 `json:"namespace"`
				Topic          string                 `json:"topic"`
				IdempotencyKey *string                `json:"idempotency_key"`
				Priority       int                    `json:"priority"`
				Payload        []byte                 `json:"payload"`
				Metadata       map[string]interface{} `json:"metadata"`
				DeliverAfter   time.Time              `json:"deliver_after"`
			}
			if err := json.NewDecoder(r.Body).Decode(&reqItems); err != nil {
				logger.Error("Failed to decode enqueue request", zap.Error(err))
				http.Error(w, "Invalid request body", http.StatusBadRequest)
				return
			}
			items := make([]store.Item, len(reqItems))
			now := time.Now()
			for i, reqItem := range reqItems {
				items[i] = store.Item{
					Namespace:      reqItem.Namespace,
					Topic:          reqItem.Topic,
					IdempotencyKey: reqItem.IdempotencyKey,
					Priority:       reqItem.Priority,
					Payload:        reqItem.Payload,
					Metadata:       reqItem.Metadata,
					DeliverAfter:   reqItem.DeliverAfter,
					Status:         "ready",
					CreatedAt:      now,
					UpdatedAt:      now,
				}
			}
			start := time.Now()
			ids, err := buffer.Enqueue(r.Context(), items, walManager)
			if err != nil {
				logger.Error("Failed to enqueue items", zap.Error(err))
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			metrics.EnqueueTotal.WithLabelValues(items[0].Namespace, items[0].Topic).Add(float64(len(items)))
			if err := json.NewEncoder(w).Encode(ids); err != nil {
				logger.Error("Failed to encode enqueue response", zap.Error(err))
				http.Error(w, "Failed to encode response", http.StatusInternalServerError)
			}
			logger.Info("Enqueued items", zap.Int("count", len(ids)), zap.Duration("duration", time.Since(start)))
		})

		r.Get("/dequeue", func(w http.ResponseWriter, r *http.Request) {
			namespace := r.URL.Query().Get("namespace")
			topic := r.URL.Query().Get("topic")
			limitStr := r.URL.Query().Get("limit")
			limit, _ := strconv.Atoi(limitStr)
			if limit <= 0 {
				limit = cfg.WorkerBatchSize
			}
			if namespace == "" || topic == "" {
				logger.Error("Missing namespace or topic")
				http.Error(w, "Missing namespace or topic", http.StatusBadRequest)
				return
			}

			start := time.Now()
			var items []store.Item

			// 1. Fast path: Try Redis ready queue first
			client := pgStore.GetRedisForShard(namespace, topic)
			readyKey := fmt.Sprintf("mqueue:queue:%s:%s", namespace, topic)
			redisData, err := client.LRange(r.Context(), readyKey, 0, int64(limit-1)).Result()
			if err == nil {
				if len(redisData) > 0 {
					for _, data := range redisData {
						var item store.Item
						if json.Unmarshal([]byte(data), &item) == nil {
							items = append(items, item)
						}
					}
					// Remove consumed items
					if len(items) > 0 {
						client.LTrim(r.Context(), readyKey, int64(len(items)), -1)
					}
				}

				// Optimization: If queue is running low/empty, wake up prefetcher immediately
				if len(redisData) < limit {
					prefetcher.Trigger()
				}
			}

			// 2. Fallback: Lease from Postgres if needed
			if len(items) < limit {
				needed := limit - len(items)
				dbItems, err := pgStore.LeaseItems(r.Context(), namespace, topic, cfg.WorkerID, needed, cfg.LeaseTTL)
				if err != nil {
					logger.Error("Failed to lease items from DB", zap.Error(err))
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				items = append(items, dbItems...)
			}

			metrics.DequeueTotal.WithLabelValues(namespace, topic).Add(float64(len(items)))
			if err := json.NewEncoder(w).Encode(items); err != nil {
				logger.Error("Failed to encode dequeue response", zap.Error(err))
				http.Error(w, "Failed to encode response", http.StatusInternalServerError)
				return
			}
			logger.Info("Dequeued items", zap.Int("count", len(items)), zap.Duration("duration", time.Since(start)))
		})

		r.Post("/ack", func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				ID        int64  `json:"id"`
				Namespace string `json:"namespace"`
				Topic     string `json:"topic"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				logger.Error("Failed to decode ack request", zap.Error(err))
				http.Error(w, "Invalid request body", http.StatusBadRequest)
				return
			}
			start := time.Now()
			if err := pgStore.AckItem(r.Context(), req.Namespace, req.Topic, req.ID); err != nil {
				logger.Error("Failed to ack item", zap.Error(err), zap.Int64("id", req.ID))
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			metrics.AckTotal.WithLabelValues(req.Namespace, req.Topic).Inc()
			logger.Info("Acknowledged item", zap.Int64("id", req.ID), zap.Duration("duration", time.Since(start)))
			w.Write([]byte("OK"))
		})

		r.Post("/nack", func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				ID        int64  `json:"id"`
				Namespace string `json:"namespace"`
				Topic     string `json:"topic"`
				Error     string `json:"error"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				logger.Error("Failed to decode nack request", zap.Error(err))
				http.Error(w, "Invalid request body", http.StatusBadRequest)
				return
			}
			item, err := pgStore.GetItem(r.Context(), req.Namespace, req.Topic, req.ID)
			if err != nil {
				logger.Error("Failed to get item for nack", zap.Error(err), zap.Int64("id", req.ID))
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			start := time.Now()
			if err := retryManager.RetryItem(r.Context(), item, req.Error); err != nil {
				logger.Error("Failed to nack item", zap.Error(err), zap.Int64("id", req.ID))
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			metrics.NackTotal.WithLabelValues(req.Namespace, req.Topic).Inc()
			logger.Info("Negatively acknowledged item", zap.Int64("id", req.ID), zap.Duration("duration", time.Since(start)))
			w.Write([]byte("OK"))
		})

		r.Get("/dlq", func(w http.ResponseWriter, r *http.Request) {
			namespace := r.URL.Query().Get("namespace")
			topic := r.URL.Query().Get("topic")
			limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
			if limit <= 0 {
				limit = 10
			}
			if namespace == "" || topic == "" {
				logger.Error("Missing namespace or topic")
				http.Error(w, "Missing namespace or topic", http.StatusBadRequest)
				return
			}
			items, err := dlqStore.GetDLQItems(r.Context(), namespace, topic, limit)
			if err != nil {
				logger.Error("Failed to get DLQ items", zap.Error(err))
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if err := json.NewEncoder(w).Encode(items); err != nil {
				logger.Error("Failed to encode DLQ response", zap.Error(err))
				http.Error(w, "Failed to encode response", http.StatusInternalServerError)
			}
		})

		r.Post("/dlq/delete", func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				ID        int64  `json:"id"`
				Namespace string `json:"namespace"`
				Topic     string `json:"topic"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				logger.Error("Failed to decode DLQ delete request", zap.Error(err))
				http.Error(w, "Invalid request body", http.StatusBadRequest)
				return
			}
			if err := dlqStore.DeleteDLQItem(r.Context(), req.ID, req.Namespace, req.Topic); err != nil {
				logger.Error("Failed to delete DLQ item", zap.Error(err), zap.Int64("id", req.ID))
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			logger.Info("Deleted DLQ item", zap.Int64("id", req.ID))
			w.Write([]byte("OK"))
		})
	})
}

func authMiddleware(jwtSecret string, logger *log.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tokenStr := r.Header.Get("Authorization")
			if tokenStr == "" {
				logger.Error("Missing authorization token")
				http.Error(w, "Missing token", http.StatusUnauthorized)
				return
			}
			if len(tokenStr) > 7 && tokenStr[:7] == "Bearer " {
				tokenStr = tokenStr[7:]
			}
			token, err := jwt.Parse(tokenStr, func(token *jwt.Token) (interface{}, error) {
				if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
					return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
				}
				return []byte(jwtSecret), nil
			})
			if err != nil || !token.Valid {
				logger.Error("Invalid JWT token", zap.Error(err))
				http.Error(w, "Invalid token", http.StatusUnauthorized)
				return
			}
			ctx := context.WithValue(r.Context(), "claims", token.Claims)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
