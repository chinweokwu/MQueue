package main

import (
	"context"
	"crypto/tls"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"mqueue/internal/buffer"
	"mqueue/internal/config"
	"mqueue/internal/flusher"
	"mqueue/internal/lease"
	"mqueue/internal/log"
	"mqueue/internal/metrics"
	"mqueue/internal/prefetch"
	"mqueue/internal/retry"
	"mqueue/internal/server"
	"mqueue/internal/store"
	"mqueue/internal/wal"

	"github.com/go-chi/chi/v5"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

func main() {
	logger := log.NewLogger()
	cfg, err := config.Load()
	if err != nil {
		logger.Fatal("Failed to load config", zap.Error(err))
	}

	redisClients := make([]*redis.Client, len(cfg.RedisAddrs))
	for i, addr := range cfg.RedisAddrs {
		redisClients[i] = redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: cfg.RedisPassword,
		})
		if err := redisClients[i].Ping(context.Background()).Err(); err != nil {
			logger.Fatal("Failed to connect to Redis", zap.Error(err), zap.Int("index", i))
		}
	}

	pgStore, err := store.NewPGStore(cfg.DatabaseURLs, redisClients, cfg)
	if err != nil {
		logger.Fatal("Failed to initialize store", zap.Error(err))
	}
	defer func() {
		for _, db := range pgStore.GetDBs() {
			db.Close()
		}
		for _, rdb := range pgStore.GetRedisClients() {
			rdb.Close()
		}
	}()

	dlqStore, err := store.NewDLQStore(cfg.DatabaseURLs, cfg)
	if err != nil {
		logger.Fatal("Failed to initialize DLQ store", zap.Error(err))
	}
	defer func() {
		for _, db := range dlqStore.GetDBs() {
			db.Close()
		}
	}()

	walManager, err := wal.NewWALManager(len(cfg.DatabaseURLs), cfg.WALDir)
	if err != nil {
		logger.Fatal("Failed to initialize WAL", zap.Error(err))
	}
	defer walManager.Close()

	if err := pgStore.Recover(context.Background(), walManager); err != nil {
		logger.Fatal("Failed to recover from WAL", zap.Error(err))
	}

	metrics := metrics.NewQueueMetrics(pgStore, cfg, logger)
	redisBuffer := buffer.NewRedisBuffer(redisClients, cfg, pgStore, logger)
	flusher := flusher.NewFlusher(redisBuffer, pgStore, cfg, logger)
	prefetcher := prefetch.NewRedisPrefetcher(redisClients, pgStore, cfg, logger)
	leaseDaemon := lease.NewLeaseDaemon(redisClients, pgStore, cfg, logger)
	retryManager := retry.NewRetryManager(pgStore, dlqStore, cfg, logger)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go flusher.Run(ctx)
	go prefetcher.Run(ctx)
	go leaseDaemon.Run(ctx)
	go metrics.Run(ctx)

	go func() {
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := pgStore.CleanEmptyTopics(ctx, cfg.Namespace); err != nil {
					logger.Error("Failed to clean empty topics", zap.Error(err))
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	r := chi.NewRouter()
	server.SetupRouter(r, cfg, pgStore, dlqStore, redisBuffer, retryManager, metrics, walManager)
	srv := &http.Server{
		Addr:    ":8080",
		Handler: r,
	}

	// Load TLS certificates
	certFile := os.Getenv("TLS_CERT_FILE")
	keyFile := os.Getenv("TLS_KEY_FILE")
	var tlsConfig *tls.Config
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			logger.Fatal("Failed to load TLS certificates", zap.Error(err))
		}
		tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}
	} else {
		logger.Warn("TLS_CERT_FILE or TLS_KEY_FILE not set, using HTTP")
	}

	go func() {
		if tlsConfig != nil {
			srv.TLSConfig = tlsConfig
			logger.Info("Server starting on :8080 with TLS")
			if err := srv.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
				logger.Fatal("Server failed", zap.Error(err))
			}
		} else {
			logger.Info("Server starting on :8080 without TLS")
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logger.Fatal("Server failed", zap.Error(err))
			}
		}
	}()

	logger.Info("Server started on :8080")
	<-ctx.Done()
	logger.Info("Shutting down")
	if err := srv.Shutdown(context.Background()); err != nil {
		logger.Error("Server shutdown failed", zap.Error(err))
	}
}
