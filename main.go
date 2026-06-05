package main

import (
	"context"
	"crypto/tls"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"strings"
	"syscall"
	"time"

	"mqueue/internal/buffer"
	"mqueue/internal/config"
	"mqueue/internal/flusher"
	"mqueue/internal/lease"
	"mqueue/internal/log"
	"mqueue/internal/metrics"
	"mqueue/internal/prefetch"
	"mqueue/internal/recovery"
	"mqueue/internal/retry"
	"mqueue/internal/server"
	"mqueue/internal/id"
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
	// Re-initialize logger with config (level/encoding)
	logger = log.NewFromConfig(cfg.LogLevel, cfg.LogEncoding)
	defer logger.Sync()

	var redisClients []redis.UniversalClient
	if cfg.RedisClusterMode {
		clusterClient := redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:    cfg.RedisAddrs,
			Password: cfg.RedisPassword,
		})
		if err := clusterClient.Ping(context.Background()).Err(); err != nil {
			logger.Fatal("Failed to connect to Redis Cluster", zap.Error(err))
		}
		// Map same cluster client to all database shards
		redisClients = make([]redis.UniversalClient, len(cfg.DatabaseURLs))
		for i := range redisClients {
			redisClients[i] = clusterClient
		}
	} else if cfg.RedisSentinelMaster != "" {
		sentinelClient := redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    cfg.RedisSentinelMaster,
			SentinelAddrs: cfg.RedisAddrs,
			Password:      cfg.RedisPassword,
		})
		if err := sentinelClient.Ping(context.Background()).Err(); err != nil {
			logger.Fatal("Failed to connect to Redis Sentinel", zap.Error(err))
		}
		// Map same failover client to all database shards
		redisClients = make([]redis.UniversalClient, len(cfg.DatabaseURLs))
		for i := range redisClients {
			redisClients[i] = sentinelClient
		}
	} else {
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
			if err := redisClients[i].Ping(context.Background()).Err(); err != nil {
				logger.Fatal("Failed to connect to Redis", zap.Error(err), zap.Int("index", i))
			}
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

	var leaseManager *id.LeaseManager
	if len(redisClients) > 0 {
		leaseManager = id.NewLeaseManager(redisClients[0], cfg.WorkerID)
		acquiredID, err := leaseManager.Acquire(context.Background())
		if err != nil {
			logger.Fatal("Failed to acquire dynamic Snowflake Node ID lease", zap.Error(err))
		}
		cfg.NodeID = acquiredID
		logger.Info("Acquired dynamic Snowflake Node ID lease", zap.Int64("node_id", cfg.NodeID))

		// Start renewal loop in background
		leaseManager.StartRenewalLoop(context.Background(), func() {
			logger.Fatal("Lost Snowflake Node ID lease from Redis. Shutting down server to prevent ID collisions.")
		})
	}

	metrics := metrics.NewQueueMetrics(pgStore, cfg, logger)
	redisBuffer := buffer.NewRedisBuffer(redisClients, cfg, pgStore, logger)
	flusher := flusher.NewFlusher(redisBuffer, pgStore, cfg, logger).WithMetrics(metrics)
	prefetcher := prefetch.NewRedisPrefetcher(redisClients, pgStore, cfg, logger)
	leaseDaemon := lease.NewLeaseDaemon(redisClients, pgStore, cfg, logger)
	retryManager := retry.NewRetryManager(pgStore, dlqStore, cfg, logger)
	recoveryDaemon := recovery.NewRecoveryDaemon(redisClients, pgStore, walManager, cfg, logger)

	// Context for background workers
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	// Start Daemons
	runDaemon := func(name string, runFunc func(context.Context)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			logger.Info("Starting daemon", zap.String("name", name))
			runFunc(ctx)
			logger.Info("Daemon stopped", zap.String("name", name))
		}()
	}

	runDaemon("Flusher", flusher.Run)
	runDaemon("Prefetcher", prefetcher.Run)
	runDaemon("LeaseDaemon", leaseDaemon.Run)
	runDaemon("Metrics", metrics.Run)
	runDaemon("RecoveryDaemon", recoveryDaemon.Run)

	// Periodic Task: Clean Empty Topics
	wg.Add(1)
	go func() {
		defer wg.Done()
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

	// Periodic Task: Cleanup Old WAL Files
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := walManager.Cleanup(cfg.WALRetention); err != nil {
					logger.Error("Failed to cleanup old WAL files", zap.Error(err))
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	r := chi.NewRouter()
	server.SetupRouter(r, cfg, pgStore, dlqStore, redisBuffer, retryManager, metrics, walManager, prefetcher)
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
			CipherSuites: []uint16{
				tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
				tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
				tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			},
			CurvePreferences: []tls.CurveID{
				tls.CurveP256,
				tls.X25519,
			},
			PreferServerCipherSuites: true,
		}
	} else {
		logger.Warn("TLS_CERT_FILE or TLS_KEY_FILE not set, using HTTP")
	}

	// Start Server
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

	// Graceful Shutdown Listener
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	logger.Info("Server started on :8080")
	<-stop // Wait for signal
	logger.Info("Shutting down...")

	// 1. Stop accepting new requests
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("Server shutdown failed", zap.Error(err))
	} else {
		logger.Info("HTTP Server stopped")
	}

	// 2. Signal background workers to stop (and flush)
	cancel()

	// 3. Wait for workers to finish
	logger.Info("Waiting for background tasks to finish...")
	wg.Wait()
	if leaseManager != nil {
		leaseManager.Close()
		logger.Info("Snowflake Node ID lease released successfully")
	}
	logger.Info("All background tasks finished")
}
