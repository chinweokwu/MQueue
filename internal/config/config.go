package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"mqueue/internal/log"

	"github.com/joho/godotenv"
	"go.uber.org/zap"
)

type Config struct {
	DatabaseURLs      []string
	RedisAddrs        []string
	RedisPassword     string
	WALDir            string
	Namespace         string
	Topic             string
	MaxRetries        int
	WorkerBatchSize   int
	PrefetchBatchSize int
	FlushBatchSize    int
	BufferTTL         time.Duration
	PrefetchInterval  time.Duration
	FlushInterval     time.Duration
	LeaseTTL          time.Duration
	LeaseRenewPeriod  time.Duration
	RetryBackoff      time.Duration
	NamespaceQuotas   map[string]int
	DeliveryMode      string
	JWTSecret         string
	WorkerID          string
}

func Load() (*Config, error) {
	// Load .env file
	if err := godotenv.Load(); err != nil {
		// Log error but continue, as .env file is optional if variables are set elsewhere
		logger := log.NewLogger()
		logger.Warn("Failed to load .env file", zap.Error(err))
	}

	logger := log.NewLogger()
	cfg := &Config{
		DatabaseURLs:      strings.Split(os.Getenv("DATABASE_URLS"), ","),
		RedisAddrs:        strings.Split(os.Getenv("REDIS_ADDRS"), ","),
		RedisPassword:     os.Getenv("REDIS_PASSWORD"),
		WALDir:            os.Getenv("WAL_DIR"),
		Namespace:         "default",
		Topic:             "tasks",
		MaxRetries:        3,
		WorkerBatchSize:   10,
		PrefetchBatchSize: 50,
		FlushBatchSize:    50,
		BufferTTL:         time.Hour,
		PrefetchInterval:  3 * time.Second,
		FlushInterval:     5 * time.Second,
		LeaseTTL:          30 * time.Second,
		LeaseRenewPeriod:  10 * time.Second,
		RetryBackoff:      1 * time.Second,
		NamespaceQuotas:   make(map[string]int),
		DeliveryMode:      os.Getenv("DELIVERY_MODE"),
		JWTSecret:         os.Getenv("JWT_SECRET"),
		WorkerID:          os.Getenv("WORKER_ID"),
	}

	if len(cfg.DatabaseURLs) == 0 || cfg.DatabaseURLs[0] == "" {
		logger.Error("DATABASE_URLS is required")
		return nil, fmt.Errorf("DATABASE_URLS is required")
	}
	if len(cfg.RedisAddrs) == 0 || cfg.RedisAddrs[0] == "" {
		logger.Error("REDIS_ADDRS is required")
		return nil, fmt.Errorf("REDIS_ADDRS is required")
	}
	if cfg.WALDir == "" {
		logger.Error("WAL_DIR is required")
		return nil, fmt.Errorf("WAL_DIR is required")
	}
	if cfg.DeliveryMode == "" {
		cfg.DeliveryMode = "at-least-once"
	}
	if cfg.JWTSecret == "" {
		logger.Error("JWT_SECRET is required")
		return nil, fmt.Errorf("JWT_SECRET is required")
	}
	if cfg.WorkerID == "" {
		cfg.WorkerID = "worker-1"
		logger.Info("Using default WorkerID", zap.String("worker_id", cfg.WorkerID))
	}

	quotas := os.Getenv("NAMESPACE_QUOTAS")
	if quotas != "" {
		for _, q := range strings.Split(quotas, ",") {
			parts := strings.Split(q, ":")
			if len(parts) != 2 {
				logger.Error("Invalid NAMESPACE_QUOTAS format", zap.String("quota", q))
				return nil, fmt.Errorf("invalid NAMESPACE_QUOTAS format: %s", q)
			}
			limit, err := strconv.Atoi(parts[1])
			if err != nil {
				logger.Error("Invalid quota limit", zap.String("namespace", parts[0]), zap.Error(err))
				return nil, fmt.Errorf("invalid quota limit for %s: %w", parts[0], err)
			}
			cfg.NamespaceQuotas[parts[0]] = limit
		}
	}

	logger.Info("Config loaded successfully")
	return cfg, nil
}
