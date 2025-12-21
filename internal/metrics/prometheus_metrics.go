package metrics

import (
	"context"
	"crypto/tls"
	"fmt"
	"mqueue/internal/config"
	"mqueue/internal/log"
	"mqueue/internal/store"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

type QueueMetrics struct {
	EnqueueTotal    *prometheus.CounterVec
	DequeueTotal    *prometheus.CounterVec
	AckTotal        *prometheus.CounterVec
	NackTotal       *prometheus.CounterVec
	QueueDepth      *prometheus.GaugeVec // Items in buffer (pre-flush)
	ReadyQueueDepth *prometheus.GaugeVec // Items in prefetched ready queue (fast path)
	ShardHealth     *prometheus.GaugeVec
	store           *store.PGStore
	cfg             *config.Config
	logger          *log.Logger
}

func NewQueueMetrics(store *store.PGStore, cfg *config.Config, logger *log.Logger) *QueueMetrics {
	metrics := &QueueMetrics{
		EnqueueTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "mqueue_enqueue_total",
				Help: "Total number of enqueued items",
			},
			[]string{"namespace", "topic"},
		),
		DequeueTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "mqueue_dequeue_total",
				Help: "Total number of dequeued items",
			},
			[]string{"namespace", "topic"},
		),
		AckTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "mqueue_ack_total",
				Help: "Total number of acknowledged items",
			},
			[]string{"namespace", "topic"},
		),
		NackTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "mqueue_nack_total",
				Help: "Total number of negatively acknowledged items",
			},
			[]string{"namespace", "topic"},
		),
		QueueDepth: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "mqueue_buffer_queue_depth",
				Help: "Number of items in Redis buffer (pre-flush) per namespace and topic",
			},
			[]string{"namespace", "topic"},
		),
		ReadyQueueDepth: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "mqueue_ready_queue_depth",
				Help: "Number of items in prefetched Redis ready queue (fast dequeue path) per namespace and topic",
			},
			[]string{"namespace", "topic"},
		),
		ShardHealth: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "mqueue_shard_health",
				Help: "Health status of shards (1 = healthy, 0 = unhealthy)",
			},
			[]string{"shard", "type"},
		),
		store:  store,
		cfg:    cfg,
		logger: logger,
	}

	prometheus.MustRegister(
		metrics.EnqueueTotal,
		metrics.DequeueTotal,
		metrics.AckTotal,
		metrics.NackTotal,
		metrics.QueueDepth,
		metrics.ReadyQueueDepth,
		metrics.ShardHealth,
	)

	return metrics
}

func (m *QueueMetrics) Run(ctx context.Context) {
	logger := m.logger
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{
		Addr:    ":2112",
		Handler: mux,
	}

	// Load TLS certificates
	certFile := os.Getenv("TLS_CERT_FILE")
	keyFile := os.Getenv("TLS_KEY_FILE")
	var tlsConfig *tls.Config
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			logger.Fatal("Failed to load TLS certificates for metrics", zap.Error(err))
		}
		tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}
	} else {
		logger.Warn("TLS_CERT_FILE or TLS_KEY_FILE not set for metrics, using HTTP")
	}

	// Start metrics collection
	go m.collectMetrics(ctx)

	go func() {
		if tlsConfig != nil {
			srv.TLSConfig = tlsConfig
			logger.Info("Metrics server starting on :2112 with TLS")
			if err := srv.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
				logger.Error("Metrics server failed", zap.Error(err))
			}
		} else {
			logger.Info("Metrics server starting on :2112 without TLS")
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logger.Error("Metrics server failed", zap.Error(err))
			}
		}
	}()
	<-ctx.Done()
	if err := srv.Shutdown(context.Background()); err != nil {
		logger.Error("Metrics server shutdown failed", zap.Error(err))
	}
}

func (m *QueueMetrics) collectMetrics(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			m.logger.Info("Metrics collection shutting down")
			return
		case <-ticker.C:
			for namespace := range m.cfg.NamespaceQuotas {
				topics, err := m.store.GetActiveTopics(ctx, namespace)
				if err != nil {
					m.logger.Error("Failed to get active topics for metrics", zap.Error(err))
					continue
				}
				for _, topic := range topics {
					client := m.store.GetRedisForShard(namespace, topic)

					// Buffer queue depth
					bufferKey := fmt.Sprintf("mqueue:buffer:%s:%s", namespace, topic)
					bufferLen, err := client.LLen(ctx, bufferKey).Result()
					if err == nil {
						m.QueueDepth.WithLabelValues(namespace, topic).Set(float64(bufferLen))
					}

					// Ready queue depth (prefetched)
					readyKey := fmt.Sprintf("mqueue:queue:%s:%s", namespace, topic)
					readyLen, err := client.LLen(ctx, readyKey).Result()
					if err == nil {
						m.ReadyQueueDepth.WithLabelValues(namespace, topic).Set(float64(readyLen))
					}
				}
			}

			// Shard health
			for i, db := range m.store.GetDBs() {
				shard := fmt.Sprintf("%d", i)
				if err := db.PingContext(ctx); err != nil {
					m.ShardHealth.WithLabelValues(shard, "postgres").Set(0)
					m.logger.Error("Postgres shard unhealthy", zap.Int("shard", i), zap.Error(err))
				} else {
					m.ShardHealth.WithLabelValues(shard, "postgres").Set(1)
				}
			}
			for i, client := range m.store.GetRedisClients() {
				shard := fmt.Sprintf("%d", i)
				if err := client.Ping(ctx).Err(); err != nil {
					m.ShardHealth.WithLabelValues(shard, "redis").Set(0)
					m.logger.Error("Redis shard unhealthy", zap.Int("shard", i), zap.Error(err))
				} else {
					m.ShardHealth.WithLabelValues(shard, "redis").Set(1)
				}
			}
		}
	}
}
