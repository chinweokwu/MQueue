package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"hash/fnv"

	"mqueue/internal/config"
	"mqueue/internal/log"

	_ "github.com/lib/pq"
	"go.uber.org/zap"
)

type DLQStore struct {
	dbs    []*sql.DB
	cfg    *config.Config
	logger *log.Logger
}

func NewDLQStore(dbURLs []string, cfg *config.Config) (*DLQStore, error) {
	logger := log.NewLogger()
	var dbs []*sql.DB
	for _, url := range dbURLs {
		db, err := sql.Open("postgres", url)
		if err != nil {
			logger.Error("Failed to open postgres", zap.Error(err), zap.String("url", url))
			return nil, fmt.Errorf("open postgres %s: %w", url, err)
		}
		db.SetMaxOpenConns(20)
		db.SetMaxIdleConns(10)
		dbs = append(dbs, db)
	}
	return &DLQStore{
		dbs:    dbs,
		cfg:    cfg,
		logger: logger,
	}, nil
}

func (s *DLQStore) GetDBs() []*sql.DB {
	return s.dbs
}

func (s *DLQStore) GetShardID(namespace, topic string) int {
	h := fnv.New32a()
	h.Write([]byte(namespace + topic))
	return int(h.Sum32() % uint32(len(s.dbs)))
}

func (s *DLQStore) getDBForShard(namespace, topic string) *sql.DB {
	return s.dbs[s.GetShardID(namespace, topic)]
}

func (s *DLQStore) GetDLQItems(ctx context.Context, namespace, topic string, limit int) ([]DeadLetter, error) {
	db := s.getDBForShard(namespace, topic)
	rows, err := db.QueryContext(ctx, `
        SELECT id, original_id, namespace, topic, idempotency_key, priority, payload, metadata, last_error, retries, first_failed_at, moved_at
        FROM dead_letter
        WHERE namespace = $1 AND topic = $2
        ORDER BY moved_at
        LIMIT $3
    `, namespace, topic, limit)
	if err != nil {
		s.logger.Error("Failed to get DLQ items", zap.Error(err))
		return nil, fmt.Errorf("get DLQ items: %w", err)
	}
	defer rows.Close()

	var items []DeadLetter
	for rows.Next() {
		var item DeadLetter
		var metadataStr []byte
		err := rows.Scan(&item.ID, &item.OriginalID, &item.Namespace, &item.Topic, &item.IdempotencyKey, &item.Priority,
			&item.Payload, &metadataStr, &item.LastError, &item.Retries, &item.FirstFailedAt, &item.MovedAt)
		if err != nil {
			s.logger.Error("Failed to scan DLQ item", zap.Error(err))
			return nil, fmt.Errorf("scan DLQ item: %w", err)
		}
		if len(metadataStr) > 0 {
			if err := json.Unmarshal(metadataStr, &item.Metadata); err != nil {
				s.logger.Error("Failed to unmarshal DLQ metadata", zap.Error(err))
				return nil, fmt.Errorf("unmarshal DLQ metadata: %w", err)
			}
		}
		items = append(items, item)
	}
	return items, nil
}

func (s *DLQStore) DeleteDLQItem(ctx context.Context, id int64, namespace, topic string) error {
	db := s.getDBForShard(namespace, topic)
	_, err := db.ExecContext(ctx, `
        DELETE FROM dead_letter WHERE original_id = $1 AND namespace = $2 AND topic = $3
    `, id, namespace, topic)
	if err != nil {
		s.logger.Error("Failed to delete DLQ item", zap.Error(err), zap.Int64("id", id))
		return fmt.Errorf("delete DLQ item: %w", err)
	}
	s.logger.Info("Deleted DLQ item", zap.Int64("id", id))
	return nil
}
