package store

import (
	"time"
)

type Item struct {
	ID             int64
	Namespace      string
	Topic          string
	IdempotencyKey *string
	Priority       int
	Payload        []byte
	Metadata       map[string]interface{}
	DeliverAfter   time.Time
	LeaseExpiresAt *time.Time
	LeaseOwner     *string
	TTL            *time.Time
	Status         string
	Retries        int
	CreatedAt      time.Time
	UpdatedAt      time.Time
	FirstFailedAt  *time.Time
	LastError      *string
}

type DeadLetter struct {
	ID             int64
	OriginalID     int64
	Namespace      string
	Topic          string
	IdempotencyKey *string
	Priority       int
	Payload        []byte
	Metadata       map[string]interface{}
	LastError      *string
	Retries        int
	FirstFailedAt  *time.Time
	MovedAt        time.Time
}
