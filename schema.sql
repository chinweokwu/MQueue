CREATE TABLE items (
    id BIGSERIAL PRIMARY KEY,
    namespace TEXT NOT NULL,
    topic TEXT NOT NULL,
    idempotency_key TEXT,
    priority INTEGER NOT NULL DEFAULT 0,
    payload BYTEA NOT NULL,
    metadata JSONB,
    deliver_after TIMESTAMP WITH TIME ZONE NOT NULL,
    lease_expires_at TIMESTAMP WITH TIME ZONE,
    lease_owner TEXT,
    ttl TIMESTAMP WITH TIME ZONE,
    status TEXT NOT NULL,
    retries INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    first_failed_at TIMESTAMP WITH TIME ZONE,
    UNIQUE(namespace, idempotency_key)
);

CREATE TABLE dead_letter (
    id BIGSERIAL PRIMARY KEY,
    original_id BIGINT NOT NULL,
    namespace TEXT NOT NULL,
    topic TEXT NOT NULL,
    idempotency_key TEXT,
    priority INTEGER NOT NULL,
    payload BYTEA NOT NULL,
    metadata JSONB,
    last_error TEXT,
    retries INTEGER NOT NULL,
    first_failed_at TIMESTAMP WITH TIME ZONE,
    moved_at TIMESTAMP WITH TIME ZONE
);