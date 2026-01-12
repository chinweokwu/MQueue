CREATE TABLE IF NOT EXISTS items (
    id BIGINT PRIMARY KEY,
    namespace TEXT NOT NULL,
    topic TEXT NOT NULL,
    idempotency_key TEXT,
    priority INTEGER NOT NULL DEFAULT 0,
    payload BYTEA,
    metadata JSONB,
    deliver_after TIMESTAMP WITH TIME ZONE,
    lease_expires_at TIMESTAMP WITH TIME ZONE,
    lease_owner TEXT,
    ttl TIMESTAMP WITH TIME ZONE,
    status TEXT NOT NULL DEFAULT 'ready',
    retries INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    first_failed_at TIMESTAMP WITH TIME ZONE,
    
    CONSTRAINT items_idempotency_key_unique UNIQUE (namespace, idempotency_key)
);

CREATE INDEX idx_items_prefetch ON items (namespace, topic, status, priority, deliver_after);
CREATE INDEX idx_items_cleanup ON items (namespace, status);

CREATE TABLE IF NOT EXISTS dead_letter (
    original_id BIGINT PRIMARY KEY,
    namespace TEXT NOT NULL,
    topic TEXT NOT NULL,
    idempotency_key TEXT,
    priority INTEGER,
    payload BYTEA,
    metadata JSONB,
    last_error TEXT,
    retries INTEGER,
    first_failed_at TIMESTAMP WITH TIME ZONE,
    moved_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_dlq_namespace_topic ON dead_letter (namespace, topic);
