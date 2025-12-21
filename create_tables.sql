-- Main queue table
CREATE TABLE IF NOT EXISTS items (
    id BIGSERIAL PRIMARY KEY,
    namespace VARCHAR(255) NOT NULL,
    topic VARCHAR(255) NOT NULL,
    idempotency_key VARCHAR(255),
    priority INTEGER NOT NULL DEFAULT 5,
    payload BYTEA NOT NULL,
    metadata JSONB,
    deliver_after TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    lease_expires_at TIMESTAMP WITH TIME ZONE,
    lease_owner VARCHAR(255),
    ttl TIMESTAMP WITH TIME ZONE,
    status VARCHAR(50) NOT NULL DEFAULT 'ready',
    retries INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    first_failed_at TIMESTAMP WITH TIME ZONE,
    last_error TEXT,

    -- Critical for idempotency (used in ON CONFLICT)
    CONSTRAINT uniq_namespace_idempotency UNIQUE (namespace, idempotency_key)
);

-- Dead letter queue
CREATE TABLE IF NOT EXISTS dead_letter (  -- Note: your code uses "dead_letter", not "dead_letters"
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
    moved_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Indexes for performance

-- 1. Main lease query index (most important!)
CREATE INDEX IF NOT EXISTS idx_items_ready_lease 
ON items (namespace, topic, priority DESC, deliver_after ASC)
WHERE status = 'ready' 
  AND deliver_after <= NOW() 
  AND (lease_expires_at IS NULL OR lease_expires_at < NOW());

-- 2. General lookup
CREATE INDEX IF NOT EXISTS idx_items_namespace_topic ON items (namespace, topic);

-- 3. DLQ lookup
CREATE INDEX IF NOT EXISTS idx_dead_letter_namespace_topic ON dead_letter (namespace, topic);

-- 4. Optional: for cleaning old items by TTL (if you add expiry job later)
-- CREATE INDEX IF NOT EXISTS idx_items_ttl ON items (ttl) WHERE ttl IS NOT NULL;