--liquibase formatted sql

--changeset fyke:01-create-outbox-table
CREATE TABLE IF NOT EXISTS fyke_outbox (
    id               UUID PRIMARY KEY,
    seq              BIGSERIAL,
    partition_key    TEXT NOT NULL DEFAULT 'default',
    type             TEXT NOT NULL,
    destination      TEXT NOT NULL,
    target           TEXT,
    business_key     TEXT NOT NULL,
    idempotency_key  TEXT NOT NULL UNIQUE,
    status           TEXT NOT NULL,
    content_type     TEXT NOT NULL DEFAULT 'application/json',
    payload          BYTEA NOT NULL,
    payload_hash     TEXT NOT NULL,
    headers          JSONB,
    correlation_id   TEXT,
    trace_id         TEXT,
    size             INT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL,
    updated_at       TIMESTAMPTZ NOT NULL,
    published_at     TIMESTAMPTZ,
    attempts         INT NOT NULL DEFAULT 0,
    next_attempt_at  TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_fyke_outbox_claim ON fyke_outbox (status, partition_key, next_attempt_at, seq)
    WHERE status IN ('NEW', 'DISPATCHING');
CREATE INDEX IF NOT EXISTS idx_fyke_outbox_business_key ON fyke_outbox (business_key, status);
CREATE INDEX IF NOT EXISTS idx_fyke_outbox_retention ON fyke_outbox (status, published_at)
    WHERE status = 'PUBLISHED';

--changeset fyke:02-create-dlq-table
CREATE TABLE IF NOT EXISTS fyke_dlq (
    id               UUID PRIMARY KEY,
    source           TEXT NOT NULL,
    outbox_id        UUID,
    partition_key    TEXT NOT NULL DEFAULT 'default',
    type             TEXT NOT NULL,
    destination      TEXT NOT NULL,
    target           TEXT,
    business_key     TEXT NOT NULL,
    content_type     TEXT NOT NULL DEFAULT 'application/json',
    payload          BYTEA NOT NULL,
    headers          JSONB,
    reason           TEXT NOT NULL,
    consumer         TEXT,
    received_at      TIMESTAMPTZ NOT NULL,
    replayed_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_fyke_dlq_business_key ON fyke_dlq (business_key, source, received_at);
CREATE INDEX IF NOT EXISTS idx_fyke_dlq_retention ON fyke_dlq (replayed_at, received_at);
