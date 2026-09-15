--liquibase formatted sql

--changeset fyke:03-create-inbox-table
CREATE TABLE IF NOT EXISTS fyke_inbox (
    id               UUID PRIMARY KEY,
    seq              BIGSERIAL,
    partition_key    TEXT NOT NULL DEFAULT 'default',
    type             TEXT NOT NULL,
    destination      TEXT NOT NULL,
    target           TEXT,
    business_key     TEXT NOT NULL,
    message_id       TEXT,
    status           TEXT NOT NULL,
    content_type     TEXT NOT NULL DEFAULT 'application/json',
    payload          BYTEA NOT NULL,
    headers          JSONB,
    consumer         TEXT,
    ordering         TEXT NOT NULL DEFAULT 'STRICT_FIFO',
    created_at       TIMESTAMPTZ NOT NULL,
    updated_at       TIMESTAMPTZ NOT NULL,
    completed_at     TIMESTAMPTZ,
    attempts         INT NOT NULL DEFAULT 0,
    next_attempt_at  TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_fyke_inbox_claim ON fyke_inbox (status, partition_key, next_attempt_at, seq)
    WHERE status IN ('NEW', 'PROCESSING');
CREATE INDEX IF NOT EXISTS idx_fyke_inbox_business_key ON fyke_inbox (business_key, status);
CREATE INDEX IF NOT EXISTS idx_fyke_inbox_retention ON fyke_inbox (status, completed_at)
    WHERE status = 'COMPLETED';
CREATE INDEX IF NOT EXISTS idx_fyke_inbox_message_id ON fyke_inbox (message_id)
    WHERE message_id IS NOT NULL;

--changeset fyke:04-add-inbox-id-to-dlq
ALTER TABLE fyke_dlq ADD COLUMN IF NOT EXISTS inbox_id UUID;
