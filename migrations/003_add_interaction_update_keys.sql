-- Migration 003: idempotency records for interaction updates
--
-- An update uses optimistic concurrency (WHERE id = $1 AND version = $2), so a client
-- that retries after a lost response gets a 409 even though its own edit already
-- succeeded. The client cannot tell that apart from a genuine concurrent edit.
--
-- Recording the outcome keyed by the client-supplied Idempotency-Key, in the same
-- transaction as the update, lets a retry replay the original result instead.
--
-- Rows are keyed by (interaction_id, idempotency_key): the key scopes one edit attempt on
-- one interaction, and two different interactions may legitimately be sent the same key.
CREATE TABLE IF NOT EXISTS interaction_update_keys (
    interaction_id UUID NOT NULL,
    idempotency_key TEXT NOT NULL,
    version INT NOT NULL,
    title VARCHAR(255),
    body TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (interaction_id, idempotency_key)
);

-- Supports the cleanup sweep that removes records older than the replay window.
CREATE INDEX IF NOT EXISTS idx_interaction_update_keys_created_at
ON interaction_update_keys (created_at);