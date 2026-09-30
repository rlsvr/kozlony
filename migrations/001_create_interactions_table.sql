-- Migration 001: Create interactions table for hierarchical posts and replies
CREATE TABLE IF NOT EXISTS interactions (
    id UUID PRIMARY KEY,
    group_id VARCHAR(64) NOT NULL DEFAULT 'root',
    root_id UUID,
    parent_id UUID,
    title VARCHAR(255),
    body TEXT NOT NULL,
    author VARCHAR(100) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ,
    reply_count INT NOT NULL DEFAULT 0,
    depth INT NOT NULL DEFAULT 0
);

-- Index for group feed pagination (root posts only, newest first)
CREATE INDEX IF NOT EXISTS idx_interactions_group_feed 
ON interactions (group_id, created_at DESC) 
WHERE parent_id IS NULL;

-- Index for retrieving an entire conversation thread in chronological order
CREATE INDEX IF NOT EXISTS idx_interactions_thread 
ON interactions (root_id, created_at ASC);
