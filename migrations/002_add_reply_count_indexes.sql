-- Migration 002: index for recalculating reply counts
--
-- Recalculating reply_count for the parents a batch touches needs two tallies:
--
--   SELECT root_id,   COUNT(*) ... WHERE root_id   = ANY($1)  -- thread roots
--   SELECT parent_id, COUNT(*) ... WHERE parent_id = ANY($1)  -- intermediate replies
--
-- The first is served by the existing idx_interactions_thread on (root_id, created_at);
-- this index serves the second, which the table otherwise has no index for at all and
-- which would otherwise be a sequential scan of every batch.
CREATE INDEX IF NOT EXISTS idx_interactions_parent_reply_counts
ON interactions (parent_id)
WHERE parent_id IS NOT NULL;