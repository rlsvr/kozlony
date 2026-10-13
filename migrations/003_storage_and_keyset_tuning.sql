-- Migration 003: Storage optimization for HOT updates and keyset pagination
--
-- 1. Table storage tuning (fillfactor = 85):
-- By default, PostgreSQL packs data pages 100% full. Counter workers frequently
-- update reply_count (WHERE id = $1 AND reply_count < $2). With fillfactor = 85,
-- PostgreSQL leaves 15% free space in each 8KB data page, enabling Heap-Only Tuple (HOT)
-- updates. HOT updates write the new row version into the same page and completely avoid
-- updating index pointers, drastically reducing table bloat, disk I/O, and WAL volume.
ALTER TABLE interactions SET (fillfactor = 85);

-- 2. Keyset pagination index on feed:
-- Under high concurrency where multiple root interactions share the exact same
-- created_at timestamp, include id DESC for deterministic index-supported cursor pagination.
DROP INDEX IF EXISTS idx_interactions_group_feed;
CREATE INDEX IF NOT EXISTS idx_interactions_group_feed
ON interactions (group_id, created_at DESC, id DESC)
WHERE parent_id IS NULL;

-- 3. Autovacuum tuning for high-frequency micro-batch writes:
ALTER TABLE interactions SET (
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.02
);
