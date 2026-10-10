-- Migration 002: index for recalculating reply counts
--
-- The drainer recalculates reply_count for every parent touched by a batch with a
-- recursive walk down parent_id:
--
--   WITH RECURSIVE tree AS (
--       SELECT c.id FROM targets t JOIN interactions c ON c.parent_id = t.id
--     UNION
--       SELECT c.id FROM tree tr JOIN interactions c ON c.parent_id = tr.id
--   ) ...
--
-- parent_id is the only column that recursion touches, so this index is what keeps the
-- count an index scan per level instead of a sequential scan of the whole table.
CREATE INDEX IF NOT EXISTS idx_interactions_parent_reply_counts
ON interactions (parent_id)
WHERE parent_id IS NOT NULL;