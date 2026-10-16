package cache

import (
	"bytes"
	"sync"
	"time"

	"github.com/google/btree"
	"github.com/google/uuid"

	"github.com/rlsvr/kozlony/internal/database"
)

// FeedItem represents an indexed root post in the B-Tree feed.
// We order items by CreatedAt DESC, then ID DESC to break timestamp ties,
// matching the PostgreSQL `ORDER BY created_at DESC, id DESC` semantics.
type FeedItem struct {
	ID        uuid.UUID
	CreatedAt time.Time
	Item      *database.Interaction
}

// feedItemLess compares two FeedItems for descending chronological order.
// In google/btree:
// If a.CreatedAt is AFTER b.CreatedAt, a comes BEFORE b in the feed (so Less returns true).
// If timestamps are equal, larger ID comes before smaller ID.
func feedItemLess(a, b *FeedItem) bool {
	if a.CreatedAt.Equal(b.CreatedAt) {
		return bytes.Compare(a.ID[:], b.ID[:]) > 0
	}
	return a.CreatedAt.After(b.CreatedAt)
}

// FeedIndex maintains an in-memory B-Tree index for root posts of a single group_id.
type FeedIndex struct {
	mu      sync.RWMutex
	tree    *btree.BTreeG[*FeedItem]
	maxSize int
}

// NewFeedIndex creates a new FeedIndex with the specified capacity limit.
func NewFeedIndex(maxSize int) *FeedIndex {
	if maxSize <= 0 {
		maxSize = 10000
	}
	return &FeedIndex{
		tree:    btree.NewG[*FeedItem](32, feedItemLess),
		maxSize: maxSize,
	}
}

// Insert adds or updates a root post in the feed index.
// If the index exceeds its capacity, the oldest item (tail of the B-Tree) is evicted.
func (f *FeedIndex) Insert(item *database.Interaction) {
	if item == nil || item.ParentID != nil {
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	feedItem := &FeedItem{
		ID:        item.ID,
		CreatedAt: item.CreatedAt,
		Item:      item,
	}
	f.tree.ReplaceOrInsert(feedItem)

	if f.tree.Len() > f.maxSize {
		// Delete the oldest item (largest in our Less definition, which is at the end)
		if maxItem, ok := f.tree.Max(); ok {
			f.tree.Delete(maxItem)
		}
	}
}

// Delete removes a root post from the feed index.
func (f *FeedIndex) Delete(id uuid.UUID, createdAt time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()

	target := &FeedItem{
		ID:        id,
		CreatedAt: createdAt,
	}
	f.tree.Delete(target)
}

// List returns up to `limit` root posts created strictly before `before` (if specified).
// Returns the slice of interactions and a boolean `covered` indicating whether the
// query was satisfied by the in-memory window.
func (f *FeedIndex) List(limit int, before *time.Time) ([]*database.Interaction, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.tree.Len() == 0 {
		return nil, false
	}

	results := make([]*database.Interaction, 0, limit)

	// In google/btree: Ascend iterates in increasing order according to Less (which means newest first).
	f.tree.Ascend(func(item *FeedItem) bool {
		if before != nil && !item.CreatedAt.Before(*before) {
			// Skip items created at or after the cursor
			return true
		}

		results = append(results, item.Item)
		return len(results) < limit
	})

	// If we found the requested number of items, the query is fully covered
	if len(results) == limit {
		return results, true
	}

	// If fewer than limit items were returned:
	// If the feed has fewer items than maxSize, we hold the entire history in memory, so it IS covered.
	// If the feed has reached maxSize, the oldest items were evicted to Postgres, so we cannot guarantee completeness.
	if f.tree.Len() < f.maxSize {
		return results, true
	}

	return results, false
}

// Len returns the number of root items in this feed index.
func (f *FeedIndex) Len() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.tree.Len()
}
