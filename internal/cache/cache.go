package cache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"

	"github.com/rlsvr/kozlony/internal/config"
	"github.com/rlsvr/kozlony/internal/database"
)

// ErrNotFound indicates the item was not found in the cache.
var ErrNotFound = errors.New("interaction not found in cache")

// numShards is the number of independent LRU shards to distribute lock contention.
const numShards = 64

//go:generate go run go.uber.org/mock/mockgen@v0.6.0 -source=cache.go -destination=mocks/mock_cache.go -package=mocks

// InteractionCache abstracts the thread-based, sharded caching layer.
type InteractionCache interface {
	// Get retrieves an individual interaction by ID (whether root or reply).
	Get(ctx context.Context, id uuid.UUID) (*database.Interaction, error)

	// GetThread retrieves the full conversation thread for a root ID (root + all replies in order).
	GetThread(ctx context.Context, rootID uuid.UUID) ([]*database.Interaction, error)

	// Put stores an interaction (root or reply) in memory, adding to its thread and feed index.
	Put(ctx context.Context, item *database.Interaction) error

	// PutThread stores an entire conversation thread in memory.
	PutThread(ctx context.Context, rootID uuid.UUID, items []*database.Interaction) error

	// ListFeed retrieves root posts for a given group using in-memory B-Tree pagination.
	// Returns interactions and a boolean indicating whether the query was fully satisfied by cache.
	ListFeed(ctx context.Context, groupID string, limit int, before *time.Time) ([]*database.Interaction, bool)

	// Update updates an interaction's body, title, version, etc. across thread and feed index.
	Update(ctx context.Context, item *database.Interaction) error

	// UpdateReplyCount updates the cached reply_count for an interaction.
	UpdateReplyCount(ctx context.Context, id uuid.UUID, count int) error

	// Delete removes an interaction / thread.
	Delete(ctx context.Context, id uuid.UUID) error

	// TotalWeight returns the total number of cached interactions.
	TotalWeight() int
}

// ShardedMemoryCache implements InteractionCache using sharded LRUs and google/btree feed indexes.
type ShardedMemoryCache struct {
	shards      [numShards]*LRUShard
	nodeLookup  sync.Map // uuid.UUID -> uuid.UUID (nodeID -> rootID)
	feedIndices sync.Map // string -> *FeedIndex (groupID -> *FeedIndex)
	maxCapacity int
	ttl         time.Duration
	totalWeight atomic.Int64
}

// NewMemoryCache initializes a thread-based sharded LRU cache with B-Tree feed pagination.
func NewMemoryCache(cfg *config.Config) *ShardedMemoryCache {
	maxCap := 500000
	ttl := 1 * time.Hour
	if cfg != nil {
		if cfg.MaxCachedInteractions > 0 {
			maxCap = cfg.MaxCachedInteractions
		}
		if cfg.CacheTTL > 0 {
			ttl = cfg.CacheTTL
		}
	}

	c := &ShardedMemoryCache{
		maxCapacity: maxCap,
		ttl:         ttl,
	}

	for i := 0; i < numShards; i++ {
		c.shards[i] = NewLRUShard()
	}

	return c
}

// shardFor computes the shard index for a root UUID using fast xxhash.
func (c *ShardedMemoryCache) shardFor(rootID uuid.UUID) *LRUShard {
	h := xxhash.Sum64(rootID[:])
	return c.shards[h%numShards]
}

// getFeedIndex returns or creates a FeedIndex for a groupID.
func (c *ShardedMemoryCache) getFeedIndex(groupID string) *FeedIndex {
	if groupID == "" {
		groupID = "root"
	}
	if val, ok := c.feedIndices.Load(groupID); ok {
		return val.(*FeedIndex)
	}
	// Default feed index size: 10,000 root posts per group
	idx := NewFeedIndex(10000)
	actual, _ := c.feedIndices.LoadOrStore(groupID, idx)
	return actual.(*FeedIndex)
}

// Get retrieves an individual interaction by ID.
func (c *ShardedMemoryCache) Get(_ context.Context, id uuid.UUID) (*database.Interaction, error) {
	rootVal, ok := c.nodeLookup.Load(id)
	if !ok {
		return nil, ErrNotFound
	}
	rootID := rootVal.(uuid.UUID)

	shard := c.shardFor(rootID)
	thread, found := shard.GetThread(rootID)
	if !found {
		c.nodeLookup.Delete(id)
		return nil, ErrNotFound
	}

	node, foundNode := thread.GetNode(id)
	if !foundNode {
		c.nodeLookup.Delete(id)
		return nil, ErrNotFound
	}
	return node, nil
}

// GetThread retrieves all interactions in a thread sorted chronologically.
func (c *ShardedMemoryCache) GetThread(_ context.Context, rootID uuid.UUID) ([]*database.Interaction, error) {
	shard := c.shardFor(rootID)
	interactions, found := shard.GetThreadInteractions(rootID)
	if !found {
		return nil, ErrNotFound
	}

	return interactions, nil
}

func (c *ShardedMemoryCache) putRoot(item *database.Interaction) {
	rootID := item.ID
	shard := c.shardFor(rootID)
	_, delta := shard.PutRoot(item)
	c.nodeLookup.Store(item.ID, rootID)
	c.getFeedIndex(item.GroupID).Insert(item)
	c.addWeight(delta)
}

func (c *ShardedMemoryCache) putReply(item *database.Interaction) {
	var rootID uuid.UUID
	if item.RootID != nil {
		rootID = *item.RootID
	} else if rVal, ok := c.nodeLookup.Load(*item.ParentID); ok {
		rootID = rVal.(uuid.UUID)
	} else {
		return
	}

	shard := c.shardFor(rootID)
	_, delta := shard.PutReply(rootID, item)
	if delta > 0 {
		c.nodeLookup.Store(item.ID, rootID)
		c.addWeight(delta)
	}
}

// Put stores an interaction (root or reply) in memory.
func (c *ShardedMemoryCache) Put(_ context.Context, item *database.Interaction) error {
	if item == nil {
		return nil
	}

	if item.ParentID == nil {
		c.putRoot(item)
	} else {
		c.putReply(item)
	}

	c.enforceCapacity()
	return nil
}

// PutThread warms a full conversation thread in memory.
func (c *ShardedMemoryCache) PutThread(_ context.Context, rootID uuid.UUID, items []*database.Interaction) error {
	if len(items) == 0 {
		return nil
	}

	shard := c.shardFor(rootID)
	thread, delta := shard.PutFullThread(rootID, items)
	if thread == nil {
		return nil
	}

	// Index all nodes
	if thread.Root != nil {
		c.nodeLookup.Store(thread.Root.ID, rootID)
		c.getFeedIndex(thread.GroupID).Insert(thread.Root)
	}
	for id := range thread.Replies {
		c.nodeLookup.Store(id, rootID)
	}

	c.addWeight(delta)
	c.enforceCapacity()
	return nil
}

// ListFeed retrieves root posts for a given group using in-memory B-Tree pagination.
func (c *ShardedMemoryCache) ListFeed(_ context.Context, groupID string, limit int, before *time.Time) ([]*database.Interaction, bool) {
	idx := c.getFeedIndex(groupID)
	return idx.List(limit, before)
}

// Update updates an interaction in place (thread + feed).
func (c *ShardedMemoryCache) Update(_ context.Context, item *database.Interaction) error {
	if item == nil {
		return nil
	}

	rootVal, ok := c.nodeLookup.Load(item.ID)
	if !ok {
		return ErrNotFound
	}
	rootID := rootVal.(uuid.UUID)

	shard := c.shardFor(rootID)
	if updated := shard.UpdateNode(rootID, item); !updated {
		return ErrNotFound
	}

	if item.ParentID == nil {
		c.getFeedIndex(item.GroupID).Insert(item)
	}
	return nil
}

// UpdateReplyCount updates the cached reply_count for an interaction.
func (c *ShardedMemoryCache) UpdateReplyCount(_ context.Context, id uuid.UUID, count int) error {
	rootVal, ok := c.nodeLookup.Load(id)
	if !ok {
		return nil
	}
	rootID := rootVal.(uuid.UUID)

	shard := c.shardFor(rootID)
	shard.UpdateReplyCount(rootID, id, count)
	return nil
}

// Delete removes an interaction or entire thread.
func (c *ShardedMemoryCache) Delete(_ context.Context, id uuid.UUID) error {
	rootVal, ok := c.nodeLookup.Load(id)
	if !ok {
		return nil
	}
	rootID := rootVal.(uuid.UUID)

	shard := c.shardFor(rootID)
	thread, weight := shard.Delete(rootID)
	if thread != nil {
		c.cleanupThread(thread)
		c.totalWeight.Add(int64(-weight))
	}
	return nil
}

// TotalWeight returns the total number of cached interactions across all threads.
func (c *ShardedMemoryCache) TotalWeight() int {
	return int(c.totalWeight.Load())
}

func (c *ShardedMemoryCache) addWeight(delta int) {
	if delta != 0 {
		c.totalWeight.Add(int64(delta))
	}
}

// enforceCapacity checks if totalWeight exceeds maxCapacity and evicts the globally oldest LRU thread across shards.
func (c *ShardedMemoryCache) enforceCapacity() {
	if c.maxCapacity <= 0 {
		return
	}

	for c.totalWeight.Load() > int64(c.maxCapacity) {
		var oldestThread *Thread
		var oldestShard *LRUShard

		// Find the globally oldest tail among all shards
		for _, s := range c.shards {
			s.mu.RLock()
			tail := s.tail
			if tail != nil {
				if oldestThread == nil || tail.LastAccess.Before(oldestThread.LastAccess) {
					oldestThread = tail
					oldestShard = s
				}
			}
			s.mu.RUnlock()
		}

		if oldestShard == nil {
			break
		}

		// Pop the oldest from the identified shard
		evicted := oldestShard.PopOldest()
		if evicted != nil {
			c.cleanupThread(evicted)
			c.totalWeight.Add(int64(-evicted.Weight))
		} else {
			break
		}
	}
}

// cleanupThread removes all lookup mappings and feed entries for an evicted thread.
func (c *ShardedMemoryCache) cleanupThread(thread *Thread) {
	if thread.Root != nil {
		c.nodeLookup.Delete(thread.Root.ID)
		if idxVal, ok := c.feedIndices.Load(thread.GroupID); ok {
			idxVal.(*FeedIndex).Delete(thread.Root.ID, thread.Root.CreatedAt)
		}
	}
	for replyID := range thread.Replies {
		c.nodeLookup.Delete(replyID)
	}
}
