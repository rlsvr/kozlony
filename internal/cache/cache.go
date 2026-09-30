package cache

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/eko/gocache/lib/v4/cache"
	"github.com/eko/gocache/lib/v4/store"
	gocachestore "github.com/eko/gocache/store/go_cache/v4"
	"github.com/google/uuid"
	gocache "github.com/patrickmn/go-cache"

	"github.com/rlsvr/kozlony/internal/config"
	"github.com/rlsvr/kozlony/internal/database"
)

// ErrNotFound indicates the item was not found in the cache.
var ErrNotFound = errors.New("interaction not found in cache")

//go:generate go run go.uber.org/mock/mockgen@v0.6.0 -source=cache.go -destination=mocks/mock_cache.go -package=mocks

// InteractionCache abstracts the caching layer for interactions.
// Implementations can be in-memory or distributed (e.g. Redis) via eko/gocache.
type InteractionCache interface {
	Get(ctx context.Context, id uuid.UUID) (*database.Interaction, error)
	Set(ctx context.Context, item *database.Interaction) error
	Delete(ctx context.Context, id uuid.UUID) error
}

// MemoryCache implements InteractionCache using eko/gocache with an in-memory go-cache store.
// Because it wraps eko/gocache, the underlying storage engine can be seamlessly swapped to Redis.
type MemoryCache struct {
	client      *gocache.Cache
	gocacheInst *cache.Cache[*database.Interaction]
	maxCapacity int
	ttl         time.Duration
	mu          sync.Mutex
	keys        []uuid.UUID
}

// NewMemoryCache initializes an in-memory cache managed by eko/gocache with configurable capacity and TTL.
func NewMemoryCache(cfg *config.Config) *MemoryCache {
	ttl := cfg.CacheTTL
	if ttl <= 0 {
		ttl = 1 * time.Hour
	}
	cleanupInterval := 10 * time.Minute

	client := gocache.New(ttl, cleanupInterval)
	storeAdapter := gocachestore.NewGoCache(client)
	cacheManager := cache.New[*database.Interaction](storeAdapter)

	return &MemoryCache{
		client:      client,
		gocacheInst: cacheManager,
		maxCapacity: cfg.MaxCachedInteractions,
		ttl:         ttl,
		keys:        make([]uuid.UUID, 0, 1024),
	}
}

// Get retrieves an interaction from the cache.
func (c *MemoryCache) Get(ctx context.Context, id uuid.UUID) (*database.Interaction, error) {
	item, err := c.gocacheInst.Get(ctx, id.String())
	if err != nil {
		return nil, ErrNotFound
	}
	return item, nil
}

// Set stores an interaction in the cache, evicting the oldest key if capacity is exceeded.
func (c *MemoryCache) Set(ctx context.Context, item *database.Interaction) error {
	if item == nil {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Enforce configured capacity limit (FIFO eviction)
	if c.maxCapacity > 0 && len(c.keys) >= c.maxCapacity {
		oldest := c.keys[0]
		_ = c.gocacheInst.Delete(ctx, oldest.String())
		c.keys = c.keys[1:]
	}

	c.keys = append(c.keys, item.ID)
	return c.gocacheInst.Set(ctx, item.ID.String(), item, store.WithExpiration(c.ttl))
}

// Delete removes an interaction from the cache.
func (c *MemoryCache) Delete(ctx context.Context, id uuid.UUID) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.gocacheInst.Delete(ctx, id.String())
}
