package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
)

// InteractionRef holds metadata of a known interaction to generate child replies or edits.
type InteractionRef struct {
	ID       uuid.UUID  `json:"id"`
	RootID   *uuid.UUID `json:"root_id,omitempty"`
	ParentID *uuid.UUID `json:"parent_id,omitempty"`
	Depth    int        `json:"depth"`
	Version  int        `json:"version"`
	Author   string     `json:"author"`
}

// InteractionPool maintains a thread-safe sliding window pool of active interactions.
type InteractionPool struct {
	mu      sync.RWMutex
	items   []InteractionRef
	byID    map[uuid.UUID]int // maps ID to index in items
	maxSize int
	head    int
}

// NewInteractionPool initializes an empty interaction pool with a maximum capacity.
func NewInteractionPool(maxSize int) *InteractionPool {
	if maxSize <= 0 {
		maxSize = 5000
	}
	return &InteractionPool{
		items:   make([]InteractionRef, 0, maxSize),
		byID:    make(map[uuid.UUID]int, maxSize),
		maxSize: maxSize,
	}
}

// Add appends a new interaction to the pool, evicting the oldest in O(1) if capacity is exceeded.
func (p *InteractionPool) Add(item InteractionRef) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if idx, exists := p.byID[item.ID]; exists {
		p.items[idx] = item
		return
	}

	if len(p.items) < p.maxSize {
		p.byID[item.ID] = len(p.items)
		p.items = append(p.items, item)
		return
	}

	// Ring buffer eviction in O(1) without re-indexing
	evictIdx := p.head
	p.head = (p.head + 1) % p.maxSize

	oldest := p.items[evictIdx]
	delete(p.byID, oldest.ID)

	p.items[evictIdx] = item
	p.byID[item.ID] = evictIdx
}

// UpdateVersion updates the version counter of an interaction in the pool.
func (p *InteractionPool) UpdateVersion(id uuid.UUID, newVersion int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if idx, exists := p.byID[id]; exists {
		p.items[idx].Version = newVersion
	}
}

// GetRandomParent returns a random interaction to attach a reply to using an RLock.
func (p *InteractionPool) GetRandomParent() (InteractionRef, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	n := len(p.items)
	if n == 0 {
		return InteractionRef{}, false
	}
	idx := rand.Intn(n) //nolint:gosec
	return p.items[idx], true
}

// GetRandomRoot returns a random root post UUID from the pool if available.
func (p *InteractionPool) GetRandomRoot() (uuid.UUID, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	n := len(p.items)
	if n == 0 {
		return uuid.Nil, false
	}

	// Try a few random picks to find a root (depth == 0 or root_id == nil)
	for attempts := 0; attempts < 10; attempts++ {
		idx := rand.Intn(n) //nolint:gosec
		item := p.items[idx]
		if item.Depth == 0 || item.RootID == nil {
			return item.ID, true
		}
		if item.RootID != nil {
			return *item.RootID, true
		}
	}

	return p.items[0].ID, true
}

// GetRandomForEdit returns a random interaction for optimistic editing using an RLock.
func (p *InteractionPool) GetRandomForEdit() (InteractionRef, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	n := len(p.items)
	if n == 0 {
		return InteractionRef{}, false
	}
	idx := rand.Intn(n) //nolint:gosec
	return p.items[idx], true
}

// Size returns the current number of interactions stored in the pool.
func (p *InteractionPool) Size() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.items)
}

type feedResponse struct {
	Interactions []struct {
		ID       uuid.UUID  `json:"id"`
		RootID   *uuid.UUID `json:"root_id"`
		ParentID *uuid.UUID `json:"parent_id"`
		Depth    int        `json:"depth"`
		Version  int        `json:"version"`
		Author   string     `json:"author"`
	} `json:"interactions"`
}

// SeedFromFeed fetches existing interactions from the API feed to populate the pool at startup.
func (p *InteractionPool) SeedFromFeed(ctx context.Context, apiURL, groupID string) error {
	client := &http.Client{Timeout: 5 * time.Second}
	reqURL := fmt.Sprintf("%s/v1/interactions?group_id=%s&limit=50", apiURL, groupID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected feed status %d", resp.StatusCode)
	}

	var feed feedResponse
	if err := json.NewDecoder(resp.Body).Decode(&feed); err != nil {
		return err
	}

	for _, item := range feed.Interactions {
		p.Add(InteractionRef{
			ID:       item.ID,
			RootID:   item.RootID,
			ParentID: item.ParentID,
			Depth:    item.Depth,
			Version:  item.Version,
			Author:   item.Author,
		})
	}

	return nil
}
