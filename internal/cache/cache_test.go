package cache_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/rlsvr/kozlony/internal/cache"
	"github.com/rlsvr/kozlony/internal/config"
	"github.com/rlsvr/kozlony/internal/database"
)

func TestMemoryCache_BasicOperations(t *testing.T) {
	cfg := &config.Config{
		MaxCachedInteractions: 10,
		CacheTTL:              5 * time.Minute,
	}
	c := cache.NewMemoryCache(cfg)
	ctx := context.Background()

	id := uuid.Must(uuid.NewV7())
	title := "Cached Post"
	item := &database.Interaction{
		ID:        id,
		GroupID:   "root",
		Title:     &title,
		Body:      "Body in cache",
		Author:    "Alice",
		CreatedAt: time.Now().UTC(),
		Version:   1,
	}

	// 1. Get non-existent
	_, err := c.Get(ctx, id)
	if !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	// 2. Set
	if err := c.Set(ctx, item); err != nil {
		t.Fatalf("failed to set item in cache: %v", err)
	}

	// 3. Get existing
	retrieved, err := c.Get(ctx, id)
	if err != nil {
		t.Fatalf("unexpected error getting item: %v", err)
	}
	if retrieved.ID != id {
		t.Errorf("expected ID %s, got %s", id, retrieved.ID)
	}
	if retrieved.Title == nil || *retrieved.Title != title {
		t.Errorf("expected title %s, got %v", title, retrieved.Title)
	}

	// 4. Delete
	if err := c.Delete(ctx, id); err != nil {
		t.Fatalf("failed to delete item: %v", err)
	}

	// 5. Get after delete
	_, err = c.Get(ctx, id)
	if !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("expected ErrNotFound after deletion, got %v", err)
	}
}

func TestMemoryCache_CapacityEviction(t *testing.T) {
	capacity := 3
	cfg := &config.Config{
		MaxCachedInteractions: capacity,
		CacheTTL:              5 * time.Minute,
	}
	c := cache.NewMemoryCache(cfg)
	ctx := context.Background()

	var ids []uuid.UUID
	for i := 0; i < 4; i++ {
		id := uuid.Must(uuid.NewV7())
		ids = append(ids, id)
		err := c.Set(ctx, &database.Interaction{
			ID:        id,
			GroupID:   "root",
			Body:      "item",
			Author:    "Tester",
			CreatedAt: time.Now().UTC(),
			Version:   1,
		})
		if err != nil {
			t.Fatalf("failed to set item %d: %v", i, err)
		}
	}

	// Oldest item (ids[0]) should have been evicted
	_, err := c.Get(ctx, ids[0])
	if !errors.Is(err, cache.ErrNotFound) {
		t.Errorf("expected oldest item %s to be evicted, got err=%v", ids[0], err)
	}

	// Newer items should still be in cache
	for i := 1; i < 4; i++ {
		item, err := c.Get(ctx, ids[i])
		if err != nil {
			t.Errorf("expected item %d (%s) to be present, got %v", i, ids[i], err)
		}
		if item != nil && item.ID != ids[i] {
			t.Errorf("expected item ID %s, got %s", ids[i], item.ID)
		}
	}
}
