package cache_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/rlsvr/kozlony/internal/cache"
	"github.com/rlsvr/kozlony/internal/config"
	"github.com/rlsvr/kozlony/internal/database"
)

const testGroupRoot = "root"

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
		GroupID:   testGroupRoot,
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

	// 2. Put
	if err := c.Put(ctx, item); err != nil {
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

	// 4. Update
	updatedBody := "Updated body in cache"
	item.Body = updatedBody
	item.Version = 2
	if err := c.Update(ctx, item); err != nil {
		t.Fatalf("failed to update item: %v", err)
	}
	retrieved2, err := c.Get(ctx, id)
	if err != nil {
		t.Fatalf("unexpected error getting updated item: %v", err)
	}
	if retrieved2.Body != updatedBody || retrieved2.Version != 2 {
		t.Errorf("expected updated body %q and version 2, got %q and %d", updatedBody, retrieved2.Body, retrieved2.Version)
	}

	// 5. Delete
	if err := c.Delete(ctx, id); err != nil {
		t.Fatalf("failed to delete item: %v", err)
	}

	// 6. Get after delete
	_, err = c.Get(ctx, id)
	if !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("expected ErrNotFound after deletion, got %v", err)
	}
}

func TestMemoryCache_ThreadRetrievalAndReplies(t *testing.T) {
	c := cache.NewMemoryCache(&config.Config{
		MaxCachedInteractions: 100,
		CacheTTL:              5 * time.Minute,
	})
	ctx := context.Background()

	rootID := uuid.Must(uuid.NewV7())
	rootTitle := "Root Post"
	now := time.Now().UTC()

	root := &database.Interaction{
		ID:        rootID,
		GroupID:   testGroupRoot,
		Title:     &rootTitle,
		Body:      "Root body",
		Author:    "Alice",
		CreatedAt: now,
		Depth:     0,
		Version:   1,
	}

	if err := c.Put(ctx, root); err != nil {
		t.Fatalf("put root: %v", err)
	}

	// Reply 1
	reply1ID := uuid.Must(uuid.NewV7())
	reply1 := &database.Interaction{
		ID:        reply1ID,
		GroupID:   testGroupRoot,
		RootID:    &rootID,
		ParentID:  &rootID,
		Body:      "Reply 1",
		Author:    "Bob",
		CreatedAt: now.Add(1 * time.Second),
		Depth:     1,
		Version:   1,
	}
	if err := c.Put(ctx, reply1); err != nil {
		t.Fatalf("put reply1: %v", err)
	}

	// Reply 2 (nested reply to reply 1)
	reply2ID := uuid.Must(uuid.NewV7())
	reply2 := &database.Interaction{
		ID:        reply2ID,
		GroupID:   testGroupRoot,
		RootID:    &rootID,
		ParentID:  &reply1ID,
		Body:      "Reply 2",
		Author:    "Charlie",
		CreatedAt: now.Add(2 * time.Second),
		Depth:     2,
		Version:   1,
	}
	if err := c.Put(ctx, reply2); err != nil {
		t.Fatalf("put reply2: %v", err)
	}

	// Verify Get individual reply via nodeLookup
	gotR2, err := c.Get(ctx, reply2ID)
	if err != nil {
		t.Fatalf("get reply2: %v", err)
	}
	if gotR2.ID != reply2ID {
		t.Errorf("expected reply2 ID %s, got %s", reply2ID, gotR2.ID)
	}

	// Verify GetThread returns root + both replies in chronological order
	thread, err := c.GetThread(ctx, rootID)
	if err != nil {
		t.Fatalf("get thread: %v", err)
	}
	if len(thread) != 3 {
		t.Fatalf("expected 3 items in thread, got %d", len(thread))
	}
	if thread[0].ID != rootID {
		t.Errorf("expected first item to be root %s, got %s", rootID, thread[0].ID)
	}
	if thread[1].ID != reply1ID {
		t.Errorf("expected second item to be reply1 %s, got %s", reply1ID, thread[1].ID)
	}
	if thread[2].ID != reply2ID {
		t.Errorf("expected third item to be reply2 %s, got %s", reply2ID, thread[2].ID)
	}
}

func TestMemoryCache_FeedPaginationBTree(t *testing.T) {
	c := cache.NewMemoryCache(&config.Config{
		MaxCachedInteractions: 100,
		CacheTTL:              5 * time.Minute,
	})
	ctx := context.Background()

	now := time.Now().UTC()
	var roots []*database.Interaction
	for i := 0; i < 10; i++ {
		title := fmt.Sprintf("Post %d", i)
		r := &database.Interaction{
			ID:        uuid.Must(uuid.NewV7()),
			GroupID:   "board-a",
			Title:     &title,
			Body:      "Post content",
			Author:    "User",
			CreatedAt: now.Add(time.Duration(i) * time.Minute),
			Depth:     0,
			Version:   1,
		}
		roots = append(roots, r)
		if err := c.Put(ctx, r); err != nil {
			t.Fatalf("put post %d: %v", i, err)
		}
	}

	// 1. Fetch first page of 5 (newest first: posts 9, 8, 7, 6, 5)
	page1, covered := c.ListFeed(ctx, "board-a", 5, nil)
	if !covered {
		t.Fatalf("expected page1 to be covered by cache")
	}
	if len(page1) != 5 {
		t.Fatalf("expected 5 items, got %d", len(page1))
	}
	if page1[0].ID != roots[9].ID {
		t.Errorf("expected newest post to be post 9, got %s", page1[0].ID)
	}
	if page1[4].ID != roots[5].ID {
		t.Errorf("expected 5th post to be post 5, got %s", page1[4].ID)
	}

	// 2. Fetch second page of 5 using cursor (cursor = page1[4].CreatedAt)
	cursor := page1[4].CreatedAt
	page2, covered := c.ListFeed(ctx, "board-a", 5, &cursor)
	if !covered {
		t.Fatalf("expected page2 to be covered by cache")
	}
	if len(page2) != 5 {
		t.Fatalf("expected 5 items on page 2, got %d", len(page2))
	}
	if page2[0].ID != roots[4].ID {
		t.Errorf("expected first item on page 2 to be post 4, got %s", page2[0].ID)
	}
	if page2[4].ID != roots[0].ID {
		t.Errorf("expected last item on page 2 to be post 0, got %s", page2[4].ID)
	}
}

func TestMemoryCache_WeightBasedEviction(t *testing.T) {
	// Set capacity to 4 total interactions
	c := cache.NewMemoryCache(&config.Config{
		MaxCachedInteractions: 4,
		CacheTTL:              5 * time.Minute,
	})
	ctx := context.Background()

	now := time.Now().UTC()

	// Thread 1: root + 2 replies (weight = 3)
	t1RootID := uuid.Must(uuid.NewV7())
	t1Title := "T1 Root"
	_ = c.Put(ctx, &database.Interaction{
		ID:        t1RootID,
		GroupID:   testGroupRoot,
		Title:     &t1Title,
		CreatedAt: now,
		Version:   1,
	})
	t1R1ID := uuid.Must(uuid.NewV7())
	_ = c.Put(ctx, &database.Interaction{
		ID:        t1R1ID,
		GroupID:   testGroupRoot,
		RootID:    &t1RootID,
		ParentID:  &t1RootID,
		CreatedAt: now.Add(1 * time.Second),
		Version:   1,
	})
	t1R2ID := uuid.Must(uuid.NewV7())
	_ = c.Put(ctx, &database.Interaction{
		ID:        t1R2ID,
		GroupID:   testGroupRoot,
		RootID:    &t1RootID,
		ParentID:  &t1R1ID,
		CreatedAt: now.Add(2 * time.Second),
		Version:   1,
	})

	if c.TotalWeight() != 3 {
		t.Fatalf("expected weight 3 for thread 1, got %d", c.TotalWeight())
	}

	// Thread 2: root + 1 reply (weight = 2)
	// Adding this will bump total weight to 5, exceeding capacity of 4, triggering eviction of oldest thread (Thread 1)
	t2RootID := uuid.Must(uuid.NewV7())
	t2Title := "T2 Root"
	_ = c.Put(ctx, &database.Interaction{
		ID:        t2RootID,
		GroupID:   testGroupRoot,
		Title:     &t2Title,
		CreatedAt: now.Add(3 * time.Second),
		Version:   1,
	})
	t2R1ID := uuid.Must(uuid.NewV7())
	_ = c.Put(ctx, &database.Interaction{
		ID:        t2R1ID,
		GroupID:   testGroupRoot,
		RootID:    &t2RootID,
		ParentID:  &t2RootID,
		CreatedAt: now.Add(4 * time.Second),
		Version:   1,
	})

	// Total weight must be bounded by capacity
	if c.TotalWeight() > 4 {
		t.Errorf("total weight %d exceeded capacity 4", c.TotalWeight())
	}

	// Thread 2 should still be in cache
	if _, err := c.Get(ctx, t2RootID); err != nil {
		t.Errorf("expected t2 root to be cached: %v", err)
	}
}

func TestMemoryCache_ConcurrentAccess(_ *testing.T) {
	c := cache.NewMemoryCache(&config.Config{
		MaxCachedInteractions: 1000,
		CacheTTL:              5 * time.Minute,
	})
	ctx := context.Background()

	var wg sync.WaitGroup
	workers := 20
	iterations := 50

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				rootID := uuid.Must(uuid.NewV7())
				title := fmt.Sprintf("Root %d-%d", workerID, i)
				root := &database.Interaction{
					ID:        rootID,
					GroupID:   "concurrent",
					Title:     &title,
					CreatedAt: time.Now().UTC(),
					Version:   1,
				}
				_ = c.Put(ctx, root)

				// Lookup root
				_, _ = c.Get(ctx, rootID)

				// Add reply
				replyID := uuid.Must(uuid.NewV7())
				reply := &database.Interaction{
					ID:        replyID,
					GroupID:   "concurrent",
					RootID:    &rootID,
					ParentID:  &rootID,
					CreatedAt: time.Now().UTC(),
					Version:   1,
				}
				_ = c.Put(ctx, reply)

				// Lookup thread
				_, _ = c.GetThread(ctx, rootID)

				// Query feed
				_, _ = c.ListFeed(ctx, "concurrent", 10, nil)
			}
		}(w)
	}

	wg.Wait()
}
