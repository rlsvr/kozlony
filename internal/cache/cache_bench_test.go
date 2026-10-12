package cache_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/rlsvr/kozlony/internal/cache"
	"github.com/rlsvr/kozlony/internal/config"
	"github.com/rlsvr/kozlony/internal/database"
)

const testGroupBench = "bench"

// BenchmarkShardedCache_Get measures individual node lookup latency via nodeLookup table.
func BenchmarkShardedCache_Get(b *testing.B) {
	c := cache.NewMemoryCache(&config.Config{MaxCachedInteractions: 100000})
	ctx := context.Background()

	id := uuid.Must(uuid.NewV7())
	title := "Bench Post"
	_ = c.Put(ctx, &database.Interaction{
		ID:        id,
		GroupID:   testGroupBench,
		Title:     &title,
		Body:      "Bench body",
		CreatedAt: time.Now().UTC(),
		Version:   1,
	})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = c.Get(ctx, id)
	}
}

// BenchmarkShardedCache_GetThread measures retrieving a full thread (root + 10 replies) in chronological order.
func BenchmarkShardedCache_GetThread(b *testing.B) {
	c := cache.NewMemoryCache(&config.Config{MaxCachedInteractions: 100000})
	ctx := context.Background()

	rootID := uuid.Must(uuid.NewV7())
	title := "Bench Thread Root"
	now := time.Now().UTC()
	_ = c.Put(ctx, &database.Interaction{
		ID:        rootID,
		GroupID:   testGroupBench,
		Title:     &title,
		Body:      "Root body",
		CreatedAt: now,
		Version:   1,
	})

	for i := 0; i < 10; i++ {
		replyID := uuid.Must(uuid.NewV7())
		_ = c.Put(ctx, &database.Interaction{
			ID:        replyID,
			GroupID:   testGroupBench,
			RootID:    &rootID,
			ParentID:  &rootID,
			Body:      "Reply body",
			CreatedAt: now.Add(time.Duration(i+1) * time.Second),
			Version:   1,
		})
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = c.GetThread(ctx, rootID)
	}
}

// BenchmarkShardedCache_ListFeed_Page measures in-memory B-Tree pagination of 20 items with a cursor.
func BenchmarkShardedCache_ListFeed_Page(b *testing.B) {
	c := cache.NewMemoryCache(&config.Config{MaxCachedInteractions: 100000})
	ctx := context.Background()

	now := time.Now().UTC()
	for i := 0; i < 1000; i++ {
		id := uuid.Must(uuid.NewV7())
		title := fmt.Sprintf("Post %d", i)
		_ = c.Put(ctx, &database.Interaction{
			ID:        id,
			GroupID:   testGroupBench,
			Title:     &title,
			Body:      "Body",
			CreatedAt: now.Add(time.Duration(i) * time.Second),
			Version:   1,
		})
	}

	cursor := now.Add(500 * time.Second)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = c.ListFeed(ctx, testGroupBench, 20, &cursor)
	}
}

// BenchmarkShardedCache_PutParallel measures concurrent writes and promotions across shards.
func BenchmarkShardedCache_PutParallel(b *testing.B) {
	c := cache.NewMemoryCache(&config.Config{MaxCachedInteractions: 500000})
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := uuid.Must(uuid.NewV7())
			_ = c.Put(ctx, &database.Interaction{
				ID:        id,
				GroupID:   testGroupBench,
				Body:      "Parallel body",
				CreatedAt: time.Now().UTC(),
				Version:   1,
			})
		}
	})
}
