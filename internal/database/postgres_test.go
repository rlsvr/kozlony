package database_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/rlsvr/kozlony/internal/database"
)

func TestConnectInvalidURL(t *testing.T) {
	_, err := database.Connect(context.Background(), "invalid://localhost:not-a-port")
	if err == nil {
		t.Fatal("expected error connecting with invalid database URL, got nil")
	}
}

func TestInsertBatchEmpty(t *testing.T) {
	repo := database.NewInteractionRepository(nil)
	if err := repo.InsertBatch(context.Background(), nil); err != nil {
		t.Fatalf("expected nil error for empty batch, got: %v", err)
	}
	if err := repo.InsertBatch(context.Background(), []*database.Interaction{}); err != nil {
		t.Fatalf("expected nil error for empty slice, got: %v", err)
	}
}

// liveRepo connects to a real Postgres and skips when none is reachable, mirroring how
// the blackbox suite treats an absent server. The descendant-count SQL cannot be verified
// against a mock, so this is where its semantics are actually pinned down.
func liveRepo(t *testing.T) *database.InteractionRepository {
	t.Helper()

	url := os.Getenv("DATABASE_URL")
	if url == "" {
		// Local development default only; override with DATABASE_URL in CI.
		url = "postgres://postgres:postgres@localhost:5433/messageboard?sslmode=disable" // #nosec G101
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := database.ConnectWithLimits(ctx, url, 4, 1)
	if err != nil {
		t.Skipf("no live postgres, skipping: %v", err)
	}
	t.Cleanup(pool.Close)

	repo := database.NewInteractionRepository(pool)
	if _, err := pool.Exec(ctx, "TRUNCATE interactions;"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return repo
}

const testGroupID = "root"

func mkInteraction(id uuid.UUID, parent, root *uuid.UUID) *database.Interaction {
	return &database.Interaction{
		ID:        id,
		GroupID:   testGroupID,
		ParentID:  parent,
		RootID:    root,
		Title:     strPtr("t"),
		Body:      "b",
		Author:    "tester",
		CreatedAt: time.Now().UTC(),
		Depth:     1,
		Version:   1,
	}
}

func strPtr(s string) *string { return &s }

// reply_count means total descendants at every depth. A non-recursive count gets this
// wrong: keying on root_id alone reports nothing for an intermediate reply, and layering
// a parent_id tally on top double-counts the top level of a subtree.
func TestCountDescendants_AllDescendants(t *testing.T) {
	repo := liveRepo(t)
	ctx := context.Background()

	root := uuid.Must(uuid.NewV7())
	a := uuid.Must(uuid.NewV7())
	a1 := uuid.Must(uuid.NewV7())
	a1a := uuid.Must(uuid.NewV7())
	b := uuid.Must(uuid.NewV7())

	rootItem := mkInteraction(root, nil, nil)
	rootItem.Depth = 0
	items := []*database.Interaction{
		rootItem,
		mkInteraction(a, &root, &root),
		mkInteraction(a1, &a, &root),
		mkInteraction(a1a, &a1, &root),
		mkInteraction(b, &root, &root),
	}
	if err := repo.InsertBatch(ctx, items); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	counts, err := repo.CountDescendants(ctx, []uuid.UUID{root, a, b})
	if err != nil {
		t.Fatalf("count descendants: %v", err)
	}

	// root has A, B, A1, A1a; A has A1, A1a; B has none.
	for id, want := range map[uuid.UUID]int{root: 4, a: 2, b: 0} {
		if got := counts[id]; got != want {
			t.Errorf("descendants for %s: got %d, want %d", id, got, want)
		}
	}
}

// The batch must count its own uncommitted rows, otherwise a fresh reply reports zero and
// the count only becomes right on some later, unrelated batch.
func TestInsertBatchWithCounts_SeesOwnUncommittedRows(t *testing.T) {
	repo := liveRepo(t)
	ctx := context.Background()

	root := uuid.Must(uuid.NewV7())
	rootItem := mkInteraction(root, nil, nil)
	rootItem.Depth = 0
	if err := repo.InsertBatch(ctx, []*database.Interaction{rootItem}); err != nil {
		t.Fatalf("insert root: %v", err)
	}

	replyA := uuid.Must(uuid.NewV7())
	replyB := uuid.Must(uuid.NewV7())
	counts, err := repo.InsertBatchWithCounts(ctx, []*database.Interaction{
		mkInteraction(replyA, &root, &root),
		mkInteraction(replyB, &root, &root),
	})
	if err != nil {
		t.Fatalf("insert batch with counts: %v", err)
	}

	if got := counts[root]; got != 2 {
		t.Errorf("reply_count for root: got %d, want 2 (batch must count its own inserts)", got)
	}
}

// Redelivery safety rests entirely on storing absolute totals. Applying the same batch
// twice must leave the value untouched rather than doubling it.
func TestApplyReplyCounts_IsIdempotent(t *testing.T) {
	repo := liveRepo(t)
	ctx := context.Background()

	root := uuid.Must(uuid.NewV7())
	rootItem := mkInteraction(root, nil, nil)
	rootItem.Depth = 0
	if err := repo.InsertBatch(ctx, []*database.Interaction{rootItem}); err != nil {
		t.Fatalf("insert root: %v", err)
	}

	if err := repo.ApplyReplyCounts(ctx, map[uuid.UUID]int{root: 5}); err != nil {
		t.Fatalf("apply counts: %v", err)
	}
	for range 3 {
		if err := repo.ApplyReplyCounts(ctx, map[uuid.UUID]int{root: 5}); err != nil {
			t.Fatalf("re-apply counts: %v", err)
		}
	}

	got, err := repo.GetByID(ctx, root)
	if err != nil {
		t.Fatalf("get root: %v", err)
	}
	if got.ReplyCount != 5 {
		t.Errorf("reply_count after repeated identical applies: got %d, want 5", got.ReplyCount)
	}
}

// A delayed event must never walk the count backwards.
func TestApplyReplyCounts_Monotonic(t *testing.T) {
	repo := liveRepo(t)
	ctx := context.Background()

	root := uuid.Must(uuid.NewV7())
	rootItem := mkInteraction(root, nil, nil)
	rootItem.Depth = 0
	if err := repo.InsertBatch(ctx, []*database.Interaction{rootItem}); err != nil {
		t.Fatalf("insert root: %v", err)
	}

	if err := repo.ApplyReplyCounts(ctx, map[uuid.UUID]int{root: 9}); err != nil {
		t.Fatalf("apply counts: %v", err)
	}
	// A stale, smaller total arriving late must be ignored.
	if err := repo.ApplyReplyCounts(ctx, map[uuid.UUID]int{root: 3}); err != nil {
		t.Fatalf("apply stale counts: %v", err)
	}

	got, err := repo.GetByID(ctx, root)
	if err != nil {
		t.Fatalf("get root: %v", err)
	}
	if got.ReplyCount != 9 {
		t.Errorf("stale lower count overwrote a newer one: got %d, want 9", got.ReplyCount)
	}
}

// reply_count must not be written by the insert path any more. That was the source of the
// lock contention this change removes.
func TestInsertBatch_DoesNotWriteReplyCount(t *testing.T) {
	repo := liveRepo(t)
	ctx := context.Background()

	root := uuid.Must(uuid.NewV7())
	rootItem := mkInteraction(root, nil, nil)
	rootItem.Depth = 0
	if err := repo.InsertBatch(ctx, []*database.Interaction{rootItem}); err != nil {
		t.Fatalf("insert root: %v", err)
	}

	if err := repo.InsertBatch(ctx, []*database.Interaction{
		mkInteraction(uuid.Must(uuid.NewV7()), &root, &root),
		mkInteraction(uuid.Must(uuid.NewV7()), &root, &root),
	}); err != nil {
		t.Fatalf("insert replies: %v", err)
	}

	got, err := repo.GetByID(ctx, root)
	if err != nil {
		t.Fatalf("get root: %v", err)
	}
	if got.ReplyCount != 0 {
		t.Errorf("InsertBatch wrote reply_count = %d, expected it to be left for the counter consumer", got.ReplyCount)
	}
}

func TestInteractionModel(t *testing.T) {
	id := uuid.Must(uuid.NewV7())
	title := "Test Interaction"
	now := time.Now().UTC()

	item := database.Interaction{
		ID:         id,
		GroupID:    "root",
		Title:      &title,
		Body:       "Body text",
		Author:     "Bob",
		CreatedAt:  now,
		ReplyCount: 0,
		Depth:      0,
		Version:    1,
	}

	if item.ID != id {
		t.Errorf("expected ID %v, got %v", id, item.ID)
	}
	if item.GroupID != "root" {
		t.Errorf("expected group root, got %s", item.GroupID)
	}
	if *item.Title != title {
		t.Errorf("expected title %s, got %s", title, *item.Title)
	}
	if item.Version != 1 {
		t.Errorf("expected version 1, got %d", item.Version)
	}
}
