package database_test

import (
	"context"
	"errors"
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
	if _, err := pool.Exec(ctx, "TRUNCATE interactions, interaction_update_keys;"); err != nil {
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

// reply_count has two meanings by design: a thread root carries its whole conversation
// size, an intermediate reply carries its direct reply count. A uniform "all descendants at
// every depth" figure would need every ancestor recounted on every insert, and any
// ancestor the drainer does not name would keep a stale number forever.
func TestCountDescendants_RootAndIntermediateSemantics(t *testing.T) {
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

	// A thread root counts every reply in the conversation; an intermediate reply counts
	// only its direct replies. Those are exactly the nodes a new reply can change, so
	// recounting only the immediate parent and the root keeps every count accurate.
	for id, want := range map[uuid.UUID]int{root: 4, a: 1, b: 0} {
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
	counts, missing, err := repo.InsertBatchWithCounts(ctx, []*database.Interaction{
		mkInteraction(replyA, &root, &root),
		mkInteraction(replyB, &root, &root),
	})
	if err != nil {
		t.Fatalf("insert batch with counts: %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("expected every parent to be visible, got %d missing", len(missing))
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

	if missing, err := repo.ApplyReplyCounts(ctx, map[uuid.UUID]int{root: 5}); err != nil {
		t.Fatalf("apply counts: %v", err)
	} else if len(missing) != 0 {
		t.Errorf("reported %d missing ids for an existing interaction", len(missing))
	}
	for range 3 {
		if _, err := repo.ApplyReplyCounts(ctx, map[uuid.UUID]int{root: 5}); err != nil {
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

	if _, err := repo.ApplyReplyCounts(ctx, map[uuid.UUID]int{root: 9}); err != nil {
		t.Fatalf("apply counts: %v", err)
	}
	// A stale, smaller total arriving late must be ignored.
	missing, err := repo.ApplyReplyCounts(ctx, map[uuid.UUID]int{root: 3})
	if err != nil {
		t.Fatalf("apply stale counts: %v", err)
	}
	if len(missing) != 0 {
		t.Error("an existing interaction must never be reported as missing, or it would be retried forever")
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

// The bug this fixes: a client retries an edit after losing the response. Its own edit
// already succeeded, so the version has moved on and it gets a 409 it cannot tell apart
// from a real concurrent edit.
func TestUpdateWithIdempotency_RetryReplaysOutcome(t *testing.T) {
	repo := liveRepo(t)
	ctx := context.Background()

	id := uuid.Must(uuid.NewV7())
	rootItem := mkInteraction(id, nil, nil)
	rootItem.Depth = 0
	if err := repo.InsertBatch(ctx, []*database.Interaction{rootItem}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	body := "edited once"
	item, replayed, err := repo.UpdateWithIdempotency(ctx, id, 1, nil, body, "key-1")
	if err != nil {
		t.Fatalf("first update: %v", err)
	}
	if replayed {
		t.Error("the first application of a key must not report a replay")
	}
	if item.Version != 2 {
		t.Errorf("expected version 2 after first update, got %d", item.Version)
	}

	// Same key, same stale expectedVersion: this is the retry.
	again, replayed, err := repo.UpdateWithIdempotency(ctx, id, 1, nil, body, "key-1")
	if err != nil {
		t.Fatalf("retry should replay, got error: %v", err)
	}
	if !replayed {
		t.Error("expected the retry to be recognised as a replay")
	}
	if again.Version != item.Version {
		t.Errorf("replay returned version %d, original was %d", again.Version, item.Version)
	}
	if again.Body != body {
		t.Errorf("replay returned body %q, original was %q", again.Body, body)
	}
}

// A replay must not increment the version again, or every retry would churn the row.
func TestUpdateWithIdempotency_ReplayDoesNotBumpVersion(t *testing.T) {
	repo := liveRepo(t)
	ctx := context.Background()

	id := uuid.Must(uuid.NewV7())
	rootItem := mkInteraction(id, nil, nil)
	rootItem.Depth = 0
	if err := repo.InsertBatch(ctx, []*database.Interaction{rootItem}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	if _, _, err := repo.UpdateWithIdempotency(ctx, id, 1, nil, "first", "k"); err != nil {
		t.Fatalf("first update: %v", err)
	}
	for range 5 {
		if _, _, err := repo.UpdateWithIdempotency(ctx, id, 1, nil, "first", "k"); err != nil {
			t.Fatalf("retry: %v", err)
		}
	}

	got, err := repo.GetByID(ctx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Version != 2 {
		t.Errorf("version drifted to %d after repeated retries, want 2", got.Version)
	}
}

// A different key is a genuinely new edit attempt and must still hit the version check.
func TestUpdateWithIdempotency_DistinctKeysStillConflict(t *testing.T) {
	repo := liveRepo(t)
	ctx := context.Background()

	id := uuid.Must(uuid.NewV7())
	rootItem := mkInteraction(id, nil, nil)
	rootItem.Depth = 0
	if err := repo.InsertBatch(ctx, []*database.Interaction{rootItem}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	if _, _, err := repo.UpdateWithIdempotency(ctx, id, 1, nil, "first", "key-a"); err != nil {
		t.Fatalf("first update: %v", err)
	}

	// Same stale version, different key: a real concurrent edit, so 409 is correct.
	_, replayed, err := repo.UpdateWithIdempotency(ctx, id, 1, nil, "second", "key-b")
	if !errors.Is(err, database.ErrVersionConflict) {
		t.Fatalf("expected ErrVersionConflict for a distinct key, got %v (replayed=%v)", err, replayed)
	}
}

// Without a key the behaviour must be identical to Update, including the conflict.
func TestUpdateWithIdempotency_NoKeyBehavesLikeUpdate(t *testing.T) {
	repo := liveRepo(t)
	ctx := context.Background()

	id := uuid.Must(uuid.NewV7())
	rootItem := mkInteraction(id, nil, nil)
	rootItem.Depth = 0
	if err := repo.InsertBatch(ctx, []*database.Interaction{rootItem}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	if _, _, err := repo.UpdateWithIdempotency(ctx, id, 1, nil, "a", ""); err != nil {
		t.Fatalf("keyless update: %v", err)
	}
	if _, _, err := repo.UpdateWithIdempotency(ctx, id, 1, nil, "b", ""); !errors.Is(err, database.ErrVersionConflict) {
		t.Fatalf("expected a conflict without an idempotency key, got %v", err)
	}
}

func TestUpdateWithIdempotency_UnknownInteraction(t *testing.T) {
	repo := liveRepo(t)

	_, _, err := repo.UpdateWithIdempotency(context.Background(), uuid.Must(uuid.NewV7()), 1, nil, "b", "k")
	if !errors.Is(err, database.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

// A parent whose row has not committed yet must be reported as missing so the caller
// retries, and must not be reported once it exists, or it would retry forever.
func TestApplyReplyCounts_ReportsMissingParent(t *testing.T) {
	repo := liveRepo(t)
	ctx := context.Background()

	existing := uuid.Must(uuid.NewV7())
	rootItem := mkInteraction(existing, nil, nil)
	rootItem.Depth = 0
	if err := repo.InsertBatch(ctx, []*database.Interaction{rootItem}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	missing := uuid.Must(uuid.NewV7())
	pending, err := repo.ApplyReplyCounts(ctx, map[uuid.UUID]int{existing: 3, missing: 7})
	if err != nil {
		t.Fatalf("apply counts: %v", err)
	}

	if len(pending) != 1 || pending[0] != missing {
		t.Errorf("expected only the absent interaction to be reported missing, got %v", pending)
	}

	// The existing one must have been written despite the other being absent.
	got, err := repo.GetByID(ctx, existing)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.ReplyCount != 3 {
		t.Errorf("expected the existing interaction to be updated to 3, got %d", got.ReplyCount)
	}

	// Once the parent commits, the redelivered event must land and stop being reported.
	newRoot := mkInteraction(missing, nil, nil)
	newRoot.Depth = 0
	if err := repo.InsertBatch(ctx, []*database.Interaction{newRoot}); err != nil {
		t.Fatalf("insert parent: %v", err)
	}

	pending, err = repo.ApplyReplyCounts(ctx, map[uuid.UUID]int{missing: 7})
	if err != nil {
		t.Fatalf("apply counts: %v", err)
	}
	if len(pending) != 0 {
		t.Errorf("expected no missing ids once the row exists, got %v", pending)
	}

	created, err := repo.GetByID(ctx, missing)
	if err != nil {
		t.Fatalf("get parent: %v", err)
	}
	if created.ReplyCount != 7 {
		t.Errorf("expected the redelivered count to land, got %d", created.ReplyCount)
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
