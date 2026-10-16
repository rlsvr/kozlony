package drainer_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/mock/gomock"

	"github.com/rlsvr/kozlony/internal/config"
	"github.com/rlsvr/kozlony/internal/database"
	dbmocks "github.com/rlsvr/kozlony/internal/database/mocks"
	"github.com/rlsvr/kozlony/internal/drainer"
	drainermocks "github.com/rlsvr/kozlony/internal/drainer/mocks"
	"github.com/rlsvr/kozlony/internal/messaging/events"
)

func counterMsg(t *testing.T, counts map[string]int) *fakeMsg {
	t.Helper()
	data, err := json.Marshal(events.ReplyCountsRecalculatedEvent{
		GroupID: testGroupID,
		Counts:  events.ReplyCountsRecalculatedEventCounts(counts),
	})
	if err != nil {
		t.Fatalf("marshal counter event: %v", err)
	}
	return newFakeMsg("BOARD.root.counter.replied", data)
}

func batchOf(msgs ...jetstream.Msg) *fakeMessageBatch {
	ch := make(chan jetstream.Msg, len(msgs))
	for _, m := range msgs {
		ch <- m
	}
	close(ch)
	return &fakeMessageBatch{ch: ch}
}

// The whole design rests on this: a redelivered event applies the same absolute total
// again, which leaves the stored value unchanged. No dedup bookkeeping, no delta.
func TestCounter_RedeliveryIsIdempotent(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	id := uuid.Must(uuid.NewV7())
	var applied []map[uuid.UUID]int

	mockFetcher := drainermocks.NewMockBatchFetcher(ctrl)
	mockRepo := drainermocks.NewMockReplyCountWriter(ctrl)

	mockRepo.EXPECT().
		ApplyReplyCounts(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, counts map[uuid.UUID]int) ([]uuid.UUID, error) {
			applied = append(applied, counts)
			return nil, nil
		}).Times(2)

	first := counterMsg(t, map[string]int{id.String(): 50})
	mockFetcher.EXPECT().Fetch(gomock.Any(), gomock.Any()).Return(batchOf(first), nil)

	c := drainer.NewCounter(mockFetcher, mockRepo, nil, &config.Config{DrainBatchSize: 10})
	if _, err := c.ProcessBatch(context.Background()); err != nil {
		t.Fatalf("first apply failed: %v", err)
	}

	// Same message again, as JetStream would deliver after a lost Ack.
	second := counterMsg(t, map[string]int{id.String(): 50})
	mockFetcher.EXPECT().Fetch(gomock.Any(), gomock.Any()).Return(batchOf(second), nil)

	if _, err := c.ProcessBatch(context.Background()); err != nil {
		t.Fatalf("redelivered apply failed: %v", err)
	}

	if len(applied) != 2 {
		t.Fatalf("expected 2 apply calls, got %d", len(applied))
	}
	if applied[0][id] != applied[1][id] {
		t.Errorf("redelivery changed the value: %d then %d", applied[0][id], applied[1][id])
	}
}

// Out-of-order delivery must never let a stale, smaller total overwrite a newer one.
// The Counter merges with max(); ApplyReplyCounts additionally guards monotonicity.
func TestCounter_KeepsHighestCountWhenOutOfOrder(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	id := uuid.Must(uuid.NewV7())

	mockFetcher := drainermocks.NewMockBatchFetcher(ctrl)
	mockRepo := drainermocks.NewMockReplyCountWriter(ctrl)

	var applied map[uuid.UUID]int
	mockRepo.EXPECT().
		ApplyReplyCounts(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, counts map[uuid.UUID]int) ([]uuid.UUID, error) {
			applied = counts
			return nil, nil
		})

	stale := counterMsg(t, map[string]int{id.String(): 10})
	fresher := counterMsg(t, map[string]int{id.String(): 80})

	mockFetcher.EXPECT().Fetch(gomock.Any(), gomock.Any()).Return(batchOf(stale, fresher), nil)

	c := drainer.NewCounter(mockFetcher, mockRepo, nil, &config.Config{DrainBatchSize: 10})
	if _, err := c.ProcessBatch(context.Background()); err != nil {
		t.Fatalf("ProcessBatch failed: %v", err)
	}

	if applied[id] != 80 {
		t.Errorf("expected the highest count 80 to be applied, got %d", applied[id])
	}
}

func TestCounter_ApplyFailureNaksMessages(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	id := uuid.Must(uuid.NewV7())

	mockFetcher := drainermocks.NewMockBatchFetcher(ctrl)
	mockRepo := drainermocks.NewMockReplyCountWriter(ctrl)

	mockRepo.EXPECT().
		ApplyReplyCounts(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("db down"))

	msg := counterMsg(t, map[string]int{id.String(): 5})
	mockFetcher.EXPECT().Fetch(gomock.Any(), gomock.Any()).Return(batchOf(msg), nil)

	c := drainer.NewCounter(mockFetcher, mockRepo, nil, &config.Config{DrainBatchSize: 10})
	count, err := c.ProcessBatch(context.Background())
	if err == nil {
		t.Fatal("expected an error when ApplyReplyCounts fails")
	}
	if count != 0 {
		t.Errorf("expected count 0, got %d", count)
	}
	if !msg.naked {
		t.Error("expected message to be naked so it is redelivered")
	}
	if msg.acked {
		t.Error("did not expect message to be acked when the write failed")
	}
}

func TestCounter_SkipsInvalidPayloads(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	id := uuid.Must(uuid.NewV7())

	mockFetcher := drainermocks.NewMockBatchFetcher(ctrl)
	mockRepo := drainermocks.NewMockReplyCountWriter(ctrl)

	var applied map[uuid.UUID]int
	mockRepo.EXPECT().
		ApplyReplyCounts(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, counts map[uuid.UUID]int) ([]uuid.UUID, error) {
			applied = counts
			return nil, nil
		})

	garbage := newFakeMsg("BOARD.root.counter.replied", []byte("not json"))
	valid := counterMsg(t, map[string]int{
		id.String():      3,
		"not-a-uuid-xyz": 9,
	})

	mockFetcher.EXPECT().Fetch(gomock.Any(), gomock.Any()).Return(batchOf(garbage, valid), nil)

	c := drainer.NewCounter(mockFetcher, mockRepo, nil, &config.Config{DrainBatchSize: 10})
	if _, err := c.ProcessBatch(context.Background()); err != nil {
		t.Fatalf("ProcessBatch failed: %v", err)
	}

	if !garbage.acked {
		t.Error("expected unparseable message to be acked rather than looped forever")
	}
	if applied[id] != 3 {
		t.Errorf("expected valid count 3 to be applied, got %d", applied[id])
	}
	if len(applied) != 1 {
		t.Errorf("expected the malformed uuid to be dropped, got %d entries", len(applied))
	}
}

func TestCounter_StopWaitsForWorkers(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFetcher := drainermocks.NewMockBatchFetcher(ctrl)
	mockRepo := drainermocks.NewMockReplyCountWriter(ctrl)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msg := counterMsg(t, map[string]int{uuid.Must(uuid.NewV7()).String(): 1})

	// The first fetch returns a real batch; ApplyReplyCounts then blocks until the
	// test releases it, so Stop has an in-flight write to wait on.
	mockFetcher.EXPECT().Fetch(gomock.Any(), gomock.Any()).Return(batchOf(msg), nil)
	mockFetcher.EXPECT().
		Fetch(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ int, _ ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
			return nil, context.Canceled
		}).
		AnyTimes()

	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	mockRepo.EXPECT().
		ApplyReplyCounts(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ map[uuid.UUID]int) ([]uuid.UUID, error) {
			once.Do(func() { close(started) })
			<-release
			return nil, nil
		}).
		AnyTimes()

	c := drainer.NewCounter(mockFetcher, mockRepo, nil, &config.Config{
		DrainBatchSize:            10,
		DrainFlushInterval:        time.Millisecond,
		NATSConcurrentSubscribers: 1,
	})

	c.Start(ctx)

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("worker never started an apply")
	}

	done := make(chan struct{})
	go func() {
		c.Stop()
		close(done)
	}()

	// Stop must block while the apply is still in flight, otherwise the database pool
	// could close underneath it.
	select {
	case <-done:
		t.Fatal("Stop returned while ApplyReplyCounts was still running")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return after the apply completed")
	}
}

// The drainer must publish counts only after the rows are durably committed, and must
// publish before it is considered done, otherwise counts are never emitted.
func TestDrainer_PublishesCountsAfterAck(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rootID := uuid.Must(uuid.NewV7())
	replyID := uuid.Must(uuid.NewV7())

	mockConsumer := drainermocks.NewMockBatchFetcher(ctrl)
	mockRepo := dbmocks.NewMockRepository(ctrl)
	mockPublisher := drainermocks.NewMockReplyCountPublisher(ctrl)

	evt := events.InteractionCreatedEvent{
		Author:    testAuthor,
		Body:      "a reply",
		CreatedAt: time.Now().UTC(),
		GroupID:   testGroupID,
		ID:        replyID.String(),
		ParentID:  ptrTo(rootID.String()),
		RootID:    ptrTo(rootID.String()),
		Version:   1,
	}
	data, _ := json.Marshal(evt)
	msg := newFakeMsg("BOARD.root.evt.created", data)

	mockConsumer.EXPECT().Fetch(gomock.Any(), gomock.Any()).Return(batchOf(msg), nil)

	// InsertBatchWithCounts returns the absolute count the drainer must forward.
	mockRepo.EXPECT().
		InsertBatchWithCounts(gomock.Any(), gomock.Len(1)).
		Return(map[uuid.UUID]int{rootID: 1}, nil)

	var published events.ReplyCountsRecalculatedEvent
	mockPublisher.EXPECT().
		PublishReplyCountsRecalculated(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, e *events.ReplyCountsRecalculatedEvent) error {
			// By the time the count is announced the rows must already be acked,
			// which proves it is published after commit rather than before.
			if !msg.acked {
				t.Error("expected the source message to be acked before counts are published")
			}
			published = *e
			return nil
		})

	d := drainer.NewWithCountPublisher(mockConsumer, mockRepo, nil, mockPublisher, &config.Config{DrainBatchSize: 10})
	if _, err := d.ProcessBatch(context.Background()); err != nil {
		t.Fatalf("ProcessBatch failed: %v", err)
	}

	if published.GroupID != testGroupID {
		t.Errorf("expected group root, got %q", published.GroupID)
	}
	if got := published.Counts[rootID.String()]; got != 1 {
		t.Errorf("expected count 1 for the parent, got %d", got)
	}
}

// A publish failure must not fail the batch: the rows are already committed and acked,
// and counts are derived data that self-heals on the next batch touching the parent.
func TestDrainer_PublishFailureDoesNotFailBatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rootID := uuid.Must(uuid.NewV7())
	replyID := uuid.Must(uuid.NewV7())

	mockConsumer := drainermocks.NewMockBatchFetcher(ctrl)
	mockRepo := dbmocks.NewMockRepository(ctrl)
	mockPublisher := drainermocks.NewMockReplyCountPublisher(ctrl)

	evt := events.InteractionCreatedEvent{
		Author:    testAuthor,
		Body:      "a reply",
		CreatedAt: time.Now().UTC(),
		GroupID:   testGroupID,
		ID:        replyID.String(),
		ParentID:  ptrTo(rootID.String()),
		RootID:    ptrTo(rootID.String()),
		Version:   1,
	}
	data, _ := json.Marshal(evt)
	msg := newFakeMsg("BOARD.root.evt.created", data)

	mockConsumer.EXPECT().Fetch(gomock.Any(), gomock.Any()).Return(batchOf(msg), nil)
	mockRepo.EXPECT().
		InsertBatchWithCounts(gomock.Any(), gomock.Any()).
		Return(map[uuid.UUID]int{rootID: 7}, nil)
	mockPublisher.EXPECT().
		PublishReplyCountsRecalculated(gomock.Any(), gomock.Any()).
		Return(errors.New("nats unavailable"))

	d := drainer.NewWithCountPublisher(mockConsumer, mockRepo, nil, mockPublisher, &config.Config{DrainBatchSize: 10})

	count, err := d.ProcessBatch(context.Background())
	if err != nil {
		t.Fatalf("publish failure must not fail the batch, got %v", err)
	}
	if count != 1 {
		t.Errorf("expected count 1, got %d", count)
	}
	if !msg.acked {
		t.Error("expected the message to stay acked since the rows are committed")
	}
}

// The drainer must never insert an edit notification. An InteractionEditedEvent has no
// author and no created_at, so decoding one as an InteractionCreatedEvent would produce a
// row with author=” and a zero timestamp. The subject rename keeps it out of the drainer's
// filter; this asserts the decode itself would also be rejected, so a filter mistake alone
// cannot corrupt data.
func TestDrainer_NeverInsertsEditNotifications(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConsumer := drainermocks.NewMockBatchFetcher(ctrl)
	mockRepo := dbmocks.NewMockRepository(ctrl)
	mockDLQ := drainermocks.NewMockDLQPublisher(ctrl)

	// Any insert attempt is a hard failure, which surfaces the bug loudly.
	mockRepo.EXPECT().
		InsertBatchWithCounts(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, items []*database.Interaction) (map[uuid.UUID]int, error) {
			t.Fatalf("drainer attempted to insert %d rows from an edit notification", len(items))
			return nil, nil
		}).
		AnyTimes()

	// It must be routed to the DLQ rather than silently dropped, so an operator can see
	// that something published onto the drainer's subject.
	var dlqReason string
	mockDLQ.EXPECT().
		PublishDLQ(gomock.Any(), "BOARD.root.audit.edited", gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, _ []byte, reason string) error {
			dlqReason = reason
			return nil
		})

	// Even if an edit notification were routed here, it must be rejected as a payload
	// rather than decoded into a half-empty interaction.
	editPayload, _ := json.Marshal(events.InteractionEditedEvent{
		Body:    "edited",
		GroupID: testGroupID,
		ID:      uuid.Must(uuid.NewV7()).String(),
		Version: 2,
	})
	msg := newFakeMsg("BOARD.root.audit.edited", editPayload)

	mockConsumer.EXPECT().
		Fetch(gomock.Any(), gomock.Any()).
		Return(batchOf(msg), nil)

	d := drainer.New(mockConsumer, mockRepo, mockDLQ, &config.Config{DrainBatchSize: 10})
	count, err := d.ProcessBatch(context.Background())
	if err != nil {
		t.Fatalf("ProcessBatch should not error on a non-created message: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 interactions drained from an edit notification, got %d", count)
	}
	if dlqReason == "" {
		t.Error("expected the edit notification to be routed to the DLQ")
	}
}

// The bug the stress test surfaced: a reply can be drained before the batch that creates
// its parent commits, so the count for that parent has nowhere to land. The batch that
// creates the parent emits counts for the parent's own parent, not for the parent itself,
// so nothing else would ever retry it. Acking here loses the count permanently.
func TestCounter_RedeliversWhenParentRowMissing(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	parentID := uuid.Must(uuid.NewV7())

	mockFetcher := drainermocks.NewMockBatchFetcher(ctrl)
	mockRepo := drainermocks.NewMockReplyCountWriter(ctrl)

	mockRepo.EXPECT().
		ApplyReplyCounts(gomock.Any(), gomock.Any()).
		Return([]uuid.UUID{parentID}, nil)

	msg := counterMsg(t, map[string]int{parentID.String(): 1})
	mockFetcher.EXPECT().Fetch(gomock.Any(), gomock.Any()).Return(batchOf(msg), nil)

	c := drainer.NewCounter(mockFetcher, mockRepo, nil, &config.Config{DrainBatchSize: 10})
	count, err := c.ProcessBatch(context.Background())
	if err != nil {
		t.Fatalf("ProcessBatch failed: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 applied, got %d", count)
	}
	if !msg.naked {
		t.Error("expected the message to be naked so the count is retried once the parent commits")
	}
	if msg.acked {
		t.Error("acking would lose the count permanently")
	}
}

// Once the parent exists the redelivered event must land and be acked.
func TestCounter_AppliesAfterParentCommits(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	parentID := uuid.Must(uuid.NewV7())

	mockFetcher := drainermocks.NewMockBatchFetcher(ctrl)
	mockRepo := drainermocks.NewMockReplyCountWriter(ctrl)

	mockRepo.EXPECT().
		ApplyReplyCounts(gomock.Any(), gomock.Any()).
		Return(nil, nil)

	msg := counterMsg(t, map[string]int{parentID.String(): 1})
	mockFetcher.EXPECT().Fetch(gomock.Any(), gomock.Any()).Return(batchOf(msg), nil)

	c := drainer.NewCounter(mockFetcher, mockRepo, nil, &config.Config{DrainBatchSize: 10})
	if _, err := c.ProcessBatch(context.Background()); err != nil {
		t.Fatalf("ProcessBatch failed: %v", err)
	}
	if !msg.acked {
		t.Error("expected the message to be acked once the parent row exists")
	}
}

// When a batch contains counts for both committed and uncommitted parents, only the
// message referencing the missing parent must be naked; messages whose IDs succeeded
// must be acked so they do not exhaust retry attempts.
func TestCounter_GranularNakOnlyAffectedMessages(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	committedID := uuid.Must(uuid.NewV7())
	missingID := uuid.Must(uuid.NewV7())

	mockFetcher := drainermocks.NewMockBatchFetcher(ctrl)
	mockRepo := drainermocks.NewMockReplyCountWriter(ctrl)

	mockRepo.EXPECT().
		ApplyReplyCounts(gomock.Any(), gomock.Any()).
		Return([]uuid.UUID{missingID}, nil)

	msgSuccess := counterMsg(t, map[string]int{committedID.String(): 5})
	msgMissing := counterMsg(t, map[string]int{missingID.String(): 2})

	mockFetcher.EXPECT().Fetch(gomock.Any(), gomock.Any()).Return(batchOf(msgSuccess, msgMissing), nil)

	c := drainer.NewCounter(mockFetcher, mockRepo, nil, &config.Config{DrainBatchSize: 10})
	count, err := c.ProcessBatch(context.Background())
	if err != nil {
		t.Fatalf("ProcessBatch failed: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 applied count, got %d", count)
	}
	if !msgSuccess.acked {
		t.Error("expected msgSuccess to be acked")
	}
	if msgSuccess.naked {
		t.Error("msgSuccess should not have been naked")
	}
	if !msgMissing.naked {
		t.Error("expected msgMissing to be naked with delay")
	}
	if msgMissing.acked {
		t.Error("msgMissing should not have been acked")
	}
}

// A reply that commits before its parent must not hold up the whole batch. Retrying a
// 2000-message batch because one parent is in another worker's open transaction cost a
// full redelivery for a condition that resolves in milliseconds, and under load those
// retries compounded until the backlog never drained. The parent's own batch counts it.
func TestDrainer_CommitsWhenParentNotVisible(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rootID := uuid.Must(uuid.NewV7())
	replyID := uuid.Must(uuid.NewV7())

	mockConsumer := drainermocks.NewMockBatchFetcher(ctrl)
	mockRepo := dbmocks.NewMockRepository(ctrl)
	mockPublisher := drainermocks.NewMockReplyCountPublisher(ctrl)

	evt := events.InteractionCreatedEvent{
		Author:    testAuthor,
		Body:      "a reply whose parent is still in flight",
		CreatedAt: time.Now().UTC(),
		GroupID:   testGroupID,
		ID:        replyID.String(),
		ParentID:  ptrTo(rootID.String()),
		RootID:    ptrTo(rootID.String()),
		Version:   1,
	}
	data, _ := json.Marshal(evt)
	msg := newFakeMsg("BOARD.root.evt.created", data)

	mockConsumer.EXPECT().Fetch(gomock.Any(), gomock.Any()).Return(batchOf(msg), nil)

	// The parent is invisible, so it is absent from the counts; only the reply itself is
	// returned, because every inserted node is counted by its own batch.
	mockRepo.EXPECT().
		InsertBatchWithCounts(gomock.Any(), gomock.Len(1)).
		Return(map[uuid.UUID]int{replyID: 0}, nil)

	var published events.ReplyCountsRecalculatedEvent
	mockPublisher.EXPECT().
		PublishReplyCountsRecalculated(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, e *events.ReplyCountsRecalculatedEvent) error {
			published = *e
			return nil
		})

	d := drainer.NewWithCountPublisher(mockConsumer, mockRepo, nil, mockPublisher, &config.Config{DrainBatchSize: 10})
	count, err := d.ProcessBatch(context.Background())
	if err != nil {
		t.Fatalf("ProcessBatch failed: %v", err)
	}
	if count != 1 {
		t.Errorf("expected the batch to be drained, got %d", count)
	}
	if !msg.acked {
		t.Error("expected the batch to commit and ack rather than be redelivered")
	}
	if msg.naked {
		t.Error("nak-ing here reintroduces the retry storm this path exists to avoid")
	}
	if _, present := published.Counts[rootID.String()]; present {
		t.Error("expected the invisible parent to be omitted; its own batch counts it")
	}
	if _, present := published.Counts[replyID.String()]; !present {
		t.Error("expected the inserted node to be counted by its own batch")
	}
}

// Verifies that when a fetcher implements PartitionBatchFetcher, ProcessBatchForPartition
// calls FetchPartition with the expected partition number.
type mockCombinedFetcher struct {
	*drainermocks.MockBatchFetcher
	*drainermocks.MockPartitionBatchFetcher
}

func TestCounter_PartitionBatchFetcher(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	id := uuid.Must(uuid.NewV7())
	mockBF := drainermocks.NewMockBatchFetcher(ctrl)
	mockPF := drainermocks.NewMockPartitionBatchFetcher(ctrl)
	fetcher := &mockCombinedFetcher{MockBatchFetcher: mockBF, MockPartitionBatchFetcher: mockPF}
	mockRepo := drainermocks.NewMockReplyCountWriter(ctrl)

	mockRepo.EXPECT().
		ApplyReplyCounts(gomock.Any(), gomock.Any()).
		Return(nil, nil)

	msg := counterMsg(t, map[string]int{id.String(): 42})

	// Must call FetchPartition specifically on partition 7
	mockPF.EXPECT().
		FetchPartition(7, 10, gomock.Any()).
		Return(batchOf(msg), nil)

	cfg := &config.Config{
		DrainBatchSize:   10,
		IngestPartitions: 16,
	}
	c := drainer.NewCounter(fetcher, mockRepo, nil, cfg)

	count, err := c.ProcessBatchForPartition(context.Background(), 7)
	if err != nil {
		t.Fatalf("ProcessBatchForPartition failed: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 applied count, got %d", count)
	}
	if !msg.acked {
		t.Error("expected message to be acked")
	}
}

// Verifies worker ID to partition mapping: (workerID - 1) % numPartitions.
func TestCounter_RunWorkerPartitionDistribution(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockBF := drainermocks.NewMockBatchFetcher(ctrl)
	mockPF := drainermocks.NewMockPartitionBatchFetcher(ctrl)
	fetcher := &mockCombinedFetcher{MockBatchFetcher: mockBF, MockPartitionBatchFetcher: mockPF}
	mockRepo := drainermocks.NewMockReplyCountWriter(ctrl)

	cfg := &config.Config{
		DrainBatchSize:   10,
		IngestPartitions: 4,
	}
	c := drainer.NewCounter(fetcher, mockRepo, nil, cfg)

	// Worker 1 should pull from partition 0
	ctx1, cancel1 := context.WithCancel(context.Background())
	cancel1()
	if err := c.RunWorker(ctx1, 1); err != nil {
		t.Errorf("expected nil error on canceled worker 1, got %v", err)
	}

	// Worker 3 should pull from partition 2: (3 - 1) % 4 = 2
	ctx3, cancel3 := context.WithCancel(context.Background())
	mockPF.EXPECT().
		FetchPartition(2, 10, gomock.Any()).
		DoAndReturn(func(_, _ int, _ ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
			cancel3()
			return batchOf(), nil
		}).
		AnyTimes()

	_ = c.RunWorker(ctx3, 3)

	if workers := c.Workers(); workers != 1 {
		t.Errorf("expected default 1 worker, got %d", workers)
	}
}

func TestCounter_UpdatesCacheOnSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	id := uuid.Must(uuid.NewV7())
	mockFetcher := drainermocks.NewMockBatchFetcher(ctrl)
	mockRepo := drainermocks.NewMockReplyCountWriter(ctrl)
	mockCache := drainermocks.NewMockReplyCountCache(ctrl)

	mockRepo.EXPECT().
		ApplyReplyCounts(gomock.Any(), gomock.Any()).
		Return(nil, nil)

	mockCache.EXPECT().
		UpdateReplyCount(gomock.Any(), id, 42).
		Return(nil)

	msg := counterMsg(t, map[string]int{id.String(): 42})
	mockFetcher.EXPECT().Fetch(gomock.Any(), gomock.Any()).Return(batchOf(msg), nil)

	c := drainer.NewCounter(mockFetcher, mockRepo, mockCache, &config.Config{DrainBatchSize: 10})
	count, err := c.ProcessBatch(context.Background())
	if err != nil {
		t.Fatalf("ProcessBatch failed: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 count applied, got %d", count)
	}
}

func ptrTo(s string) *string { return &s }
