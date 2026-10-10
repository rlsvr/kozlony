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
		DoAndReturn(func(_ context.Context, counts map[uuid.UUID]int) error {
			applied = append(applied, counts)
			return nil
		}).Times(2)

	first := counterMsg(t, map[string]int{id.String(): 50})
	mockFetcher.EXPECT().Fetch(gomock.Any(), gomock.Any()).Return(batchOf(first), nil)

	c := drainer.NewCounter(mockFetcher, mockRepo, &config.Config{DrainBatchSize: 10})
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
		DoAndReturn(func(_ context.Context, counts map[uuid.UUID]int) error {
			applied = counts
			return nil
		})

	stale := counterMsg(t, map[string]int{id.String(): 10})
	fresher := counterMsg(t, map[string]int{id.String(): 80})

	mockFetcher.EXPECT().Fetch(gomock.Any(), gomock.Any()).Return(batchOf(stale, fresher), nil)

	c := drainer.NewCounter(mockFetcher, mockRepo, &config.Config{DrainBatchSize: 10})
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
		Return(errors.New("db down"))

	msg := counterMsg(t, map[string]int{id.String(): 5})
	mockFetcher.EXPECT().Fetch(gomock.Any(), gomock.Any()).Return(batchOf(msg), nil)

	c := drainer.NewCounter(mockFetcher, mockRepo, &config.Config{DrainBatchSize: 10})
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
		DoAndReturn(func(_ context.Context, counts map[uuid.UUID]int) error {
			applied = counts
			return nil
		})

	garbage := newFakeMsg("BOARD.root.counter.replied", []byte("not json"))
	valid := counterMsg(t, map[string]int{
		id.String():      3,
		"not-a-uuid-xyz": 9,
	})

	mockFetcher.EXPECT().Fetch(gomock.Any(), gomock.Any()).Return(batchOf(garbage, valid), nil)

	c := drainer.NewCounter(mockFetcher, mockRepo, &config.Config{DrainBatchSize: 10})
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
		DoAndReturn(func(_ context.Context, _ map[uuid.UUID]int) error {
			once.Do(func() { close(started) })
			<-release
			return nil
		}).
		AnyTimes()

	c := drainer.NewCounter(mockFetcher, mockRepo, &config.Config{
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

func ptrTo(s string) *string { return &s }
