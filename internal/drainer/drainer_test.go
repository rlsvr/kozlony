package drainer_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/mock/gomock"

	"github.com/rlsvr/kozlony/internal/config"
	"github.com/rlsvr/kozlony/internal/database"
	dbmocks "github.com/rlsvr/kozlony/internal/database/mocks"
	"github.com/rlsvr/kozlony/internal/drainer"
	drainermocks "github.com/rlsvr/kozlony/internal/drainer/mocks"
	"github.com/rlsvr/kozlony/internal/messaging/events"
)

type fakeMsg struct {
	subject string
	data    []byte
	ackMu   sync.Mutex
	acked   bool
	naked   bool
}

func newFakeMsg(subject string, data []byte) *fakeMsg {
	return &fakeMsg{
		subject: subject,
		data:    data,
	}
}

func (*fakeMsg) Metadata() (*jetstream.MsgMetadata, error) { return nil, nil }
func (m *fakeMsg) Data() []byte                            { return m.data }
func (*fakeMsg) Headers() natsio.Header                    { return nil }
func (m *fakeMsg) Subject() string                         { return m.subject }
func (*fakeMsg) Reply() string                             { return "" }
func (m *fakeMsg) Ack() error {
	m.ackMu.Lock()
	defer m.ackMu.Unlock()
	m.acked = true
	return nil
}
func (*fakeMsg) DoubleAck(_ context.Context) error { return nil }
func (m *fakeMsg) Nak() error {
	m.ackMu.Lock()
	defer m.ackMu.Unlock()
	m.naked = true
	return nil
}
func (*fakeMsg) NakWithDelay(_ time.Duration) error { return nil }
func (*fakeMsg) InProgress() error                  { return nil }
func (*fakeMsg) Term() error                        { return nil }
func (*fakeMsg) TermWithReason(_ string) error      { return nil }

type fakeMessageBatch struct {
	ch  chan jetstream.Msg
	err error
}

func (b *fakeMessageBatch) Messages() <-chan jetstream.Msg { return b.ch }
func (b *fakeMessageBatch) Error() error                   { return b.err }

func TestProcessBatch_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConsumer := drainermocks.NewMockBatchFetcher(ctrl)
	mockRepo := dbmocks.NewMockRepository(ctrl)

	cfg := &config.Config{
		DrainBatchSize:     10,
		DrainFlushInterval: 50 * time.Millisecond,
	}
	d := drainer.New(mockConsumer, mockRepo, nil, cfg)

	id := uuid.Must(uuid.NewV7()).String()
	evt := events.InteractionCreatedEvent{
		Author:    "Alice",
		Body:      "Batch message",
		CreatedAt: time.Now().UTC(),
		GroupID:   "root",
		ID:        id,
		Version:   1,
	}
	data, _ := json.Marshal(evt)
	msg := newFakeMsg("BOARD.root.evt.created", data)

	batchCh := make(chan jetstream.Msg, 1)
	batchCh <- msg
	close(batchCh)
	batch := &fakeMessageBatch{ch: batchCh}

	mockConsumer.EXPECT().
		Fetch(10, gomock.Any()).
		Return(batch, nil)

	mockRepo.EXPECT().
		InsertBatch(gomock.Any(), gomock.Len(1)).
		DoAndReturn(func(_ context.Context, items []*database.Interaction) error {
			if len(items) != 1 {
				t.Fatalf("expected 1 item, got %d", len(items))
			}
			if items[0].ID.String() != id {
				t.Errorf("expected ID %s, got %s", id, items[0].ID.String())
			}
			return nil
		})

	count, err := d.ProcessBatch(context.Background())
	if err != nil {
		t.Fatalf("unexpected error from ProcessBatch: %v", err)
	}
	if count != 1 {
		t.Errorf("expected count 1, got %d", count)
	}
	if !msg.acked {
		t.Error("expected message to be acked")
	}
	if msg.naked {
		t.Error("did not expect message to be naked")
	}
}

func TestProcessBatch_InsertError_NaksMessages(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConsumer := drainermocks.NewMockBatchFetcher(ctrl)
	mockRepo := dbmocks.NewMockRepository(ctrl)

	d := drainer.New(mockConsumer, mockRepo, nil, nil)

	evt := events.InteractionCreatedEvent{
		Author:    "Bob",
		Body:      "Message to fail",
		CreatedAt: time.Now().UTC(),
		GroupID:   "root",
		ID:        uuid.Must(uuid.NewV7()).String(),
		Version:   1,
	}
	data, _ := json.Marshal(evt)
	msg := newFakeMsg("BOARD.root.evt.created", data)

	batchCh := make(chan jetstream.Msg, 1)
	batchCh <- msg
	close(batchCh)
	batch := &fakeMessageBatch{ch: batchCh}

	mockConsumer.EXPECT().
		Fetch(gomock.Any(), gomock.Any()).
		Return(batch, nil)

	mockRepo.EXPECT().
		InsertBatch(gomock.Any(), gomock.Any()).
		Return(errors.New("db connection failure"))

	count, err := d.ProcessBatch(context.Background())
	if err == nil {
		t.Fatal("expected error from ProcessBatch on db failure, got nil")
	}
	if count != 0 {
		t.Errorf("expected count 0, got %d", count)
	}
	if !msg.naked {
		t.Error("expected message to be naked on insert failure")
	}
	if msg.acked {
		t.Error("did not expect message to be acked on insert failure")
	}
}

func TestProcessBatch_EmptyMessages(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConsumer := drainermocks.NewMockBatchFetcher(ctrl)
	mockRepo := dbmocks.NewMockRepository(ctrl)

	d := drainer.New(mockConsumer, mockRepo, nil, nil)

	batchCh := make(chan jetstream.Msg)
	close(batchCh)
	batch := &fakeMessageBatch{ch: batchCh}

	mockConsumer.EXPECT().
		Fetch(gomock.Any(), gomock.Any()).
		Return(batch, nil)

	count, err := d.ProcessBatch(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 0 {
		t.Errorf("expected count 0, got %d", count)
	}
}

func TestProcessBatch_CorruptMessage_RoutesToDLQ(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConsumer := drainermocks.NewMockBatchFetcher(ctrl)
	mockRepo := dbmocks.NewMockRepository(ctrl)
	mockDLQ := drainermocks.NewMockDLQPublisher(ctrl)

	d := drainer.New(mockConsumer, mockRepo, mockDLQ, nil)

	corruptMsg := newFakeMsg("BOARD.root.evt.created", []byte("invalid-json{"))
	batchCh := make(chan jetstream.Msg, 1)
	batchCh <- corruptMsg
	close(batchCh)
	batch := &fakeMessageBatch{ch: batchCh}

	mockConsumer.EXPECT().Fetch(gomock.Any(), gomock.Any()).Return(batch, nil)
	mockDLQ.EXPECT().PublishDLQ(gomock.Any(), "BOARD.root.evt.created", []byte("invalid-json{"), gomock.Any()).Return(nil)

	count, err := d.ProcessBatch(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 0 {
		t.Errorf("expected count 0, got %d", count)
	}
	if !corruptMsg.acked {
		t.Error("expected corrupt message to be acked after successful DLQ publish")
	}
}

func TestDrainer_RunShutdown(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConsumer := drainermocks.NewMockBatchFetcher(ctrl)
	mockRepo := dbmocks.NewMockRepository(ctrl)

	d := drainer.New(mockConsumer, mockRepo, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	if err := d.Run(ctx); err != nil {
		t.Fatalf("expected nil error on canceled run, got: %v", err)
	}
}

func TestDrainer_Start(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConsumer := drainermocks.NewMockBatchFetcher(ctrl)
	mockRepo := dbmocks.NewMockRepository(ctrl)

	d := drainer.New(mockConsumer, mockRepo, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	// Start in background goroutine and ensure it exits cleanly
	d.Start(ctx)
}

func TestDrainer_StartHelper(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConsumer := drainermocks.NewMockBatchFetcher(ctrl)
	mockRepo := dbmocks.NewMockRepository(ctrl)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	d := drainer.Start(ctx, mockConsumer, mockRepo, nil, nil)
	if d == nil {
		t.Fatal("expected non-nil drainer from Start helper")
	}
}
