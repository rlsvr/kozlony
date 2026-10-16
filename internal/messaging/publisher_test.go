package messaging_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rlsvr/blazesse"

	"github.com/rlsvr/kozlony/internal/messaging"
)

func TestPublisher_SubscribeTopicLifecycle(t *testing.T) {
	// Initialize standalone publisher (Client == nil is valid for in-memory broadcaster testing)
	pub := messaging.NewPublisher(nil)
	defer pub.Close()

	topic := "test-topic-lifecycle"

	// 1. First subscriber
	sub1, unsub1, err := pub.SubscribeTopic(topic)
	if err != nil {
		t.Fatalf("unexpected error on first subscribe: %v", err)
	}
	defer unsub1()

	if sub1 == nil {
		t.Fatal("expected non-nil subscriber 1")
	}

	// 2. Second subscriber to the same topic
	sub2, unsub2, err := pub.SubscribeTopic(topic)
	if err != nil {
		t.Fatalf("unexpected error on second subscribe: %v", err)
	}
	defer unsub2()

	if sub2 == nil {
		t.Fatal("expected non-nil subscriber 2")
	}

	// Wait for actor reconfig interval (10ms) to incorporate into snapshot
	time.Sleep(20 * time.Millisecond)

	// 3. Publish frame to topic via Broadcaster
	frame := blazesse.TextFrame(`{"msg":"hello topic"}`)
	pub.Broadcaster().Publish(topic, frame)

	// Verify sub1 received
	select {
	case <-sub1.Recv():
		batch := sub1.Fetch()
		if len(batch) == 0 || batch[0].String() != "data: {\"msg\":\"hello topic\"}\n\n" {
			t.Fatalf("unexpected batch in sub1: %v", batch)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for sub1 to receive frame")
	}

	// Verify sub2 received
	select {
	case <-sub2.Recv():
		batch := sub2.Fetch()
		if len(batch) == 0 || batch[0].String() != "data: {\"msg\":\"hello topic\"}\n\n" {
			t.Fatalf("unexpected batch in sub2: %v", batch)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for sub2 to receive frame")
	}

	// 4. Test unsubscribe
	unsub1()
	unsub2()
}

func TestPublisher_SubscribeFilteredEvents(t *testing.T) {
	pub := messaging.NewPublisher(nil)
	defer pub.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	threadID := uuid.MustParse("01923f12-0000-7000-8000-000000000099")
	groupID := "root"

	eventCh, cleanup, err := pub.SubscribeFilteredEvents(ctx, &groupID, &threadID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer cleanup()

	// Wait for actor reconfig interval (10ms) to incorporate into snapshot
	time.Sleep(20 * time.Millisecond)

	// Publish to threadID topic
	payload := `{"id":"evt-1","root_id":"01923f12-0000-7000-8000-000000000099"}`
	pub.Broadcaster().Publish(threadID.String(), blazesse.TextFrame(payload))

	select {
	case data := <-eventCh:
		expected := "data: " + payload + "\n\n"
		if string(data) != expected {
			t.Fatalf("expected %q, got %q", expected, string(data))
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for event on channel")
	}
}
