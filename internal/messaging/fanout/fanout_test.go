package fanout_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/rlsvr/kozlony/internal/messaging/fanout"
)

func TestFanoutPipelineBroadcastAndRouting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := fanout.NewPipeline(ctx, 1*time.Millisecond, 10)
	defer p.Stop()

	rootID1 := uuid.MustParse("01923f12-0000-7000-8000-000000000001")
	rootID2 := uuid.MustParse("01923f12-0000-7000-8000-000000000002")

	// Client 1: Feed subscriber on "root" group
	clientFeed := fanout.NewClient("c-feed", "root", nil, 10)
	p.RegisterAndWait(clientFeed)

	// Client 2: Thread subscriber on "root" group for rootID1
	clientThread1 := fanout.NewClient("c-thread1", "root", &rootID1, 10)
	p.RegisterAndWait(clientThread1)

	// Client 3: Thread subscriber on "root" group for rootID2
	clientThread2 := fanout.NewClient("c-thread2", "root", &rootID2, 10)
	p.RegisterAndWait(clientThread2)

	if total := p.TotalClients(); total != 3 {
		t.Fatalf("expected 3 total clients, got %d", total)
	}

	// 1. Broadcast event for rootID1
	msg1 := []byte(`{"id":"01923f12-0000-7000-8000-000000000003","root_id":"01923f12-0000-7000-8000-000000000001","group_id":"root"}`)
	delivered := p.Broadcast("root", &rootID1, msg1)
	if delivered != 2 {
		t.Errorf("expected 2 delivered (feed + thread1), got %d", delivered)
	}

	select {
	case m := <-clientFeed.Events:
		if string(m) != string(msg1) {
			t.Errorf("feed client got unexpected message: %s", string(m))
		}
	default:
		t.Errorf("feed client did not receive message")
	}

	select {
	case m := <-clientThread1.Events:
		if string(m) != string(msg1) {
			t.Errorf("thread1 client got unexpected message: %s", string(m))
		}
	default:
		t.Errorf("thread1 client did not receive message")
	}

	select {
	case m := <-clientThread2.Events:
		t.Errorf("thread2 client should not have received message, got: %s", string(m))
	default:
		// expected
	}

	// 2. Broadcast feed-level event (rootID == nil)
	msgFeed := []byte(`{"group_id":"root","text":"feed announcement"}`)
	delivered = p.Broadcast("root", nil, msgFeed)
	if delivered != 1 {
		t.Errorf("expected 1 delivered to feed, got %d", delivered)
	}

	select {
	case <-clientFeed.Events:
		// expected
	default:
		t.Errorf("feed client did not receive feed announcement")
	}

	select {
	case m := <-clientThread1.Events:
		t.Errorf("thread1 should not receive feed-only event, got: %s", string(m))
	default:
	}

	// 3. Test Switch: Move clientThread2 from rootID2 to rootID1
	p.Switch(clientThread2, &rootID1)
	// Wait a moment for actor batch interval to apply
	time.Sleep(5 * time.Millisecond)

	delivered = p.Broadcast("root", &rootID1, msg1)
	if delivered != 3 {
		t.Errorf("expected 3 delivered after switch (feed + thread1 + thread2), got %d", delivered)
	}

	// Drain
	<-clientFeed.Events
	<-clientThread1.Events
	select {
	case <-clientThread2.Events:
		// expected
	default:
		t.Errorf("thread2 did not receive message after switching to rootID1")
	}

	// 4. Test Remove
	p.RemoveAndWait(clientThread1)
	if total := p.TotalClients(); total != 2 {
		t.Errorf("expected 2 total clients after removal, got %d", total)
	}
}

func TestFanoutClientBufferDrop(t *testing.T) {
	c := fanout.NewClient("c-slow", "root", nil, 1)

	if !c.Send([]byte("msg1")) {
		t.Error("expected msg1 to be sent")
	}

	// Buffer is full (size 1)
	if c.Send([]byte("msg2")) {
		t.Error("expected msg2 to be dropped without blocking")
	}

	c.Close()
	if c.Send([]byte("msg3")) {
		t.Error("expected msg3 to fail on closed client")
	}
}

func TestFanoutViewEpochRejectsStaleEvents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := fanout.NewPipeline(ctx, 1*time.Millisecond, 10)
	defer p.Stop()

	rootID1 := uuid.MustParse("01923f12-0000-7000-8000-000000000001")
	rootID2 := uuid.MustParse("01923f12-0000-7000-8000-000000000002")

	// Client starts on rootID1 with epoch 1
	client := fanout.NewClient("c-view-switch", "root", &rootID1, 10)
	p.RegisterAndWait(client)

	if client.Epoch() != 1 {
		t.Fatalf("expected initial epoch 1, got %d", client.Epoch())
	}

	// Stale event send simulation: snapshot had epoch 1, but client switches to epoch 2
	client.BumpEpoch() // Now epoch 2

	// An in-flight broadcast reading a snapshot recorded at epoch 1 attempts delivery:
	delivered := client.SendWithEpoch([]byte("stale-event-from-view-1"), 1)
	if delivered {
		t.Fatal("expected SendWithEpoch to drop stale event from previous epoch")
	}

	// Verify buffer is empty
	select {
	case m := <-client.Events:
		t.Fatalf("expected no event in channel, received: %s", string(m))
	default:
	}

	// Now switch properly via SwitchAndWait to rootID2
	p.SwitchAndWait(client, &rootID2)

	// Broadcast for rootID2
	msg2 := []byte(`{"id":"msg2","root_id":"01923f12-0000-7000-8000-000000000002","group_id":"root"}`)
	d := p.Broadcast("root", &rootID2, msg2)
	if d != 1 {
		t.Fatalf("expected 1 delivered for new view, got %d", d)
	}

	select {
	case m := <-client.Events:
		if string(m) != string(msg2) {
			t.Fatalf("expected %s, got %s", string(msg2), string(m))
		}
	default:
		t.Fatal("expected client to receive event for new view")
	}
}

type mockGroupListener struct {
	active   []string
	inactive []string
}

func (m *mockGroupListener) OnGroupActive(groupID string) error {
	m.active = append(m.active, groupID)
	return nil
}

func (m *mockGroupListener) OnGroupInactive(groupID string) {
	m.inactive = append(m.inactive, groupID)
}

func TestFanoutGroupSubscriptionListener(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	listener := &mockGroupListener{}
	p := fanout.NewPipeline(ctx, 1*time.Millisecond, 10, fanout.WithGroupListener(listener))
	defer p.Stop()

	// 1. Add first client to group "g1" -> should trigger OnGroupActive("g1")
	c1 := fanout.NewClient("c1", "g1", nil, 10)
	p.RegisterAndWait(c1)

	if len(listener.active) != 1 || listener.active[0] != "g1" {
		t.Fatalf("expected OnGroupActive for g1, got: %v", listener.active)
	}

	// 2. Add second client to group "g1" -> should NOT trigger OnGroupActive again
	c2 := fanout.NewClient("c2", "g1", nil, 10)
	p.RegisterAndWait(c2)

	if len(listener.active) != 1 {
		t.Fatalf("expected still only 1 OnGroupActive call, got: %v", listener.active)
	}

	// 3. Add client to group "g2" -> should trigger OnGroupActive("g2")
	c3 := fanout.NewClient("c3", "g2", nil, 10)
	p.RegisterAndWait(c3)

	if len(listener.active) != 2 || listener.active[1] != "g2" {
		t.Fatalf("expected OnGroupActive for g2, got: %v", listener.active)
	}

	// 4. Remove one client from "g1" (c1) -> should NOT trigger OnGroupInactive yet
	p.RemoveAndWait(c1)
	if len(listener.inactive) != 0 {
		t.Fatalf("expected no OnGroupInactive calls, got: %v", listener.inactive)
	}

	// 5. Remove last client from "g1" (c2) -> should trigger OnGroupInactive("g1")
	p.RemoveAndWait(c2)
	if len(listener.inactive) != 1 || listener.inactive[0] != "g1" {
		t.Fatalf("expected OnGroupInactive for g1, got: %v", listener.inactive)
	}

	// 6. Remove last client from "g2" (c3) -> should trigger OnGroupInactive("g2")
	p.RemoveAndWait(c3)
	if len(listener.inactive) != 2 || listener.inactive[1] != "g2" {
		t.Fatalf("expected OnGroupInactive for g2, got: %v", listener.inactive)
	}
}

func TestFanoutTopicDispatchDirectDelivery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := fanout.NewPipeline(ctx, 1*time.Millisecond, 10)
	defer p.Stop()

	rootID := uuid.MustParse("01923f12-0000-7000-8000-000000000099")
	client := fanout.NewClient("c-worker-test", "root", &rootID, 50)
	p.RegisterAndWait(client)

	// In the zero-filter architecture, payload is dispatched directly to the target thread topic
	payload := []byte(`{"id":"msg-worker-1","root_id":"01923f12-0000-7000-8000-000000000099","group_id":"root"}`)
	delivered := p.Broadcast("root", &rootID, payload)
	if delivered != 1 {
		t.Fatalf("expected 1 delivered, got %d", delivered)
	}

	select {
	case m := <-client.Events:
		if string(m) != string(payload) {
			t.Fatalf("expected payload %s, got: %s", string(payload), string(m))
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for topic dispatch to deliver message")
	}
}
