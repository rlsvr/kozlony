package messaging_test

import (
	"context"
	"testing"

	"github.com/rlsvr/kozlony/internal/messaging"
	"github.com/rlsvr/kozlony/internal/messaging/events"
)

func TestPartitionConsumersNilClient(t *testing.T) {
	ctx := context.Background()

	// NewPartitionedDrainerConsumer should return an error if client is nil
	if _, err := messaging.NewPartitionedDrainerConsumer(ctx, nil, 16); err == nil {
		t.Error("expected error from NewPartitionedDrainerConsumer with nil client, got nil")
	}

	// NewCounterConsumer should return an error if client is nil
	if _, err := messaging.NewCounterConsumer(ctx, nil, 16); err == nil {
		t.Error("expected error from NewCounterConsumer with nil client, got nil")
	}
}

func TestPartitionConsumersUninitialized(t *testing.T) {
	// A nil or uninitialized PartitionedDrainerConsumer
	var pdc *messaging.PartitionedDrainerConsumer
	if _, err := pdc.FetchPartition(0, 10); err == nil {
		t.Error("expected error from nil pdc.FetchPartition, got nil")
	}
	if _, err := pdc.Fetch(10); err == nil {
		t.Error("expected error from nil pdc.Fetch, got nil")
	}
	if p := pdc.Partitions(); p != 0 {
		t.Errorf("expected 0 partitions for nil pdc, got %d", p)
	}

	// A nil or uninitialized CounterConsumer
	var cc *messaging.CounterConsumer
	if _, err := cc.FetchPartition(0, 10); err == nil {
		t.Error("expected error from nil cc.FetchPartition, got nil")
	}
	if _, err := cc.Fetch(10); err == nil {
		t.Error("expected error from nil cc.Fetch, got nil")
	}
	if p := cc.Partitions(); p != 0 {
		t.Errorf("expected 0 partitions for nil cc, got %d", p)
	}
}

func TestDrainerConsumerNilClient(t *testing.T) {
	var dc *messaging.DrainerConsumer
	if _, err := dc.Fetch(10); err == nil {
		t.Error("expected error from nil dc.Fetch, got nil")
	}
	if raw := dc.RawConsumer(); raw != nil {
		t.Errorf("expected nil raw consumer from nil dc, got %v", raw)
	}
}

func TestPublishReplyCountsValidation(t *testing.T) {
	client := &messaging.Client{}
	pub := messaging.NewPublisher(client)

	// Nil event
	if err := pub.PublishReplyCountsRecalculated(context.Background(), nil); err == nil {
		t.Error("expected error publishing nil ReplyCountsRecalculatedEvent")
	}

	// Empty counts should be a no-op (return nil without network call)
	evt := &events.ReplyCountsRecalculatedEvent{
		GroupID: "dev",
		Counts:  map[string]int{},
	}
	if err := pub.PublishReplyCountsRecalculated(context.Background(), evt); err != nil {
		t.Errorf("expected nil error for empty counts, got: %v", err)
	}
}

func TestClientPingAndDLQWithNilConn(t *testing.T) {
	var client *messaging.Client
	if err := client.Ping(context.Background()); err == nil {
		t.Error("expected error for nil client Ping")
	}
	if err := client.PublishDLQ(context.Background(), "test.sub", []byte("bad"), "corrupt"); err == nil {
		t.Error("expected error for nil client PublishDLQ")
	}
	if conn := client.Conn(); conn != nil {
		t.Errorf("expected nil conn from nil client, got %v", conn)
	}

	// Client with nil inner connection
	emptyClient := &messaging.Client{}
	if err := emptyClient.Ping(context.Background()); err == nil {
		t.Error("expected error for emptyClient Ping")
	}
	if err := emptyClient.PublishDLQ(context.Background(), "test.sub", []byte("bad"), "corrupt"); err == nil {
		t.Error("expected error for emptyClient PublishDLQ")
	}
	// Calling Close on empty client should not panic
	emptyClient.Close()
}

func TestStartPullConsumerDefaultConfig(t *testing.T) {
	client := &messaging.Client{}
	// Note: client without underlying connection cannot register consumers
	if client.Ping(context.Background()) == nil {
		t.Error("expected client to not be connected")
	}
}
