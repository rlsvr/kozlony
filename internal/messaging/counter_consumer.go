package messaging

import (
	"context"
	"errors"
	"fmt"

	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// CounterConsumer embeds *Client and manages the JetStream pull consumer that applies
// recalculated reply counts.
//
// It is a second durable pull consumer on the same stream as the drainer, filtered to the
// counter subject. Its purpose is to keep reply_count writes out of the drainer's insert
// transaction: the drainer only inserts rows, this consumer writes counters in its own
// small transactions, so row locks on hot threads are held for microseconds rather than
// for a whole batch.
type CounterConsumer struct {
	*Client
	consumer jetstream.Consumer
}

// NewCounterConsumer creates or updates the durable pull consumer for reply count updates.
func NewCounterConsumer(ctx context.Context, client *Client) (*CounterConsumer, error) {
	if client == nil {
		return nil, errors.New("client cannot be nil")
	}

	stream, err := client.Raw().Stream(ctx, client.cfg.NATSStreamName)
	if err != nil {
		return nil, fmt.Errorf("get stream: %w", err)
	}

	maxAckPending := client.cfg.NATSConcurrentSubscribers * client.cfg.DrainBatchSize * 2
	if maxAckPending < 20000 {
		maxAckPending = 20000
	}

	consumerCfg := jetstream.ConsumerConfig{
		Durable:       client.cfg.NATSCounterConsumerName,
		FilterSubject: SubjectAllReplyCounts(client.cfg.NATSStreamName),
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckWait:       client.cfg.NATSAckWait,
		MaxAckPending: maxAckPending,
		// Counts are redelivered while their parent row is still uncommitted. Cap the
		// attempts so an event referencing an interaction that never arrives is parked
		// instead of being retried forever, and back off between attempts.
		MaxDeliver: 20,
		BackOff:    []time.Duration{100 * time.Millisecond, 250 * time.Millisecond, time.Second, 2 * time.Second},
	}

	cons, err := stream.CreateOrUpdateConsumer(ctx, consumerCfg)
	if err != nil {
		return nil, fmt.Errorf("create or update counter consumer: %w", err)
	}

	return &CounterConsumer{
		Client:   client,
		consumer: cons,
	}, nil
}

// Fetch fetches a micro-batch of reply count events.
func (cc *CounterConsumer) Fetch(batch int, opts ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	if cc == nil || cc.consumer == nil {
		return nil, errors.New("counter consumer not initialized")
	}
	return cc.consumer.Fetch(batch, opts...)
}

// RawConsumer returns the underlying jetstream.Consumer.
func (cc *CounterConsumer) RawConsumer() jetstream.Consumer {
	if cc == nil {
		return nil
	}
	return cc.consumer
}
