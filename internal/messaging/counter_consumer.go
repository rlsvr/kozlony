package messaging

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	qpnats "github.com/rlsvr/hirnok/pkg/nats"
)

// CounterConsumer embeds *Client and manages N durable pull consumers, one per counter partition.
type CounterConsumer struct {
	*Client
	consumers []jetstream.Consumer
}

// NewCounterConsumer creates or updates durable pull consumers for each counter partition.
func NewCounterConsumer(ctx context.Context, client *Client, numPartitions int) (*CounterConsumer, error) {
	if client == nil {
		return nil, errors.New("client cannot be nil")
	}
	if numPartitions <= 0 {
		numPartitions = client.cfg.IngestPartitions
		if numPartitions <= 0 {
			numPartitions = DefaultCounterPartitions
		}
	}

	stream, err := client.Raw().Stream(ctx, client.cfg.NATSStreamName)
	if err != nil {
		return nil, fmt.Errorf("get stream: %w", err)
	}

	maxAckPending := client.cfg.NATSConcurrentSubscribers * client.cfg.DrainBatchSize * 2
	if maxAckPending < 20000 {
		maxAckPending = 20000
	}
	maxAckPendingPerPart := maxAckPending / numPartitions
	if maxAckPendingPerPart < 2000 {
		maxAckPendingPerPart = 2000
	}

	consumers := make([]jetstream.Consumer, numPartitions)
	for p := 0; p < numPartitions; p++ {
		consumerCfg := qpnats.DefaultConsumerConfig(fmt.Sprintf("%s-%d", client.cfg.NATSCounterConsumerName, p))
		consumerCfg.FilterSubject = SubjectCounterPartition(p)
		if client.cfg.NATSAckWait > 0 {
			consumerCfg.AckWait = client.cfg.NATSAckWait
		}
		consumerCfg.MaxAckPending = maxAckPendingPerPart
		consumerCfg.MaxDeliver = 20
		consumerCfg.BackOff = []time.Duration{100 * time.Millisecond, 250 * time.Millisecond, time.Second, 2 * time.Second}

		cons, err := stream.CreateOrUpdateConsumer(ctx, consumerCfg)
		if err != nil {
			return nil, fmt.Errorf("create or update counter partition %d consumer: %w", p, err)
		}
		consumers[p] = cons
	}

	return &CounterConsumer{
		Client:    client,
		consumers: consumers,
	}, nil
}

// FetchPartition fetches a micro-batch of reply count events from the consumer for the given partition.
func (cc *CounterConsumer) FetchPartition(partition int, batch int, opts ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	if cc == nil || len(cc.consumers) == 0 {
		return nil, errors.New("counter consumer not initialized")
	}
	p := partition % len(cc.consumers)
	if p < 0 {
		p += len(cc.consumers)
	}
	return cc.consumers[p].Fetch(batch, opts...)
}

// Fetch satisfies the BatchFetcher interface by fetching from partition 0.
func (cc *CounterConsumer) Fetch(batch int, opts ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	return cc.FetchPartition(0, batch, opts...)
}

// Partitions returns the number of active partition consumers.
func (cc *CounterConsumer) Partitions() int {
	if cc == nil {
		return 0
	}
	return len(cc.consumers)
}
