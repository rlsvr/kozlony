package messaging

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
	qpnats "github.com/rlsvr/hirnok/pkg/nats"
)

// PartitionedDrainerConsumer embeds *Client and manages N durable pull consumers, one per partition.
type PartitionedDrainerConsumer struct {
	*Client
	consumers []jetstream.Consumer
}

// NewPartitionedDrainerConsumer creates or updates durable pull consumers for each ingestion partition.
func NewPartitionedDrainerConsumer(ctx context.Context, client *Client, numPartitions int) (*PartitionedDrainerConsumer, error) {
	if client == nil {
		return nil, errors.New("client cannot be nil")
	}
	if numPartitions <= 0 {
		numPartitions = client.cfg.IngestPartitions
		if numPartitions <= 0 {
			numPartitions = DefaultIngestPartitions
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
		consumerCfg := qpnats.DefaultConsumerConfig(fmt.Sprintf("%s-%d", client.cfg.NATSConsumerName, p))
		consumerCfg.FilterSubject = SubjectIngestPartition(p)
		if client.cfg.NATSAckWait > 0 {
			consumerCfg.AckWait = client.cfg.NATSAckWait
		}
		consumerCfg.MaxAckPending = maxAckPendingPerPart

		cons, err := stream.CreateOrUpdateConsumer(ctx, consumerCfg)
		if err != nil {
			return nil, fmt.Errorf("create or update partition %d consumer: %w", p, err)
		}
		consumers[p] = cons
	}

	return &PartitionedDrainerConsumer{
		Client:    client,
		consumers: consumers,
	}, nil
}

// FetchPartition fetches a micro-batch of messages from the consumer for the given partition.
func (pdc *PartitionedDrainerConsumer) FetchPartition(partition int, batch int, opts ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	if pdc == nil || len(pdc.consumers) == 0 {
		return nil, errors.New("partitioned drainer consumer not initialized")
	}
	p := partition % len(pdc.consumers)
	if p < 0 {
		p += len(pdc.consumers)
	}
	return pdc.consumers[p].Fetch(batch, opts...)
}

// Fetch satisfies the BatchFetcher interface by fetching from partition 0.
func (pdc *PartitionedDrainerConsumer) Fetch(batch int, opts ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	return pdc.FetchPartition(0, batch, opts...)
}

// Partitions returns the number of active partition consumers.
func (pdc *PartitionedDrainerConsumer) Partitions() int {
	if pdc == nil {
		return 0
	}
	return len(pdc.consumers)
}

// DrainerConsumer embeds *Client and manages the JetStream pull consumer for micro-batch ingestion.
// Deprecated: use PartitionedDrainerConsumer for high-throughput partitioned ingestion.
type DrainerConsumer struct {
	*Client
	consumer jetstream.Consumer
}

// NewDrainerConsumer creates or updates the durable pull consumer used by the batch drainer.
func NewDrainerConsumer(ctx context.Context, client *Client) (*DrainerConsumer, error) {
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

	consumerCfg := qpnats.DefaultConsumerConfig(client.cfg.NATSConsumerName)
	consumerCfg.FilterSubject = SubjectIngestFilter()
	if client.cfg.NATSAckWait > 0 {
		consumerCfg.AckWait = client.cfg.NATSAckWait
	}
	consumerCfg.MaxAckPending = maxAckPending

	cons, err := stream.CreateOrUpdateConsumer(ctx, consumerCfg)
	if err != nil {
		return nil, fmt.Errorf("create or update drainer consumer: %w", err)
	}

	return &DrainerConsumer{
		Client:   client,
		consumer: cons,
	}, nil
}

// Fetch fetches a micro-batch of messages from the JetStream pull consumer.
func (dc *DrainerConsumer) Fetch(batch int, opts ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	if dc == nil || dc.consumer == nil {
		return nil, errors.New("drainer consumer not initialized")
	}
	return dc.consumer.Fetch(batch, opts...)
}

// RawConsumer returns the underlying jetstream.Consumer.
func (dc *DrainerConsumer) RawConsumer() jetstream.Consumer {
	if dc == nil {
		return nil
	}
	return dc.consumer
}
