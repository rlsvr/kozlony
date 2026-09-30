package messaging

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

// DrainerConsumer embeds *Client and manages the JetStream pull consumer for micro-batch ingestion.
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

	consumerCfg := jetstream.ConsumerConfig{
		Durable:       client.cfg.NATSConsumerName,
		FilterSubject: SubjectAllEvents(client.cfg.NATSStreamName),
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckWait:       client.cfg.NATSAckWait,
	}

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
