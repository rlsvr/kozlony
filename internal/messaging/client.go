package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	qpnats "github.com/rlsvr/hirnok/pkg/nats"
	"github.com/rs/zerolog/log"

	"github.com/rlsvr/kozlony/internal/config"
	"github.com/rlsvr/kozlony/internal/messaging/commands"
	"github.com/rlsvr/kozlony/internal/messaging/events"
)

// Client manages the NATS JetStream connection, pull consumers, and publishers via hirnok.
type Client struct {
	*qpnats.JetStream
	cfg       *config.Config
	conn      *qpnats.Conn
	mu        sync.Mutex
	consumers []*qpnats.Consumer
}

// NewClient establishes a new hirnok connection and initializes the JetStream context.
func NewClient(cfg *config.Config) (*Client, error) {
	if cfg == nil {
		return nil, errors.New("config cannot be nil")
	}

	conn, err := qpnats.Connect(qpnats.Config{
		URL:       cfg.NATSURL,
		Name:      cfg.AppName,
		QueueSize: cfg.NATSConcurrentSubscribers,
		Workers:   cfg.NATSConcurrentSubscribers,
		AckBuffer: time.Millisecond,
	})
	if err != nil {
		return nil, fmt.Errorf("connect to NATS: %w", err)
	}

	js, err := conn.JetStream()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("get JetStream context: %w", err)
	}

	return &Client{
		JetStream: js,
		cfg:       cfg,
		conn:      conn,
	}, nil
}

// Init creates a new Client and initializes it against NATS JetStream.
func Init(ctx context.Context, cfg *config.Config) (*Client, error) {
	c, err := NewClient(cfg)
	if err != nil {
		return nil, err
	}
	if err := c.Init(ctx); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

// Init initializes client dependencies such as ensuring the configured JetStream stream exists.
func (c *Client) Init(ctx context.Context) error {
	if err := c.EnsureStream(ctx); err != nil {
		return fmt.Errorf("ensure stream: %w", err)
	}
	log.Info().Str("stream", c.cfg.NATSStreamName).Msg("ensured NATS JetStream stream")
	return nil
}

// EnsureStream idempotently creates or updates the configured JetStream stream with sliding retention.
func (c *Client) EnsureStream(ctx context.Context) error {
	streamCfg := jetstream.StreamConfig{
		Name:      c.cfg.NATSStreamName,
		Subjects:  []string{SubjectStreamFilter(c.cfg.NATSStreamName)},
		Retention: jetstream.LimitsPolicy,
		Storage:   jetstream.FileStorage,
		MaxAge:    c.cfg.NATSStreamRetention,
	}

	_, err := c.Raw().CreateOrUpdateStream(ctx, streamCfg)
	if err != nil {
		return fmt.Errorf("create or update stream %q: %w", c.cfg.NATSStreamName, err)
	}
	return nil
}

// StartPullConsumer creates and starts a durable pull consumer on the stream.
func (c *Client) StartPullConsumer(
	ctx context.Context,
	cfg jetstream.ConsumerConfig,
	h qpnats.JetHandler,
	opts ...qpnats.Option,
) (*qpnats.Consumer, error) {
	if cfg.Durable == "" {
		cfg.Durable = c.cfg.NATSConsumerName
	}
	if cfg.AckWait == 0 {
		cfg.AckWait = c.cfg.NATSAckWait
	}
	if cfg.MaxAckPending == 0 {
		cfg.MaxAckPending = c.cfg.NATSConcurrentSubscribers
	}
	cfg.AckPolicy = jetstream.AckExplicitPolicy

	consumerOpts := append([]qpnats.Option{
		qpnats.WithQueueSize(c.cfg.NATSConcurrentSubscribers),
		qpnats.WithWorkers(c.cfg.NATSConcurrentSubscribers),
		qpnats.WithAckBuffer(time.Millisecond),
	}, opts...)

	consumer, err := c.NewConsumer(ctx, c.cfg.NATSStreamName, cfg, h, consumerOpts...)
	if err != nil {
		return nil, fmt.Errorf("start JetStream pull consumer: %w", err)
	}

	c.mu.Lock()
	c.consumers = append(c.consumers, consumer)
	c.mu.Unlock()

	return consumer, nil
}

// CreateDrainerConsumer creates or updates the durable pull consumer used by the batch drainer.
func (c *Client) CreateDrainerConsumer(ctx context.Context) (jetstream.Consumer, error) {
	stream, err := c.Raw().Stream(ctx, c.cfg.NATSStreamName)
	if err != nil {
		return nil, fmt.Errorf("get stream: %w", err)
	}

	consumerCfg := jetstream.ConsumerConfig{
		Durable:       c.cfg.NATSConsumerName,
		FilterSubject: SubjectAllEvents(c.cfg.NATSStreamName),
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckWait:       c.cfg.NATSAckWait,
	}

	cons, err := stream.CreateOrUpdateConsumer(ctx, consumerCfg)
	if err != nil {
		return nil, fmt.Errorf("create or update drainer consumer: %w", err)
	}
	return cons, nil
}

// PublishInteractionCreated publishes an InteractionCreatedEvent with Nats-Msg-Id deduplication.
func (c *Client) PublishInteractionCreated(ctx context.Context, evt *events.InteractionCreatedEvent) (*jetstream.PubAck, error) {
	if evt == nil {
		return nil, errors.New("event cannot be nil")
	}

	data, err := json.Marshal(evt)
	if err != nil {
		return nil, fmt.Errorf("marshal interaction created event: %w", err)
	}

	subject := SubjectInteractionCreated(c.cfg.NATSStreamName, evt.GroupID)
	msg := &natsio.Msg{
		Subject: subject,
		Data:    data,
		Header:  natsio.Header{},
	}
	if evt.ID != "" {
		msg.Header.Set(jetstream.MsgIDHeader, evt.ID)
	}

	return c.PublishMsg(ctx, msg)
}

// PublishInteractionEdited publishes an InteractionEditedEvent.
func (c *Client) PublishInteractionEdited(ctx context.Context, evt *events.InteractionEditedEvent) (*jetstream.PubAck, error) {
	if evt == nil {
		return nil, errors.New("event cannot be nil")
	}

	data, err := json.Marshal(evt)
	if err != nil {
		return nil, fmt.Errorf("marshal interaction edited event: %w", err)
	}

	subject := SubjectInteractionEdited(c.cfg.NATSStreamName, evt.GroupID)
	return c.Publish(ctx, subject, data)
}

// PublishCreateInteractionCmd publishes a CreateInteractionCommand.
func (c *Client) PublishCreateInteractionCmd(ctx context.Context, groupID string, cmd *commands.CreateInteractionCommand) (*jetstream.PubAck, error) {
	if cmd == nil {
		return nil, errors.New("command cannot be nil")
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("marshal create interaction command: %w", err)
	}

	subject := SubjectCreateInteractionCmd(c.cfg.NATSStreamName, groupID)
	return c.Publish(ctx, subject, data)
}

// SubscribeEvents subscribes to live interaction events for a given group (or all groups if nil)
// and returns a receive-only channel of raw JSON event payloads and a cleanup function.
func (c *Client) SubscribeEvents(_ context.Context, groupID *string) (<-chan []byte, func(), error) {
	if c == nil || c.conn == nil || c.conn.Raw() == nil {
		return nil, nil, errors.New("nats client not connected")
	}

	subject := SubjectAllEvents(c.cfg.NATSStreamName)
	if groupID != nil && *groupID != "" {
		subject = fmt.Sprintf("%s.%s.evt.>", c.cfg.NATSStreamName, *groupID)
	}

	msgChan := make(chan []byte, 256)
	sub, err := c.conn.Raw().Subscribe(subject, func(m *natsio.Msg) {
		select {
		case msgChan <- append([]byte(nil), m.Data...):
		default:
			log.Warn().Str("subject", m.Subject).Msg("sse event buffer full, dropping message")
		}
	})
	if err != nil {
		return nil, nil, fmt.Errorf("subscribe to nats subject %s: %w", subject, err)
	}

	cleanup := func() {
		_ = sub.Unsubscribe()
	}
	return msgChan, cleanup, nil
}

// Conn returns the underlying hirnok Conn.
func (c *Client) Conn() *qpnats.Conn {
	return c.conn
}

// Ping checks whether the NATS connection is active.
func (c *Client) Ping(_ context.Context) error {
	if c == nil || c.conn == nil || c.conn.Raw() == nil || !c.conn.Raw().IsConnected() {
		return errors.New("nats client not connected")
	}
	return nil
}

// PublishDLQ publishes a poison or unprocessable message to the dead letter queue stream.
func (c *Client) PublishDLQ(ctx context.Context, originalSubject string, data []byte, reason string) error {
	if c == nil || c.conn == nil {
		return errors.New("nats client not connected")
	}

	dlqSubject := fmt.Sprintf("%s.dlq", c.cfg.NATSStreamName)
	msg := &natsio.Msg{
		Subject: dlqSubject,
		Data:    data,
		Header:  natsio.Header{},
	}
	msg.Header.Set("X-DLQ-Reason", reason)
	msg.Header.Set("X-Original-Subject", originalSubject)
	_, err := c.PublishMsg(ctx, msg)
	return err
}

// Close gracefully stops all consumers and closes the NATS connection.
func (c *Client) Close() {
	c.mu.Lock()
	for _, cons := range c.consumers {
		cons.Stop()
	}
	c.consumers = nil
	c.mu.Unlock()

	if c.conn != nil {
		c.conn.Close()
	}
}
