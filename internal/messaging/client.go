package messaging

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	qpnats "github.com/rlsvr/hirnok/pkg/nats"
	"github.com/rs/zerolog/log"

	"github.com/rlsvr/kozlony/internal/config"
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
//
// Note: currently unused. The drainer path uses NewDrainerConsumer, which pulls
// explicit micro-batches via Fetch rather than running a push handler loop.
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

	dlqSubject := SubjectDeadLetter(c.cfg.NATSStreamName)
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
