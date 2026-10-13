package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"time"

	"github.com/google/uuid"
	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/rlsvr/kozlony/internal/messaging/commands"
	"github.com/rlsvr/kozlony/internal/messaging/events"
	"github.com/rlsvr/kozlony/internal/messaging/fanout"
)

type liveMsg struct {
	subject string
	data    []byte
}

// Publisher embeds *Client and provides methods to publish domain events and commands to JetStream,
// while also managing SSE subscriber fan-out via an integrated async pipeline with Hirnok worker pool.
type Publisher struct {
	*Client
	fanoutPipeline *fanout.Pipeline
	liveCh         chan liveMsg
	liveStop       chan struct{}
}

// NewPublisher initializes a new Publisher embedding the provided Client.
func NewPublisher(client *Client) *Publisher {
	p := &Publisher{
		Client:   client,
		liveCh:   make(chan liveMsg, 65536),
		liveStop: make(chan struct{}),
	}

	if client != nil && client.conn != nil && client.conn.Raw() != nil {
		livePrefix := client.cfg.LivePrefix
		if livePrefix == "" {
			livePrefix = DefaultLivePrefix
		}
		p.fanoutPipeline = fanout.NewPipeline(
			context.Background(),
			15*time.Millisecond,
			128,
			fanout.WithNATSConn(client.conn.Raw(), livePrefix),
		)
		go p.runLivePublisher()
	}

	return p
}

func (p *Publisher) publishLiveAsync(subject string, data []byte) {
	if p == nil || p.liveCh == nil || subject == "" || len(data) == 0 {
		return
	}
	select {
	case p.liveCh <- liveMsg{subject: subject, data: data}:
	default:
		// Queue full under extreme burst pressure, drop rather than blocking write path
	}
}

func (p *Publisher) runLivePublisher() {
	if p.Client == nil || p.conn == nil || p.conn.Raw() == nil {
		return
	}
	rawNC := p.conn.Raw()
	for {
		select {
		case <-p.liveStop:
			return
		case msg := <-p.liveCh:
			_ = rawNC.Publish(msg.subject, msg.data)
		}
	}
}

// PublishInteractionCreated publishes an InteractionCreatedEvent to JetStream partitioned ingestion
// and broadcasts asynchronously to Core NATS live topics.
func (p *Publisher) PublishInteractionCreated(ctx context.Context, evt *events.InteractionCreatedEvent) (*jetstream.PubAck, error) {
	if evt == nil {
		return nil, errors.New("event cannot be nil")
	}
	if p == nil || p.Client == nil {
		return nil, errors.New("publisher client not initialized")
	}

	data, err := json.Marshal(evt)
	if err != nil {
		return nil, fmt.Errorf("marshal interaction created event: %w", err)
	}

	threadRoot := evt.ID
	if evt.RootID != nil && *evt.RootID != "" {
		threadRoot = *evt.RootID
	}
	numPartitions := p.cfg.IngestPartitions
	if numPartitions <= 0 {
		numPartitions = DefaultIngestPartitions
	}
	partition := PartitionForThread(threadRoot, numPartitions)
	subject := SubjectIngestPartition(partition)

	msg := &natsio.Msg{
		Subject: subject,
		Data:    data,
		Header:  natsio.Header{},
	}
	if evt.ID != "" {
		msg.Header.Set(jetstream.MsgIDHeader, evt.ID)
	}
	msg.Header.Set("X-Group-ID", evt.GroupID)
	msg.Header.Set("X-Thread-ID", threadRoot)

	ack, err := p.PublishMsg(ctx, msg)
	if err != nil {
		return nil, err
	}

	// Non-blocking real-time fan-out via ephemeral Core NATS (zero retention)
	livePrefix := p.cfg.LivePrefix
	if livePrefix == "" {
		livePrefix = DefaultLivePrefix
	}
	p.publishLiveAsync(SubjectLiveFeed(livePrefix, evt.GroupID), data)
	if evt.RootID != nil && *evt.RootID != "" {
		p.publishLiveAsync(SubjectLiveThread(livePrefix, evt.GroupID, *evt.RootID), data)
	}

	return ack, nil
}

// PublishInteractionEdited publishes an InteractionEditedEvent to live Core NATS topics.
func (p *Publisher) PublishInteractionEdited(_ context.Context, evt *events.InteractionEditedEvent) (*jetstream.PubAck, error) {
	if evt == nil {
		return nil, errors.New("event cannot be nil")
	}
	if p == nil || p.Client == nil {
		return nil, errors.New("publisher client not initialized")
	}

	data, err := json.Marshal(evt)
	if err != nil {
		return nil, fmt.Errorf("marshal interaction edited event: %w", err)
	}

	livePrefix := p.cfg.LivePrefix
	if livePrefix == "" {
		livePrefix = DefaultLivePrefix
	}
	p.publishLiveAsync(SubjectLiveFeed(livePrefix, evt.GroupID), data)
	p.publishLiveAsync(SubjectLiveThread(livePrefix, evt.GroupID, evt.ID), data)

	return nil, nil
}

// PublishCreateInteractionCmd publishes a CreateInteractionCommand.
func (p *Publisher) PublishCreateInteractionCmd(ctx context.Context, groupID string, cmd *commands.CreateInteractionCommand) (*jetstream.PubAck, error) {
	if cmd == nil {
		return nil, errors.New("command cannot be nil")
	}
	if p == nil || p.Client == nil {
		return nil, errors.New("publisher client not initialized")
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("marshal create interaction command: %w", err)
	}

	subject := SubjectCreateInteractionCmd(p.cfg.NATSStreamName, groupID)
	return p.Publish(ctx, subject, data)
}

// PublishReplyCountsRecalculated publishes absolute reply counts for the counter consumer.
func (p *Publisher) PublishReplyCountsRecalculated(ctx context.Context, evt *events.ReplyCountsRecalculatedEvent) error {
	if evt == nil {
		return errors.New("event cannot be nil")
	}
	if p == nil || p.Client == nil {
		return errors.New("publisher client not initialized")
	}
	if len(evt.Counts) == 0 {
		return nil
	}

	data, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal reply counts recalculated event: %w", err)
	}

	subject := SubjectReplyCountsRecalculated(p.cfg.NATSStreamName, evt.GroupID)
	if _, err := p.Publish(ctx, subject, data); err != nil {
		return fmt.Errorf("publish reply counts: %w", err)
	}

	// Also broadcast asynchronously to live topics for connected SSE clients
	livePrefix := p.cfg.LivePrefix
	if livePrefix == "" {
		livePrefix = DefaultLivePrefix
	}
	p.publishLiveAsync(SubjectLiveFeed(livePrefix, evt.GroupID), data)
	for threadID := range evt.Counts {
		p.publishLiveAsync(SubjectLiveThread(livePrefix, evt.GroupID, threadID), data)
	}
	return nil
}

// SubscribeFilteredEvents subscribes to live interaction events for a given group and optional thread rootID
// using the shared fan-out pipeline, eliminating per-connection NATS subscriptions.
func (p *Publisher) SubscribeFilteredEvents(_ context.Context, groupID *string, rootID *uuid.UUID) (<-chan []byte, func(), error) {
	if p == nil || p.Client == nil || p.conn == nil || p.conn.Raw() == nil {
		return nil, nil, errors.New("nats client not connected")
	}
	if p.fanoutPipeline == nil {
		return nil, nil, errors.New("fanout pipeline not initialized")
	}

	gID := DefaultGroupID
	if groupID != nil && *groupID != "" {
		gID = *groupID
	}

	clientID := uuid.New().String()
	client := fanout.NewClient(clientID, gID, rootID, 256)
	p.fanoutPipeline.RegisterAndWait(client)

	cleanup := func() {
		p.fanoutPipeline.RemoveAndWait(client)
		client.Close()
	}

	return client.Events, cleanup, nil
}

// SubscribeEvents subscribes to live interaction events for a given group (or all groups if nil)
// and returns a receive-only channel of raw JSON event payloads and a cleanup function.
// It delegates to SubscribeFilteredEvents without a rootID filter.
func (p *Publisher) SubscribeEvents(ctx context.Context, groupID *string) (<-chan []byte, func(), error) {
	return p.SubscribeFilteredEvents(ctx, groupID, nil)
}
