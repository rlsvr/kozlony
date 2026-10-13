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
	"github.com/rs/zerolog/log"

	"github.com/rlsvr/kozlony/internal/messaging/commands"
	"github.com/rlsvr/kozlony/internal/messaging/events"
	"github.com/rlsvr/kozlony/internal/messaging/fanout"
)

// Publisher embeds *Client and provides methods to publish domain events and commands to JetStream,
// while also managing SSE subscriber fan-out via a shared broker consumer and async pipeline.
type Publisher struct {
	*Client
	fanoutPipeline *fanout.Pipeline
	fanoutHub      *fanout.Hub
}

// NewPublisher initializes a new Publisher embedding the provided Client.
func NewPublisher(client *Client) *Publisher {
	p := &Publisher{
		Client: client,
	}

	if client != nil && client.conn != nil && client.conn.Raw() != nil {
		hub := fanout.NewHub(nil, client.conn.Raw(), client.cfg.NATSStreamName)
		p.fanoutPipeline = fanout.NewPipeline(
			context.Background(),
			5*time.Millisecond,
			128,
			fanout.WithGroupListener(hub),
		)
		p.fanoutHub = hub
		hub.SetPipeline(p.fanoutPipeline)
		if err := p.fanoutHub.Start(context.Background()); err != nil {
			log.Warn().Err(err).Msg("failed to start fan-out hub")
		}
	}

	return p
}

// PublishInteractionCreated publishes an InteractionCreatedEvent with Nats-Msg-Id deduplication.
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

	subject := SubjectInteractionCreated(p.cfg.NATSStreamName, evt.GroupID)
	msg := &natsio.Msg{
		Subject: subject,
		Data:    data,
		Header:  natsio.Header{},
	}
	if evt.ID != "" {
		msg.Header.Set(jetstream.MsgIDHeader, evt.ID)
	}

	return p.PublishMsg(ctx, msg)
}

// PublishInteractionEdited publishes an InteractionEditedEvent.
func (p *Publisher) PublishInteractionEdited(ctx context.Context, evt *events.InteractionEditedEvent) (*jetstream.PubAck, error) {
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

	subject := SubjectInteractionEdited(p.cfg.NATSStreamName, evt.GroupID)
	return p.Publish(ctx, subject, data)
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
//
// The subject lives outside the "evt." namespace, so the drainer's filter
// (BOARD.<group>.evt.>) never matches it and the drainer cannot consume its own output.
// No Nats-Msg-Id is set: redelivery is already harmless because counts are absolute, not
// deltas, and JetStream's dedup window would not span the full retry horizon anyway.
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
		p.fanoutPipeline.Remove(client)
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
