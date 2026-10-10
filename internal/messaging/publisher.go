package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"

	"github.com/rlsvr/kozlony/internal/messaging/commands"
	"github.com/rlsvr/kozlony/internal/messaging/events"
)

// Publisher embeds *Client and provides methods to publish domain events and commands to JetStream.
type Publisher struct {
	*Client
}

// NewPublisher initializes a new Publisher embedding the provided Client.
func NewPublisher(client *Client) *Publisher {
	return &Publisher{
		Client: client,
	}
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

// SubscribeEvents subscribes to live interaction events for a given group (or all groups if nil)
// and returns a receive-only channel of raw JSON event payloads and a cleanup function.
func (p *Publisher) SubscribeEvents(_ context.Context, groupID *string) (<-chan []byte, func(), error) {
	if p == nil || p.Client == nil || p.conn == nil || p.conn.Raw() == nil {
		return nil, nil, errors.New("nats client not connected")
	}

	// Two namespaces are fanned out to SSE clients: creations on the "evt." subjects and
	// edit notifications on the "audit." subjects. Edits live outside "evt." precisely so
	// the drainer cannot consume them, which means SSE has to listen on both explicitly.
	createdSubject := SubjectAllEvents(p.cfg.NATSStreamName)
	auditSubject := SubjectAllAuditEvents(p.cfg.NATSStreamName)
	if groupID != nil && *groupID != "" {
		createdSubject = fmt.Sprintf("%s.%s.evt.>", p.cfg.NATSStreamName, *groupID)
		auditSubject = SubjectGroupAuditEvents(p.cfg.NATSStreamName, *groupID)
	}

	msgChan := make(chan []byte, 256)
	handler := func(m *natsio.Msg) {
		select {
		case msgChan <- append([]byte(nil), m.Data...):
		default:
			log.Warn().Str("subject", m.Subject).Msg("sse event buffer full, dropping message")
		}
	}

	raw := p.conn.Raw()
	subs := make([]*natsio.Subscription, 0, 2)
	for _, subject := range []string{createdSubject, auditSubject} {
		sub, err := raw.Subscribe(subject, handler)
		if err != nil {
			for _, done := range subs {
				_ = done.Unsubscribe()
			}
			return nil, nil, fmt.Errorf("subscribe to nats subject %s: %w", subject, err)
		}
		subs = append(subs, sub)
	}

	cleanup := func() {
		for _, sub := range subs {
			_ = sub.Unsubscribe()
		}
	}
	return msgChan, cleanup, nil
}
