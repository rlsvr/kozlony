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

// SubscribeEvents subscribes to live interaction events for a given group (or all groups if nil)
// and returns a receive-only channel of raw JSON event payloads and a cleanup function.
func (p *Publisher) SubscribeEvents(_ context.Context, groupID *string) (<-chan []byte, func(), error) {
	if p == nil || p.Client == nil || p.conn == nil || p.conn.Raw() == nil {
		return nil, nil, errors.New("nats client not connected")
	}

	subject := SubjectAllEvents(p.cfg.NATSStreamName)
	if groupID != nil && *groupID != "" {
		subject = fmt.Sprintf("%s.%s.evt.>", p.cfg.NATSStreamName, *groupID)
	}

	msgChan := make(chan []byte, 256)
	sub, err := p.conn.Raw().Subscribe(subject, func(m *natsio.Msg) {
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
