package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rlsvr/blazesse"

	"github.com/rlsvr/kozlony/internal/messaging/commands"
	"github.com/rlsvr/kozlony/internal/messaging/events"
)

type liveMsg struct {
	subject string
	data    []byte
}

// Publisher embeds *Client and provides methods to publish domain events and commands to JetStream,
// while also managing dynamic per-topic Core NATS subscriptions and SSE subscriber fan-out via blazesse.Broadcaster.
type Publisher struct {
	*Client
	broadcaster *blazesse.Broadcaster
	liveCh      chan liveMsg
	liveStop    chan struct{}

	topicMu     sync.Mutex
	topicCounts map[string]int
	topicSubs   map[string]*natsio.Subscription
}

// NewPublisher initializes a new Publisher embedding the provided Client.
func NewPublisher(client *Client) *Publisher {
	p := &Publisher{
		Client:      client,
		liveCh:      make(chan liveMsg, 65536),
		liveStop:    make(chan struct{}),
		topicCounts: make(map[string]int),
		topicSubs:   make(map[string]*natsio.Subscription),
	}

	p.broadcaster = blazesse.NewBroadcaster(
		blazesse.NewEngineLockFree(
			blazesse.WithReconfigInterval(10*time.Millisecond),
			blazesse.WithReconfigBatchSize(256),
			blazesse.WithReconfigWait(false),
		),
		blazesse.WithDeliveryMode(blazesse.DeliveryFIFO),
		blazesse.WithFIFOBufferSize(512),
		blazesse.WithOverflowPolicy(blazesse.OverflowDropOldest),
		blazesse.WithWriteTimeout(3*time.Second),
		blazesse.WithHeartbeat(15*time.Second, 5*time.Second),
	)

	if client != nil && client.Conn() != nil && client.Conn().Raw() != nil {
		go p.runLivePublisher()
	}

	return p
}

// SubscribeTopic subscribes a client to a live topic (feedID or threadUUID), establishing a single
// Core NATS subscription on LIVE.<topic> for the first subscriber on this node, and reusing it
// for all subsequent subscribers via blazesse.Broadcaster.
func (p *Publisher) SubscribeTopic(topic string) (blazesse.Subscriber, func(), error) {
	if p == nil || p.broadcaster == nil {
		return nil, nil, errors.New("broadcaster not initialized")
	}
	if topic == "" {
		topic = DefaultGroupID
	}

	p.topicMu.Lock()
	count := p.topicCounts[topic]
	p.topicCounts[topic] = count + 1

	if count == 0 {
		if err := p.ensureNATSSubscribed(topic); err != nil {
			p.topicCounts[topic]--
			if p.topicCounts[topic] == 0 {
				delete(p.topicCounts, topic)
			}
			p.topicMu.Unlock()
			return nil, nil, err
		}
	}
	p.topicMu.Unlock()

	bSub, bUnsub := p.broadcaster.Subscribe(topic)

	var unsubOnce sync.Once
	teardown := func() {
		unsubOnce.Do(func() {
			bUnsub()

			p.topicMu.Lock()
			remaining := p.topicCounts[topic] - 1
			if remaining <= 0 {
				delete(p.topicCounts, topic)
				if sub, ok := p.topicSubs[topic]; ok {
					_ = sub.Unsubscribe()
					delete(p.topicSubs, topic)
				}
			} else {
				p.topicCounts[topic] = remaining
			}
			p.topicMu.Unlock()
		})
	}

	return bSub, teardown, nil
}

func (p *Publisher) ensureNATSSubscribed(topic string) error {
	if p.Client == nil || p.Conn() == nil || p.Conn().Raw() == nil {
		return nil
	}
	prefix := p.cfg.LivePrefix
	if prefix == "" {
		prefix = DefaultLivePrefix
	}
	subject := SubjectLiveTopic(prefix, topic)
	rawNC := p.Conn().Raw()
	targetTopic := topic
	sub, err := rawNC.Subscribe(subject, func(m *natsio.Msg) {
		if p.broadcaster != nil {
			p.broadcaster.Publish(targetTopic, blazesse.TextFrame(string(m.Data)))
		}
	})
	if err != nil {
		return fmt.Errorf("subscribe to live NATS topic %q: %w", subject, err)
	}
	p.topicSubs[topic] = sub
	return nil
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
	if p.Client == nil || p.Conn() == nil || p.Conn().Raw() == nil {
		return
	}
	rawNC := p.Conn().Raw()
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
	p.publishLiveAsync(SubjectLiveTopic(livePrefix, evt.GroupID), data)
	if evt.RootID != nil && *evt.RootID != "" {
		p.publishLiveAsync(SubjectLiveTopic(livePrefix, *evt.RootID), data)
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
	p.publishLiveAsync(SubjectLiveTopic(livePrefix, evt.GroupID), data)
	p.publishLiveAsync(SubjectLiveTopic(livePrefix, evt.ID), data)

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

	numPartitions := p.cfg.IngestPartitions
	if numPartitions <= 0 {
		numPartitions = DefaultCounterPartitions
	}
	partition := PartitionForGroup(evt.GroupID, numPartitions)
	subject := SubjectCounterPartition(partition)

	msg := &natsio.Msg{
		Subject: subject,
		Data:    data,
		Header:  natsio.Header{},
	}
	msg.Header.Set("X-Group-ID", evt.GroupID)

	if _, err := p.PublishMsg(ctx, msg); err != nil {
		return fmt.Errorf("publish reply counts: %w", err)
	}

	// Also broadcast asynchronously to live topics for connected SSE clients
	livePrefix := p.cfg.LivePrefix
	if livePrefix == "" {
		livePrefix = DefaultLivePrefix
	}
	p.publishLiveAsync(SubjectLiveTopic(livePrefix, evt.GroupID), data)
	for threadID := range evt.Counts {
		p.publishLiveAsync(SubjectLiveTopic(livePrefix, threadID), data)
	}
	return nil
}

// Broadcaster returns the underlying blazesse.Broadcaster engine.
func (p *Publisher) Broadcaster() *blazesse.Broadcaster {
	if p == nil {
		return nil
	}
	return p.broadcaster
}

// Close gracefully stops the publisher, Core NATS live subscriptions, and the underlying broadcaster.
func (p *Publisher) Close() {
	if p == nil {
		return
	}

	p.topicMu.Lock()
	for topic, sub := range p.topicSubs {
		_ = sub.Unsubscribe()
		delete(p.topicSubs, topic)
	}
	p.topicCounts = make(map[string]int)
	p.topicMu.Unlock()

	if p.liveStop != nil {
		select {
		case <-p.liveStop:
		default:
			close(p.liveStop)
		}
	}

	if p.broadcaster != nil {
		_ = p.broadcaster.Close()
	}

	if p.Client != nil {
		p.Client.Close()
	}
}

// SubscribeFilteredEvents subscribes to live interaction events for a given group and optional thread rootID
// using the shared blazesse.Broadcaster, eliminating per-connection NATS subscriptions.
func (p *Publisher) SubscribeFilteredEvents(ctx context.Context, groupID *string, rootID *uuid.UUID) (<-chan []byte, func(), error) {
	if p == nil || p.broadcaster == nil {
		return nil, nil, errors.New("broadcaster not initialized")
	}

	targetTopic := DefaultGroupID
	if groupID != nil && *groupID != "" {
		targetTopic = *groupID
	}
	if rootID != nil {
		targetTopic = rootID.String()
	}

	sub, unsub, err := p.SubscribeTopic(targetTopic)
	if err != nil {
		return nil, nil, err
	}
	eventCh := make(chan []byte, 256)
	done := make(chan struct{})

	go func() {
		defer close(eventCh)
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-sub.Done():
				return
			case <-sub.Recv():
				batch := sub.Fetch()
				for _, f := range batch {
					data := f.Raw()
					select {
					case eventCh <- data:
					case <-ctx.Done():
						return
					case <-done:
						return
					}
				}
			}
		}
	}()

	var closeOnce sync.Once
	cleanup := func() {
		closeOnce.Do(func() {
			close(done)
			unsub()
		})
	}

	return eventCh, cleanup, nil
}

// SubscribeEvents subscribes to live interaction events for a given group (or all groups if nil)
// and returns a receive-only channel of raw JSON event payloads and a cleanup function.
// It delegates to SubscribeFilteredEvents without a rootID filter.
func (p *Publisher) SubscribeEvents(ctx context.Context, groupID *string) (<-chan []byte, func(), error) {
	return p.SubscribeFilteredEvents(ctx, groupID, nil)
}
