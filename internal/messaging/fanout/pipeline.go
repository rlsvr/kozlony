package fanout

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	natsio "github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

// Snapshot represents an immutable copy-on-write view of active SSE subscriber groupings.
// Broadcast operations load this snapshot with atomic.Pointer without any mutex locking.
type Snapshot struct {
	// groups maps targetKey (either board name like "root" or thread UUID string) to its clients.
	groups       map[string]map[string]*Client
	totalClients int
}

func newEmptySnapshot() *Snapshot {
	return &Snapshot{
		groups:       make(map[string]map[string]*Client),
		totalClients: 0,
	}
}

const (
	opRegister = "register"
	opRemove   = "remove"
	opSwitch   = "switch"
)

type pipelineRequest struct {
	op        string
	client    *Client
	newRootID *uuid.UUID
	done      chan struct{}
}

// GroupSubscriptionListener notifies external components when a group transitions
// between having active subscribers and having none.
type GroupSubscriptionListener interface {
	OnGroupActive(groupID string) error
	OnGroupInactive(groupID string)
}

// Pipeline manages active SSE connection groupings asynchronously using the actor pattern
// and per-topic Core NATS subscriptions with zero application-layer payload filtering.
type Pipeline struct {
	ctx           context.Context
	cancel        context.CancelFunc
	done          chan struct{}
	current       atomic.Pointer[Snapshot]
	requests      chan pipelineRequest
	batchInterval time.Duration
	batchSize     int

	// Core NATS dynamic subscriptions per active topic
	conn        *natsio.Conn
	livePrefix  string
	subMu       sync.Mutex
	topicCounts map[string]int
	topicSubs   map[string]*natsio.Subscription
	listener    GroupSubscriptionListener
}

// Option configures Pipeline behavior.
type Option func(*Pipeline)

// WithGroupListener sets an optional external GroupSubscriptionListener.
func WithGroupListener(l GroupSubscriptionListener) Option {
	return func(p *Pipeline) {
		p.listener = l
	}
}

// WithNATSConn attaches a raw Core NATS connection and live subject prefix.
func WithNATSConn(conn *natsio.Conn, livePrefix string) Option {
	return func(p *Pipeline) {
		p.conn = conn
		if livePrefix != "" {
			p.livePrefix = livePrefix
		}
	}
}

// WithLivePrefix sets the live subject prefix.
func WithLivePrefix(prefix string) Option {
	return func(p *Pipeline) {
		if prefix != "" {
			p.livePrefix = prefix
		}
	}
}

// NewPipeline creates and starts a new fan-out Pipeline with per-topic Core NATS subscriptions.
func NewPipeline(ctx context.Context, batchInterval time.Duration, batchSize int, opts ...Option) *Pipeline {
	if batchInterval <= 0 {
		batchInterval = 15 * time.Millisecond
	}
	if batchSize <= 0 {
		batchSize = 128
	}

	pipelineCtx, cancel := context.WithCancel(ctx)
	p := &Pipeline{
		ctx:           pipelineCtx,
		cancel:        cancel,
		done:          make(chan struct{}),
		requests:      make(chan pipelineRequest, 2048),
		batchInterval: batchInterval,
		batchSize:     batchSize,
		livePrefix:    "LIVE",
		topicCounts:   make(map[string]int),
		topicSubs:     make(map[string]*natsio.Subscription),
	}

	for _, opt := range opts {
		opt(p)
	}

	p.current.Store(newEmptySnapshot())
	go p.runBatchActor()

	return p
}

// Stop shuts down NATS topic subscriptions and the actor loop cleanly.
func (p *Pipeline) Stop() {
	p.cancel()

	p.subMu.Lock()
	for topic, s := range p.topicSubs {
		_ = s.Unsubscribe()
		delete(p.topicSubs, topic)
	}
	p.subMu.Unlock()

	<-p.done
}

// Register adds a client subscriber asynchronously.
func (p *Pipeline) Register(client *Client) {
	p.requests <- pipelineRequest{op: opRegister, client: client}
}

// RegisterAndWait adds a client subscriber and blocks until incorporated into the snapshot.
func (p *Pipeline) RegisterAndWait(client *Client) {
	done := make(chan struct{})
	p.requests <- pipelineRequest{op: opRegister, client: client, done: done}
	<-done
}

// Remove unregisters a client subscriber asynchronously.
func (p *Pipeline) Remove(client *Client) {
	p.requests <- pipelineRequest{op: opRemove, client: client}
}

// RemoveAndWait unregisters a client subscriber and blocks until removed from the snapshot.
func (p *Pipeline) RemoveAndWait(client *Client) {
	done := make(chan struct{})
	p.requests <- pipelineRequest{op: opRemove, client: client, done: done}
	<-done
}

// Switch updates a client's subscription focus (e.g. from feed to thread, or between threads).
// It atomically increments the client's view epoch to drop any in-flight stale messages.
func (p *Pipeline) Switch(client *Client, newRootID *uuid.UUID) {
	client.BumpEpoch()
	p.requests <- pipelineRequest{op: opSwitch, client: client, newRootID: newRootID}
}

// SwitchAndWait updates a client's subscription focus, blocks until applied to the snapshot,
// and drains any residual events from the client's buffer.
func (p *Pipeline) SwitchAndWait(client *Client, newRootID *uuid.UUID) {
	done := make(chan struct{})
	client.BumpEpoch()
	p.requests <- pipelineRequest{op: opSwitch, client: client, newRootID: newRootID, done: done}
	<-done
	client.Drain()
}

// Current returns the current immutable snapshot of subscribers.
func (p *Pipeline) Current() *Snapshot {
	return p.current.Load()
}

// TotalClients returns the current total number of active subscribers.
func (p *Pipeline) TotalClients() int {
	snap := p.current.Load()
	if snap == nil {
		return 0
	}
	return snap.totalClients
}

// Broadcast distributes a payload to subscribers (used in tests and internal calls).
func (p *Pipeline) Broadcast(groupID string, rootID *uuid.UUID, data []byte) int {
	delivered := 0
	if rootID != nil {
		delivered += p.dispatchTopic(rootID.String(), data)
	}
	delivered += p.dispatchTopic(groupID, data)
	return delivered
}

// dispatchTopic distributes raw payload bytes to all clients on a target key.
// Zero JSON decoding: delivers raw bytes directly to client channels.
func (p *Pipeline) dispatchTopic(targetKey string, data []byte) int {
	snap := p.current.Load()
	if snap == nil || snap.totalClients == 0 {
		return 0
	}

	clients := snap.groups[targetKey]
	if len(clients) == 0 {
		return 0
	}

	delivered := 0
	for _, c := range clients {
		if c.Send(data) {
			delivered++
		}
	}
	return delivered
}

func (p *Pipeline) topicForClient(c *Client) (string, string) {
	prefix := p.livePrefix
	if prefix == "" {
		prefix = "LIVE"
	}
	targetKey := c.TargetGroupID()
	if c.RootID != nil {
		return fmt.Sprintf("%s.%s.thread.%s", prefix, c.GroupID, c.RootID.String()), targetKey
	}
	return fmt.Sprintf("%s.%s.feed", prefix, c.GroupID), targetKey
}

func (p *Pipeline) runBatchActor() {
	defer close(p.done)

	batch := make([]pipelineRequest, 0, p.batchSize)
	timer := time.NewTimer(p.batchInterval)
	defer timer.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case req := <-p.requests:
			batch = append(batch, req)
			if len(batch) >= p.batchSize {
				p.applyBatch(batch)
				batch = batch[:0]
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(p.batchInterval)
			}
		case <-timer.C:
			if len(batch) > 0 {
				p.applyBatch(batch)
				batch = batch[:0]
			}
			timer.Reset(p.batchInterval)
		}
	}
}

func (p *Pipeline) applyBatch(batch []pipelineRequest) {
	current := p.current.Load()

	nextGroups := make(map[string]map[string]*Client, len(current.groups)+len(batch))
	for gID, clients := range current.groups {
		nextGroups[gID] = clients
	}

	cloned := make(map[string]bool)

	cloneGroupIfNeeded := func(gID string) {
		if cloned[gID] {
			if nextGroups[gID] == nil {
				nextGroups[gID] = make(map[string]*Client, 4)
			}
			return
		}
		orig := nextGroups[gID]
		if orig == nil {
			nextGroups[gID] = make(map[string]*Client, 4)
			cloned[gID] = true
			return
		}
		groupCopy := make(map[string]*Client, len(orig)+1)
		for id, c := range orig {
			groupCopy[id] = c
		}
		nextGroups[gID] = groupCopy
		cloned[gID] = true
	}

	for _, req := range batch {
		c := req.client
		if c == nil {
			if req.done != nil {
				close(req.done)
			}
			continue
		}

		switch req.op {
		case opRegister:
			topic, targetKey := p.topicForClient(c)
			cloneGroupIfNeeded(targetKey)
			nextGroups[targetKey][c.ID] = c
			p.incrTopicCount(topic, targetKey)

		case opRemove:
			topic, targetKey := p.topicForClient(c)
			cloneGroupIfNeeded(targetKey)
			delete(nextGroups[targetKey], c.ID)
			if len(nextGroups[targetKey]) == 0 {
				delete(nextGroups, targetKey)
			}
			p.decrTopicCount(topic, targetKey)

		case opSwitch:
			oldTopic, oldKey := p.topicForClient(c)
			cloneGroupIfNeeded(oldKey)
			delete(nextGroups[oldKey], c.ID)
			if len(nextGroups[oldKey]) == 0 {
				delete(nextGroups, oldKey)
			}
			p.decrTopicCount(oldTopic, oldKey)

			c.RootID = req.newRootID
			newTopic, newKey := p.topicForClient(c)
			cloneGroupIfNeeded(newKey)
			nextGroups[newKey][c.ID] = c
			p.incrTopicCount(newTopic, newKey)
		}

		if req.done != nil {
			close(req.done)
		}
	}

	total := 0
	for _, grp := range nextGroups {
		total += len(grp)
	}

	p.current.Store(&Snapshot{
		groups:       nextGroups,
		totalClients: total,
	})
}

func (p *Pipeline) incrTopicCount(topic, targetKey string) {
	p.subMu.Lock()
	defer p.subMu.Unlock()

	prev := p.topicCounts[topic]
	p.topicCounts[topic] = prev + 1
	if prev > 0 {
		return
	}

	if p.listener != nil {
		_ = p.listener.OnGroupActive(targetKey)
	}
	p.subscribeTopicLocked(topic, targetKey)
}

func (p *Pipeline) subscribeTopicLocked(topic, targetKey string) {
	if p.conn == nil {
		return
	}
	key := targetKey
	sub, err := p.conn.Subscribe(topic, func(m *natsio.Msg) {
		p.dispatchTopic(key, m.Data)
	})
	if err != nil {
		log.Error().Err(err).Str("topic", topic).Msg("failed to subscribe to live NATS topic")
		return
	}
	p.topicSubs[topic] = sub
	log.Debug().Str("topic", topic).Msg("subscribed to live NATS topic")
}

func (p *Pipeline) decrTopicCount(topic, targetKey string) {
	p.subMu.Lock()
	defer p.subMu.Unlock()

	prev := p.topicCounts[topic]
	if prev <= 1 {
		delete(p.topicCounts, topic)
		if sub, exists := p.topicSubs[topic]; exists {
			_ = sub.Unsubscribe()
			delete(p.topicSubs, topic)
			log.Debug().Str("topic", topic).Msg("unsubscribed from live NATS topic")
		}
		if p.listener != nil {
			p.listener.OnGroupInactive(targetKey)
		}
	} else {
		p.topicCounts[topic] = prev - 1
	}
}
