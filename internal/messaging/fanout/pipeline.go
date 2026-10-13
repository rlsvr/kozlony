package fanout

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// Snapshot represents an immutable copy-on-write view of active SSE subscriber groupings.
// Broadcast operations load this snapshot with atomic.Pointer without any mutex locking.
type Snapshot struct {
	// feedClients: groupID -> clientID -> *Client (clients subscribed to the entire board feed)
	feedClients map[string]map[string]*Client

	// feedEpochs: groupID -> clientID -> epoch at registration/switch
	feedEpochs map[string]map[string]uint64

	// threadClients: "groupID:rootID" -> clientID -> *Client (clients subscribed to a specific conversation)
	threadClients map[string]map[string]*Client

	// threadEpochs: "groupID:rootID" -> clientID -> epoch at registration/switch
	threadEpochs map[string]map[string]uint64

	// totalClients count
	totalClients int
}

func newEmptySnapshot() *Snapshot {
	return &Snapshot{
		feedClients:   make(map[string]map[string]*Client),
		feedEpochs:    make(map[string]map[string]uint64),
		threadClients: make(map[string]map[string]*Client),
		threadEpochs:  make(map[string]map[string]uint64),
		totalClients:  0,
	}
}

func threadKey(groupID string, rootID uuid.UUID) string {
	return fmt.Sprintf("%s:%s", groupID, rootID.String())
}

const (
	opRegister = "register"
	opRemove   = "remove"
	opSwitch   = "switch"
)

type pipelineRequest struct {
	op        string
	client    *Client
	newRootID *uuid.UUID // for "switch"
	done      chan struct{}
}

// GroupSubscriptionListener notifies external brokers (such as Core NATS)
// when a group transitions between having active subscribers and having none.
type GroupSubscriptionListener interface {
	OnGroupActive(groupID string) error
	OnGroupInactive(groupID string)
}

// Pipeline manages active SSE connection groupings asynchronously using the actor pattern
// and atomic copy-on-write snapshots, eliminating lock contention under high concurrency.
type Pipeline struct {
	ctx           context.Context
	cancel        context.CancelFunc
	requests      chan pipelineRequest
	current       atomic.Pointer[Snapshot]
	batchInterval time.Duration
	batchSize     int
	groupCounts   map[string]int
	listener      GroupSubscriptionListener
}

// Option configures Pipeline behavior.
type Option func(*Pipeline)

// WithGroupListener sets the GroupSubscriptionListener for dynamic group lifecycle callbacks.
func WithGroupListener(l GroupSubscriptionListener) Option {
	return func(p *Pipeline) {
		p.listener = l
	}
}

// NewPipeline creates and starts a new fan-out Pipeline.
func NewPipeline(ctx context.Context, batchInterval time.Duration, batchSize int, opts ...Option) *Pipeline {
	if batchInterval <= 0 {
		batchInterval = 5 * time.Millisecond
	}
	if batchSize <= 0 {
		batchSize = 128
	}

	pipelineCtx, cancel := context.WithCancel(ctx)
	p := &Pipeline{
		ctx:           pipelineCtx,
		cancel:        cancel,
		requests:      make(chan pipelineRequest, 2048),
		batchInterval: batchInterval,
		batchSize:     batchSize,
		groupCounts:   make(map[string]int),
	}
	for _, opt := range opts {
		opt(p)
	}
	p.current.Store(newEmptySnapshot())
	go p.run()
	return p
}

// Stop shuts down the pipeline actor loop.
func (p *Pipeline) Stop() {
	p.cancel()
}

// Register adds a client subscriber.
func (p *Pipeline) Register(client *Client) {
	p.requests <- pipelineRequest{op: opRegister, client: client}
}

// RegisterAndWait adds a client subscriber and blocks until it is incorporated into the snapshot.
func (p *Pipeline) RegisterAndWait(client *Client) {
	done := make(chan struct{})
	p.requests <- pipelineRequest{op: opRegister, client: client, done: done}
	<-done
}

// Remove unregisters a client subscriber.
func (p *Pipeline) Remove(client *Client) {
	p.requests <- pipelineRequest{op: opRemove, client: client}
}

// RemoveAndWait unregisters a client subscriber and blocks until it is removed from the snapshot.
func (p *Pipeline) RemoveAndWait(client *Client) {
	done := make(chan struct{})
	p.requests <- pipelineRequest{op: opRemove, client: client, done: done}
	<-done
}

// Switch updates a client's subscription focus (e.g. from feed to thread, or between threads).
// It increments the client's view epoch to immediately reject stale messages.
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
	return p.current.Load().totalClients
}

// Broadcast distributes an event payload to all matching clients lock-free.
// If rootID is nil, message is delivered to all feed subscribers of groupID.
// If rootID is specified, message is delivered to both feed subscribers of groupID
// and specific thread subscribers for that rootID.
// Stale messages targeted at a client that switched views are discarded via epoch check.
func (p *Pipeline) Broadcast(groupID string, rootID *uuid.UUID, data []byte) int {
	snap := p.current.Load()
	if snap.totalClients == 0 {
		return 0
	}

	delivered := 0

	// 1. Deliver to feed subscribers of this group
	if feeds, ok := snap.feedClients[groupID]; ok {
		epochs := snap.feedEpochs[groupID]
		for id, client := range feeds {
			expEpoch := epochs[id]
			if client.SendWithEpoch(data, expEpoch) {
				delivered++
			}
		}
	}

	// 2. Deliver to specific thread subscribers if rootID is present
	if rootID != nil {
		key := threadKey(groupID, *rootID)
		if threads, ok := snap.threadClients[key]; ok {
			epochs := snap.threadEpochs[key]
			for id, client := range threads {
				expEpoch := epochs[id]
				if client.SendWithEpoch(data, expEpoch) {
					delivered++
				}
			}
		}
	}

	return delivered
}

func (p *Pipeline) run() {
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
	next := copySnapshot(current)

	for _, req := range batch {
		switch req.op {
		case opRegister:
			if c := req.client; c != nil {
				prevCount := p.groupCounts[c.GroupID]
				p.groupCounts[c.GroupID] = prevCount + 1
				if prevCount == 0 && p.listener != nil {
					_ = p.listener.OnGroupActive(c.GroupID)
				}
			}
			addClientToSnapshot(next, req.client)
		case opRemove:
			removeClientFromSnapshot(next, req.client)
			if c := req.client; c != nil {
				prevCount := p.groupCounts[c.GroupID]
				if prevCount <= 1 {
					delete(p.groupCounts, c.GroupID)
					if prevCount == 1 && p.listener != nil {
						p.listener.OnGroupInactive(c.GroupID)
					}
				} else {
					p.groupCounts[c.GroupID] = prevCount - 1
				}
			}
		case opSwitch:
			removeClientFromSnapshot(next, req.client)
			req.client.RootID = req.newRootID
			addClientToSnapshot(next, req.client)
		}
		if req.done != nil {
			close(req.done)
		}
	}

	p.current.Store(next)
}

func copySnapshot(src *Snapshot) *Snapshot {
	dst := &Snapshot{
		feedClients:   make(map[string]map[string]*Client, len(src.feedClients)),
		feedEpochs:    make(map[string]map[string]uint64, len(src.feedEpochs)),
		threadClients: make(map[string]map[string]*Client, len(src.threadClients)),
		threadEpochs:  make(map[string]map[string]uint64, len(src.threadEpochs)),
		totalClients:  src.totalClients,
	}

	for groupID, clients := range src.feedClients {
		groupCopy := make(map[string]*Client, len(clients))
		for id, c := range clients {
			groupCopy[id] = c
		}
		dst.feedClients[groupID] = groupCopy
	}

	for groupID, epochs := range src.feedEpochs {
		epochCopy := make(map[string]uint64, len(epochs))
		for id, ep := range epochs {
			epochCopy[id] = ep
		}
		dst.feedEpochs[groupID] = epochCopy
	}

	for key, clients := range src.threadClients {
		threadCopy := make(map[string]*Client, len(clients))
		for id, c := range clients {
			threadCopy[id] = c
		}
		dst.threadClients[key] = threadCopy
	}

	for key, epochs := range src.threadEpochs {
		epochCopy := make(map[string]uint64, len(epochs))
		for id, ep := range epochs {
			epochCopy[id] = ep
		}
		dst.threadEpochs[key] = epochCopy
	}

	return dst
}

func addClientToSnapshot(snap *Snapshot, c *Client) {
	if c == nil {
		return
	}
	if c.RootID == nil {
		addFeedClient(snap, c)
		return
	}
	addThreadClient(snap, c)
}

func addFeedClient(snap *Snapshot, c *Client) {
	group, ok := snap.feedClients[c.GroupID]
	if !ok {
		group = make(map[string]*Client)
		snap.feedClients[c.GroupID] = group
	}
	epochs, ok := snap.feedEpochs[c.GroupID]
	if !ok {
		epochs = make(map[string]uint64)
		snap.feedEpochs[c.GroupID] = epochs
	}
	if _, exists := group[c.ID]; !exists {
		snap.totalClients++
	}
	group[c.ID] = c
	epochs[c.ID] = c.Epoch()
}

func addThreadClient(snap *Snapshot, c *Client) {
	key := threadKey(c.GroupID, *c.RootID)
	group, ok := snap.threadClients[key]
	if !ok {
		group = make(map[string]*Client)
		snap.threadClients[key] = group
	}
	epochs, ok := snap.threadEpochs[key]
	if !ok {
		epochs = make(map[string]uint64)
		snap.threadEpochs[key] = epochs
	}
	if _, exists := group[c.ID]; !exists {
		snap.totalClients++
	}
	group[c.ID] = c
	epochs[c.ID] = c.Epoch()
}

func removeClientFromSnapshot(snap *Snapshot, c *Client) {
	if c == nil {
		return
	}
	removeFeedClient(snap, c)
	if c.RootID != nil {
		removeThreadClient(snap, c)
	}
}

func removeFeedClient(snap *Snapshot, c *Client) {
	group, ok := snap.feedClients[c.GroupID]
	if !ok {
		return
	}
	if _, exists := group[c.ID]; exists {
		delete(group, c.ID)
		if epochs, ok := snap.feedEpochs[c.GroupID]; ok {
			delete(epochs, c.ID)
			if len(epochs) == 0 {
				delete(snap.feedEpochs, c.GroupID)
			}
		}
		snap.totalClients--
		if len(group) == 0 {
			delete(snap.feedClients, c.GroupID)
		}
	}
}

func removeThreadClient(snap *Snapshot, c *Client) {
	key := threadKey(c.GroupID, *c.RootID)
	group, ok := snap.threadClients[key]
	if !ok {
		return
	}
	if _, exists := group[c.ID]; exists {
		delete(group, c.ID)
		if epochs, ok := snap.threadEpochs[key]; ok {
			delete(epochs, c.ID)
			if len(epochs) == 0 {
				delete(snap.threadEpochs, key)
			}
		}
		snap.totalClients--
		if len(group) == 0 {
			delete(snap.threadClients, key)
		}
	}
}
