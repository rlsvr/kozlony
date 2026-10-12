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

	// threadClients: "groupID:rootID" -> clientID -> *Client (clients subscribed to a specific conversation)
	threadClients map[string]map[string]*Client

	// totalClients count
	totalClients int
}

func newEmptySnapshot() *Snapshot {
	return &Snapshot{
		feedClients:   make(map[string]map[string]*Client),
		threadClients: make(map[string]map[string]*Client),
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

// Pipeline manages active SSE connection groupings asynchronously using the actor pattern
// and atomic copy-on-write snapshots, eliminating lock contention under high concurrency.
type Pipeline struct {
	ctx           context.Context
	cancel        context.CancelFunc
	requests      chan pipelineRequest
	current       atomic.Pointer[Snapshot]
	batchInterval time.Duration
	batchSize     int
}

// NewPipeline creates and starts a new fan-out Pipeline.
func NewPipeline(ctx context.Context, batchInterval time.Duration, batchSize int) *Pipeline {
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
func (p *Pipeline) Switch(client *Client, newRootID *uuid.UUID) {
	p.requests <- pipelineRequest{op: opSwitch, client: client, newRootID: newRootID}
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
func (p *Pipeline) Broadcast(groupID string, rootID *uuid.UUID, data []byte) int {
	snap := p.current.Load()
	if snap.totalClients == 0 {
		return 0
	}

	delivered := 0

	// 1. Deliver to feed subscribers of this group
	if feeds, ok := snap.feedClients[groupID]; ok {
		for _, client := range feeds {
			if client.Send(data) {
				delivered++
			}
		}
	}

	// 2. Deliver to specific thread subscribers if rootID is present
	if rootID != nil {
		key := threadKey(groupID, *rootID)
		if threads, ok := snap.threadClients[key]; ok {
			for _, client := range threads {
				if client.Send(data) {
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
			addClientToSnapshot(next, req.client)
		case opRemove:
			removeClientFromSnapshot(next, req.client)
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
		threadClients: make(map[string]map[string]*Client, len(src.threadClients)),
		totalClients:  src.totalClients,
	}

	for groupID, clients := range src.feedClients {
		groupCopy := make(map[string]*Client, len(clients))
		for id, c := range clients {
			groupCopy[id] = c
		}
		dst.feedClients[groupID] = groupCopy
	}

	for key, clients := range src.threadClients {
		threadCopy := make(map[string]*Client, len(clients))
		for id, c := range clients {
			threadCopy[id] = c
		}
		dst.threadClients[key] = threadCopy
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
	if _, exists := group[c.ID]; !exists {
		snap.totalClients++
	}
	group[c.ID] = c
}

func addThreadClient(snap *Snapshot, c *Client) {
	key := threadKey(c.GroupID, *c.RootID)
	group, ok := snap.threadClients[key]
	if !ok {
		group = make(map[string]*Client)
		snap.threadClients[key] = group
	}
	if _, exists := group[c.ID]; !exists {
		snap.totalClients++
	}
	group[c.ID] = c
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
		snap.totalClients--
		if len(group) == 0 {
			delete(snap.threadClients, key)
		}
	}
}
