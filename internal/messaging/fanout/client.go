package fanout

import (
	"sync/atomic"

	"github.com/google/uuid"
)

// DefaultClientBufferSize is the default event channel capacity per subscriber,
// sized to absorb mobile network jitter, cellular handovers, and latency spikes.
const (
	DefaultClientBufferSize = 512
	DefaultGroupID          = "root"
)

// Client represents an individual active SSE connection subscriber.
type Client struct {
	ID      string
	GroupID string
	RootID  *uuid.UUID // nil for whole group feed, or root UUID for a specific conversation thread
	Events  chan []byte
	epoch   atomic.Uint64
	closed  atomic.Bool
}

// NewClient creates a new Client subscriber with a buffered event channel.
func NewClient(id, groupID string, rootID *uuid.UUID, bufSize int) *Client {
	if bufSize <= 0 {
		bufSize = DefaultClientBufferSize
	}
	if groupID == "" {
		groupID = DefaultGroupID
	}
	c := &Client{
		ID:      id,
		GroupID: groupID,
		RootID:  rootID,
		Events:  make(chan []byte, bufSize),
	}
	c.epoch.Store(1)
	return c
}

// TargetGroupID returns the effective routing group key:
// either rootID string if focused on a thread, or GroupID for the board feed.
func (c *Client) TargetGroupID() string {
	if c.RootID != nil {
		return c.RootID.String()
	}
	return c.GroupID
}

// Epoch returns the current view epoch generation of the client.
func (c *Client) Epoch() uint64 {
	return c.epoch.Load()
}

// BumpEpoch atomically increments the client's view epoch generation.
func (c *Client) BumpEpoch() uint64 {
	return c.epoch.Add(1)
}

// Send attempts to deliver an event to the client's channel without blocking.
// If the client's buffer is full, the message is dropped to prevent slow consumers
// from stalling the fan-out broadcast. Returns true if delivered.
func (c *Client) Send(msg []byte) bool {
	return c.SendWithEpoch(msg, 0)
}

// SendWithEpoch delivers an event only if the client is still at expectedEpoch
// (or if expectedEpoch is 0, ignoring epoch checks). Drops the message if the view epoch
// has changed, preventing stale cross-view event delivery during switches.
func (c *Client) SendWithEpoch(msg []byte, expectedEpoch uint64) bool {
	if c.closed.Load() {
		return false
	}
	if expectedEpoch > 0 && c.epoch.Load() != expectedEpoch {
		return false
	}
	select {
	case c.Events <- msg:
		return true
	default:
		return false
	}
}

// Drain clears any queued messages from the client's event buffer.
func (c *Client) Drain() {
	for {
		select {
		case <-c.Events:
		default:
			return
		}
	}
}

// Close marks the client closed so no further events are sent to it.
// Note: We intentionally do NOT call close(c.Events) here because in lock-free
// copy-on-write architectures, concurrent dispatchDirect workers may hold a reference
// to this client in an in-flight snapshot. The channel and client will be safely
// garbage collected once the HTTP SSE consumer and snapshot references are dropped.
func (c *Client) Close() {
	c.closed.Store(true)
}
