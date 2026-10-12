package fanout

import (
	"sync/atomic"

	"github.com/google/uuid"
)

// Client represents an individual active SSE connection subscriber.
type Client struct {
	ID      string
	GroupID string
	RootID  *uuid.UUID // nil for whole group feed, or root UUID for a specific conversation thread
	Events  chan []byte
	closed  atomic.Bool
}

// NewClient creates a new Client subscriber with a buffered event channel.
func NewClient(id, groupID string, rootID *uuid.UUID, bufSize int) *Client {
	if bufSize <= 0 {
		bufSize = 256
	}
	if groupID == "" {
		groupID = "root"
	}
	return &Client{
		ID:      id,
		GroupID: groupID,
		RootID:  rootID,
		Events:  make(chan []byte, bufSize),
	}
}

// Send attempts to deliver an event to the client's channel without blocking.
// If the client's buffer is full, the message is dropped to prevent slow consumers
// from stalling the fan-out broadcast. Returns true if delivered.
func (c *Client) Send(msg []byte) bool {
	if c.closed.Load() {
		return false
	}
	select {
	case c.Events <- msg:
		return true
	default:
		return false
	}
}

// Close marks the client closed and drains/closes its channel safely.
func (c *Client) Close() {
	if c.closed.CompareAndSwap(false, true) {
		close(c.Events)
	}
}
