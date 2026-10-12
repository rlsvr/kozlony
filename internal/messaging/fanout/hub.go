package fanout

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/google/uuid"
	natsio "github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

const (
	defaultStreamName = "BOARD"
	defaultGroupID    = "root"
)

// EventRouter abstracts event parsing to extract groupID and optional rootID.
type eventEnvelope struct {
	GroupID string  `json:"group_id"`
	ID      string  `json:"id"`
	RootID  *string `json:"root_id,omitempty"`
}

// Hub bridges NATS JetStream events into the fan-out Pipeline with a single broker subscription.
type Hub struct {
	pipeline *Pipeline
	conn     *natsio.Conn
	stream   string

	mu   sync.Mutex
	subs []*natsio.Subscription
}

// NewHub initializes an event fan-out Hub.
func NewHub(pipeline *Pipeline, conn *natsio.Conn, stream string) *Hub {
	if stream == "" {
		stream = defaultStreamName
	}
	return &Hub{
		pipeline: pipeline,
		conn:     conn,
		stream:   stream,
	}
}

// Start begins consuming events from NATS and routing them to the fanout pipeline.
func (h *Hub) Start(_ context.Context) error {
	if h.conn == nil {
		return nil
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	handler := func(m *natsio.Msg) {
		h.routeMessage(m)
	}

	// 1 shared subscription per subject namespace for the entire replica
	subjects := []string{
		fmt.Sprintf("%s.*.evt.>", h.stream),           // BOARD.*.evt.>
		fmt.Sprintf("%s.*.audit.edited", h.stream),    // BOARD.*.audit.edited
		fmt.Sprintf("%s.*.counter.replied", h.stream), // BOARD.*.counter.replied
	}

	for _, subj := range subjects {
		sub, err := h.conn.Subscribe(subj, handler)
		if err != nil {
			for _, done := range h.subs {
				_ = done.Unsubscribe()
			}
			h.subs = nil
			return fmt.Errorf("subscribe to %s: %w", subj, err)
		}
		h.subs = append(h.subs, sub)
	}

	log.Info().Int("subscriptions", len(h.subs)).Msg("fan-out hub started with shared NATS consumers")
	return nil
}

// Stop unsubscribes from NATS subjects.
func (h *Hub) Stop() {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, sub := range h.subs {
		_ = sub.Unsubscribe()
	}
	h.subs = nil
}

// routeMessage extracts group_id and root_id from incoming event payload and broadcasts to the pipeline.
func (h *Hub) routeMessage(m *natsio.Msg) {
	if h.pipeline == nil || len(m.Data) == 0 {
		return
	}

	var env eventEnvelope
	if err := json.Unmarshal(m.Data, &env); err != nil {
		// If unmarshal fails, parse group from subject: <stream>.<group>.<suffix>
		parts := strings.Split(m.Subject, ".")
		if len(parts) >= 2 {
			env.GroupID = parts[1]
		}
	}

	if env.GroupID == "" {
		env.GroupID = defaultGroupID
	}

	var rootUUID *uuid.UUID
	if env.RootID != nil && *env.RootID != "" {
		if parsed, err := uuid.Parse(*env.RootID); err == nil {
			rootUUID = &parsed
		}
	} else if env.ID != "" {
		// Root post itself is its own thread root
		if parsed, err := uuid.Parse(env.ID); err == nil {
			rootUUID = &parsed
		}
	}

	h.pipeline.Broadcast(env.GroupID, rootUUID, m.Data)
}
