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

// Hub bridges NATS events into the fan-out Pipeline using dynamic, on-demand subscriptions per active group.
type Hub struct {
	pipeline *Pipeline
	conn     *natsio.Conn
	stream   string

	mu        sync.Mutex
	groupSubs map[string][]*natsio.Subscription
}

// NewHub initializes an event fan-out Hub.
func NewHub(pipeline *Pipeline, conn *natsio.Conn, stream string) *Hub {
	if stream == "" {
		stream = defaultStreamName
	}
	return &Hub{
		pipeline:  pipeline,
		conn:      conn,
		stream:    stream,
		groupSubs: make(map[string][]*natsio.Subscription),
	}
}

// SetPipeline updates the fan-out pipeline associated with this hub.
func (h *Hub) SetPipeline(pipeline *Pipeline) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.pipeline = pipeline
}

// Start initializes the fan-out Hub. NATS subscriptions are dynamically managed per group.
func (*Hub) Start(_ context.Context) error {
	return nil
}

// Stop unsubscribes from all active NATS group subjects.
func (h *Hub) Stop() {
	h.mu.Lock()
	defer h.mu.Unlock()

	for groupID, subs := range h.groupSubs {
		for _, sub := range subs {
			_ = sub.Unsubscribe()
		}
		delete(h.groupSubs, groupID)
	}
}

// OnGroupActive dynamically subscribes to Core NATS subjects for the active group.
func (h *Hub) OnGroupActive(groupID string) error {
	if h.conn == nil {
		return nil
	}
	if groupID == "" {
		groupID = defaultGroupID
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if _, exists := h.groupSubs[groupID]; exists {
		return nil
	}

	handler := func(m *natsio.Msg) {
		h.routeMessage(m)
	}

	subjects := []string{
		fmt.Sprintf("%s.%s.evt.>", h.stream, groupID),
		fmt.Sprintf("%s.%s.audit.edited", h.stream, groupID),
		fmt.Sprintf("%s.%s.counter.replied", h.stream, groupID),
	}

	subs := make([]*natsio.Subscription, 0, len(subjects))
	for _, subj := range subjects {
		sub, err := h.conn.Subscribe(subj, handler)
		if err != nil {
			for _, done := range subs {
				_ = done.Unsubscribe()
			}
			return fmt.Errorf("subscribe to group %s on %s: %w", groupID, subj, err)
		}
		subs = append(subs, sub)
	}

	h.groupSubs[groupID] = subs
	log.Debug().Str("group_id", groupID).Int("subscriptions", len(subs)).Msg("subscribed to group subjects on NATS")
	return nil
}

// OnGroupInactive unsubscribes from Core NATS subjects for the inactive group.
func (h *Hub) OnGroupInactive(groupID string) {
	if groupID == "" {
		groupID = defaultGroupID
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	subs, exists := h.groupSubs[groupID]
	if !exists {
		return
	}

	for _, sub := range subs {
		_ = sub.Unsubscribe()
	}
	delete(h.groupSubs, groupID)
	log.Debug().Str("group_id", groupID).Msg("unsubscribed from group subjects on NATS")
}

// ActiveGroups returns a list of currently subscribed group IDs.
func (h *Hub) ActiveGroups() []string {
	h.mu.Lock()
	defer h.mu.Unlock()

	groups := make([]string, 0, len(h.groupSubs))
	for g := range h.groupSubs {
		groups = append(groups, g)
	}
	return groups
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
