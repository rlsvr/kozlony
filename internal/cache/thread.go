package cache

import (
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/rlsvr/kozlony/internal/database"
)

// Thread represents a complete conversation tree rooted at a single root post.
// It tracks its root interaction, all replies, weight (for LRU eviction),
// and doubly-linked list pointers for O(1) LRU promotion/eviction.
type Thread struct {
	RootID     uuid.UUID
	GroupID    string
	Root       *database.Interaction
	Replies    map[uuid.UUID]*database.Interaction // nodeID -> reply
	Weight     int                                 // 1 (root) + len(Replies)
	LastAccess time.Time

	// Pointers for doubly-linked list within an LRU shard
	prev *Thread
	next *Thread
}

// NewThread creates a new Thread with the given root interaction.
func NewThread(root *database.Interaction) *Thread {
	return &Thread{
		RootID:     root.ID,
		GroupID:    root.GroupID,
		Root:       root,
		Replies:    make(map[uuid.UUID]*database.Interaction),
		Weight:     1,
		LastAccess: time.Now(),
	}
}

// AddReply attaches or updates a reply in this thread and updates weight.
func (t *Thread) AddReply(reply *database.Interaction) {
	if _, exists := t.Replies[reply.ID]; !exists {
		t.Weight++
	}
	t.Replies[reply.ID] = reply
	t.LastAccess = time.Now()
}

// UpdateNode updates either the root interaction or one of its replies.
func (t *Thread) UpdateNode(item *database.Interaction) bool {
	if item.ID == t.RootID {
		t.Root = item
		t.LastAccess = time.Now()
		return true
	}
	if _, exists := t.Replies[item.ID]; exists {
		t.Replies[item.ID] = item
		t.LastAccess = time.Now()
		return true
	}
	return false
}

// GetNode retrieves either the root interaction or a reply by ID.
func (t *Thread) GetNode(id uuid.UUID) (*database.Interaction, bool) {
	if id == t.RootID {
		return t.Root, true
	}
	item, ok := t.Replies[id]
	return item, ok
}

// AllInteractionsChronological returns the root and all replies sorted by created_at ASC.
func (t *Thread) AllInteractionsChronological() []*database.Interaction {
	result := make([]*database.Interaction, 0, 1+len(t.Replies))
	if t.Root != nil {
		result = append(result, t.Root)
	}
	for _, reply := range t.Replies {
		result = append(result, reply)
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].CreatedAt.Equal(result[j].CreatedAt) {
			return result[i].ID.String() < result[j].ID.String()
		}
		return result[i].CreatedAt.Before(result[j].CreatedAt)
	})

	return result
}
