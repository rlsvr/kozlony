package cache

import (
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/rlsvr/kozlony/internal/database"
)

// LRUShard manages a subset of conversation threads with an internal doubly-linked list
// for O(1) LRU promotion and eviction, protected by its own RWMutex to avoid lock contention.
type LRUShard struct {
	mu          sync.RWMutex
	threads     map[uuid.UUID]*Thread
	head        *Thread // Most recently used
	tail        *Thread // Least recently used
	totalWeight int     // Sum of all thread weights in this shard
}

// NewLRUShard creates an empty LRU shard.
func NewLRUShard() *LRUShard {
	return &LRUShard{
		threads: make(map[uuid.UUID]*Thread),
	}
}

// GetThread retrieves a thread and promotes it to the head of the LRU list.
func (s *LRUShard) GetThread(rootID uuid.UUID) (*Thread, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	thread, ok := s.threads[rootID]
	if !ok {
		return nil, false
	}
	s.promote(thread)
	return thread, true
}

// GetThreadInteractions retrieves all interactions in a thread sorted chronologically
// while holding the shard lock to prevent concurrent map modification during iteration.
func (s *LRUShard) GetThreadInteractions(rootID uuid.UUID) ([]*database.Interaction, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	thread, ok := s.threads[rootID]
	if !ok {
		return nil, false
	}
	s.promote(thread)
	return thread.AllInteractionsChronological(), true
}

// GetThreadReadOnly retrieves a thread without promoting it (for read-heavy paths when promotion isn't required).
func (s *LRUShard) GetThreadReadOnly(rootID uuid.UUID) (*Thread, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	thread, ok := s.threads[rootID]
	return thread, ok
}

// PutRoot inserts or updates a root post as a Thread.
// If the thread already exists, updates its root and promotes it.
// Returns the thread and change in shard weight.
func (s *LRUShard) PutRoot(root *database.Interaction) (*Thread, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	thread, exists := s.threads[root.ID]
	if exists {
		thread.Root = root
		thread.LastAccess = time.Now()
		s.promote(thread)
		return thread, 0
	}

	thread = NewThread(root)
	s.threads[root.ID] = thread
	s.addToHead(thread)
	s.totalWeight += thread.Weight
	return thread, thread.Weight
}

// PutReply inserts a reply into an existing thread if present, or creates a stub thread if root is not yet cached.
// Returns the thread, whether it was added, and change in shard weight.
func (s *LRUShard) PutReply(rootID uuid.UUID, reply *database.Interaction) (*Thread, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	thread, exists := s.threads[rootID]
	if !exists {
		// Parent thread not in this shard; cannot cache isolated reply without root
		return nil, 0
	}

	oldWeight := thread.Weight
	thread.AddReply(reply)
	weightDelta := thread.Weight - oldWeight
	s.totalWeight += weightDelta
	s.promote(thread)
	return thread, weightDelta
}

// PutFullThread stores or replaces an entire thread (root + all replies).
func (s *LRUShard) PutFullThread(rootID uuid.UUID, items []*database.Interaction) (*Thread, int) {
	if len(items) == 0 {
		return nil, 0
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var root *database.Interaction
	for _, it := range items {
		if it.ID == rootID {
			root = it
			break
		}
	}
	if root == nil {
		root = items[0]
	}

	thread, exists := s.threads[rootID]
	oldWeight := 0
	if exists {
		oldWeight = thread.Weight
		s.remove(thread)
	}

	thread = NewThread(root)
	for _, it := range items {
		if it.ID != root.ID {
			thread.AddReply(it)
		}
	}

	s.threads[rootID] = thread
	s.addToHead(thread)
	weightDelta := thread.Weight - oldWeight
	s.totalWeight += weightDelta
	return thread, weightDelta
}

// UpdateNode updates a node in the thread (e.g. on OCC edit).
func (s *LRUShard) UpdateNode(rootID uuid.UUID, item *database.Interaction) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	thread, exists := s.threads[rootID]
	if !exists {
		return false
	}
	updated := thread.UpdateNode(item)
	if updated {
		s.promote(thread)
	}
	return updated
}

// UpdateReplyCount updates the reply count on a node in this shard's thread.
func (s *LRUShard) UpdateReplyCount(rootID uuid.UUID, nodeID uuid.UUID, count int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	thread, exists := s.threads[rootID]
	if !exists {
		return false
	}
	node, ok := thread.GetNode(nodeID)
	if !ok {
		return false
	}
	node.ReplyCount = count
	return true
}

// Delete removes a thread entirely from the shard.
func (s *LRUShard) Delete(rootID uuid.UUID) (*Thread, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	thread, exists := s.threads[rootID]
	if !exists {
		return nil, 0
	}
	s.remove(thread)
	delete(s.threads, rootID)
	weight := thread.Weight
	s.totalWeight -= weight
	return thread, weight
}

// PopOldest removes and returns the least recently used thread from the tail.
func (s *LRUShard) PopOldest() *Thread {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.tail == nil {
		return nil
	}
	oldest := s.tail
	s.remove(oldest)
	delete(s.threads, oldest.RootID)
	s.totalWeight -= oldest.Weight
	return oldest
}

// TotalWeight returns the total weight of all threads currently in this shard.
func (s *LRUShard) TotalWeight() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.totalWeight
}

// Count returns the number of threads currently in this shard.
func (s *LRUShard) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.threads)
}

// Internal linked list helpers (must be called with s.mu held)

func (s *LRUShard) promote(t *Thread) {
	if s.head == t {
		return
	}
	s.remove(t)
	s.addToHead(t)
}

func (s *LRUShard) addToHead(t *Thread) {
	t.prev = nil
	t.next = s.head
	if s.head != nil {
		s.head.prev = t
	}
	s.head = t
	if s.tail == nil {
		s.tail = t
	}
}

func (s *LRUShard) remove(t *Thread) {
	if t.prev != nil {
		t.prev.next = t.next
	} else {
		s.head = t.next
	}
	if t.next != nil {
		t.next.prev = t.prev
	} else {
		s.tail = t.prev
	}
	t.prev = nil
	t.next = nil
}
