package watch

import (
	"sync"
)

// Cache is a ring buffer of recent watch events for resumption.
//
// If a client's index ages out, we return ErrIndexCompacted and force a re-list.
// Silently skipping events would leave the client permanently wrong
// (Kubernetes returns HTTP 410 Gone for the same reason).
type Cache struct {
	mu     sync.RWMutex
	events []Event
	cap    int
	oldest uint64
	newest uint64
}

// NewCache creates a ring buffer with the given capacity (default 1000).
func NewCache(capacity int) *Cache {
	if capacity <= 0 {
		capacity = 1000
	}
	return &Cache{
		events: make([]Event, 0, capacity),
		cap:    capacity,
	}
}

// Append adds an event.
func (c *Cache) Append(ev Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.events) >= c.cap {
		// drop oldest
		c.events = c.events[1:]
		if len(c.events) > 0 {
			c.oldest = c.events[0].Index
		}
	}
	c.events = append(c.events, ev)
	if c.oldest == 0 || ev.Index < c.oldest {
		if len(c.events) == 1 {
			c.oldest = ev.Index
		}
	}
	if ev.Index > c.newest {
		c.newest = ev.Index
	}
	// recompute oldest
	if len(c.events) > 0 {
		c.oldest = c.events[0].Index
	}
}

// Since returns events with index > index.
func (c *Cache) Since(index uint64) ([]Event, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.events) == 0 {
		return nil, nil
	}
	if index < c.oldest && index > 0 {
		return nil, ErrIndexCompacted
	}
	out := make([]Event, 0)
	for _, ev := range c.events {
		if ev.Index > index {
			out = append(out, ev)
		}
	}
	return out, nil
}

// Oldest returns the oldest retained index.
func (c *Cache) Oldest() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.oldest
}

// Len returns buffered event count.
func (c *Cache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.events)
}

// Newest returns newest index.
func (c *Cache) Newest() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.newest
}

// Events returns a snapshot of events.
func (c *Cache) Events() []Event {
	c.mu.RLock()
	defer c.mu.RUnlock()
	cp := make([]Event, len(c.events))
	copy(cp, c.events)
	return cp
}
