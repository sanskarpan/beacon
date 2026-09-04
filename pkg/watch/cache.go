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
	buf    []Event
	cap    int
	head   int // index of oldest
	size   int
	oldest uint64
	newest uint64
}

// NewCache creates a ring buffer with the given capacity (default 1000).
func NewCache(capacity int) *Cache {
	if capacity <= 0 {
		capacity = 1000
	}
	return &Cache{
		buf: make([]Event, capacity),
		cap: capacity,
	}
}

// Append adds an event — O(1) ring, no shift.
func (c *Cache) Append(ev Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.size < c.cap {
		pos := (c.head + c.size) % c.cap
		c.buf[pos] = ev
		c.size++
	} else {
		// overwrite oldest
		c.buf[c.head] = ev
		c.head = (c.head + 1) % c.cap
	}
	if c.size == 1 {
		c.oldest = ev.Index
	} else {
		c.oldest = c.buf[c.head].Index
	}
	if ev.Index > c.newest {
		c.newest = ev.Index
	}
}

// Since returns events with index > index.
func (c *Cache) Since(index uint64) ([]Event, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.size == 0 {
		return nil, nil
	}
	if index < c.oldest && index > 0 {
		return nil, ErrIndexCompacted
	}
	out := make([]Event, 0, c.size)
	for i := 0; i < c.size; i++ {
		ev := c.buf[(c.head+i)%c.cap]
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
	return c.size
}

// Newest returns newest index.
func (c *Cache) Newest() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.newest
}

// Events returns a snapshot of events in order.
func (c *Cache) Events() []Event {
	c.mu.RLock()
	defer c.mu.RUnlock()
	cp := make([]Event, c.size)
	for i := 0; i < c.size; i++ {
		cp[i] = c.buf[(c.head+i)%c.cap]
	}
	return cp
}
