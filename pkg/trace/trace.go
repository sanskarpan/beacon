// Package trace generates and propagates TraceIDs for end-to-end propagation measurement.
package trace

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"time"
)

var counter atomic.Uint64

// NewID returns a unique trace identifier.
// Format: <unix_nano_hex>-<counter>-<rand4> (hex zero-padded for lexicographic sort)
func NewID() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		// fallback to time-based entropy on crypto failure
		now := time.Now().UnixNano()
		for i := range b {
			b[i] = byte(now >> (i * 8))
		}
	}
	n := counter.Add(1)
	return fmt.Sprintf("%016x-%016x-%s", uint64(time.Now().UnixNano()), n, hex.EncodeToString(b[:]))
}

// NewIDAt uses a provided timestamp (for virtual-clock tests).
func NewIDAt(t time.Time) string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		v := t.UnixNano()
		for i := range b {
			b[i] = byte(v >> (i * 8))
		}
	}
	n := counter.Add(1)
	return fmt.Sprintf("%016x-%016x-%s", uint64(t.UnixNano()), n, hex.EncodeToString(b[:]))
}
