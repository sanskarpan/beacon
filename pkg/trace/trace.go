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
// Format: <unix_nano_hex>-<counter>-<rand4>
func NewID() string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	n := counter.Add(1)
	return fmt.Sprintf("%x-%x-%s", time.Now().UnixNano(), n, hex.EncodeToString(b[:]))
}

// NewIDAt uses a provided timestamp (for virtual-clock tests).
func NewIDAt(t time.Time) string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	n := counter.Add(1)
	return fmt.Sprintf("%x-%x-%s", t.UnixNano(), n, hex.EncodeToString(b[:]))
}
