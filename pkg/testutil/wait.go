// Package testutil provides shared helpers for test files across the codebase.
package testutil

import (
	"testing"
	"time"
)

// WaitUntil polls cond every pollInterval until it returns true or timeout
// elapses. It fails the test with t.Fatal if the condition is not met.
func WaitUntil(t *testing.T, timeout time.Duration, pollInterval time.Duration, cond func() bool, msg string) {
	t.Helper()
	if pollInterval <= 0 {
		pollInterval = time.Millisecond
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(pollInterval)
	}
	t.Fatalf("condition not met within %s: %s", timeout, msg)
}

// Eventually is like WaitUntil with a default 5s timeout and 1ms poll.
func Eventually(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	WaitUntil(t, 5*time.Second, time.Millisecond, cond, msg)
}
