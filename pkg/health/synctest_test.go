package health

import (
	"testing"

	"github.com/sanskar/beacon/pkg/catalog"
)

// TestSynctest_HysteresisTransition verifies the hysteresis state machine
// transitions correctly under various sequences of health observations.
// This is a non-timer test but validates the core state machine that
// underpins the timer-driven health runner.
func TestSynctest_HysteresisTransition(t *testing.T) {
	// 3 fails → critical, 2 passes → passing.
	h := NewHysteresis(3, 2)

	if h.Current() != catalog.HealthPassing {
		t.Fatal("initial state should be passing")
	}

	// 2 failures — should still be passing.
	h.Observe(catalog.HealthCritical)
	h.Observe(catalog.HealthCritical)
	if h.Current() != catalog.HealthPassing {
		t.Fatal("should still be passing after 2 failures")
	}

	// 3rd failure — should transition to critical.
	_, changed := h.Observe(catalog.HealthCritical)
	if !changed {
		t.Fatal("expected state change on 3rd failure")
	}
	if h.Current() != catalog.HealthCritical {
		t.Fatal("should be critical after 3 failures")
	}

	// 1 pass — should still be critical.
	h.Observe(catalog.HealthPassing)
	if h.Current() != catalog.HealthCritical {
		t.Fatal("should still be critical after 1 pass")
	}

	// 2nd pass — should transition to passing.
	_, changed = h.Observe(catalog.HealthPassing)
	if !changed {
		t.Fatal("expected state change on 2nd pass")
	}
	if h.Current() != catalog.HealthPassing {
		t.Fatal("should be passing after 2 passes")
	}
}

// TestSynctest_HysteresisWarningSurfaces verifies that warning is surfaced
// immediately when currently passing (without requiring consecutive counts).
func TestSynctest_HysteresisWarningSurfaces(t *testing.T) {
	h := NewHysteresis(3, 2)

	_, changed := h.Observe(catalog.HealthWarning)
	if !changed {
		t.Fatal("warning should surface immediately when passing")
	}
	if h.Current() != catalog.HealthWarning {
		t.Fatal("should be warning")
	}

	// Transition to critical.
	h.Observe(catalog.HealthCritical)
	h.Observe(catalog.HealthCritical)
	h.Observe(catalog.HealthCritical)
	if h.Current() != catalog.HealthCritical {
		t.Fatal("should be critical")
	}

	// Warning while critical should not change state.
	_, changed = h.Observe(catalog.HealthWarning)
	if changed {
		t.Fatal("warning should not change state when critical")
	}
}
