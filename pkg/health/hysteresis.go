// Package health implements agent-local health checking with hysteresis and outlier detection.
package health

import (
	"github.com/sanskar/beacon/pkg/catalog"
)

// Hysteresis suppresses flapping.
//
// An instance must fail N times consecutively before critical, and pass M
// times before returning. Without this, a flapping instance generates on EVERY
// interval: a catalog write + index bump + notification to every watcher +
// connection-pool rebuild. One sick instance can produce more control-plane
// load than the entire healthy fleet.
type Hysteresis struct {
	failuresBeforeCritical int
	successesBeforePassing int

	consecutiveFailures  int
	consecutiveSuccesses int
	current              catalog.HealthStatus
	// transition count in a window for flapping detection
	transitions int
}

// NewHysteresis creates a state machine. Defaults: 3 fails to eject, 2 passes to return.
func NewHysteresis(failuresBeforeCritical, successesBeforePassing int) *Hysteresis {
	if failuresBeforeCritical <= 0 {
		failuresBeforeCritical = 3
	}
	if successesBeforePassing <= 0 {
		successesBeforePassing = 2
	}
	return &Hysteresis{
		failuresBeforeCritical: failuresBeforeCritical,
		successesBeforePassing: successesBeforePassing,
		current:                catalog.HealthPassing,
	}
}

// Current returns the stable status.
func (h *Hysteresis) Current() catalog.HealthStatus { return h.current }

// SetCurrent forces the starting state (e.g. after restore).
func (h *Hysteresis) SetCurrent(s catalog.HealthStatus) {
	h.current = s
	h.consecutiveFailures = 0
	h.consecutiveSuccesses = 0
}

// Observe applies one check result. changed is true only on a stable transition.
func (h *Hysteresis) Observe(result catalog.HealthStatus) (newStatus catalog.HealthStatus, changed bool) {
	switch result {
	case catalog.HealthPassing:
		h.consecutiveFailures = 0
		h.consecutiveSuccesses++
		if h.current != catalog.HealthPassing && h.consecutiveSuccesses >= h.successesBeforePassing {
			h.current = catalog.HealthPassing
			h.transitions++
			return h.current, true
		}
	case catalog.HealthCritical:
		h.consecutiveSuccesses = 0
		h.consecutiveFailures++
		if h.current != catalog.HealthCritical && h.consecutiveFailures >= h.failuresBeforeCritical {
			h.current = catalog.HealthCritical
			h.transitions++
			return h.current, true
		}
	case catalog.HealthWarning:
		// Warning does not reset either counter — neither success nor failure.
		// An instance that alternates passing/warning stays at its current stable state
		// unless we want warning to surface immediately when currently passing.
		if h.current == catalog.HealthPassing {
			// surface warning without requiring consecutive counts
			h.current = catalog.HealthWarning
			h.transitions++
			return h.current, true
		}
	case catalog.HealthMaint:
		if h.current != catalog.HealthMaint {
			h.current = catalog.HealthMaint
			h.transitions++
			return h.current, true
		}
	}
	return h.current, false
}

// Transitions returns how many stable transitions have occurred.
func (h *Hysteresis) Transitions() int { return h.transitions }
