package health_test

import (
	"testing"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/health"
)

func TestHysteresis_FlappingProducesZeroTransitions(t *testing.T) {
	h := health.NewHysteresis(3, 2)
	h.SetCurrent(catalog.HealthPassing)

	transitions := 0
	for i := 0; i < 100; i++ {
		result := catalog.HealthPassing
		if i%2 == 1 {
			result = catalog.HealthCritical
		}
		if _, changed := h.Observe(result); changed {
			transitions++
		}
	}
	if transitions != 0 {
		t.Fatalf("flapping instance caused %d state transitions; hysteresis is not working", transitions)
	}
}

func TestHysteresis_ThreeFailsThenTwoPass(t *testing.T) {
	h := health.NewHysteresis(3, 2)
	h.SetCurrent(catalog.HealthPassing)

	for i := 0; i < 2; i++ {
		_, changed := h.Observe(catalog.HealthCritical)
		if changed {
			t.Fatal("should not transition before 3 failures")
		}
	}
	st, changed := h.Observe(catalog.HealthCritical)
	if !changed || st != catalog.HealthCritical {
		t.Fatalf("want critical after 3, got %s changed=%v", st, changed)
	}

	_, changed = h.Observe(catalog.HealthPassing)
	if changed {
		t.Fatal("need 2 passes")
	}
	st, changed = h.Observe(catalog.HealthPassing)
	if !changed || st != catalog.HealthPassing {
		t.Fatalf("want passing after 2, got %s", st)
	}
}
