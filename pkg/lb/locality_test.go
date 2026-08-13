package lb_test

import (
	"testing"

	"github.com/sanskar/beacon/pkg/lb"
)

func TestLocalityPrefersLocalZone(t *testing.T) {
	eps := []*lb.Endpoint{
		{Addr: "local", Weight: 1, Healthy: true, Zone: "z1", Region: "r1"},
		{Addr: "remote", Weight: 1, Healthy: true, Zone: "z2", Region: "r1"},
	}
	p := lb.NewLocalityPicker(eps, "z1", "r1", lb.NewRoundRobin(nil))
	counts := map[string]int{}
	for i := 0; i < 100; i++ {
		ep, done, err := p.Pick(lb.PickInfo{})
		if err != nil {
			t.Fatal(err)
		}
		counts[ep.Addr]++
		done(lb.DoneInfo{})
	}
	if counts["local"] < counts["remote"] {
		t.Fatalf("should prefer local zone: %v", counts)
	}
}

func TestPanicModeRoutesToAll(t *testing.T) {
	eps := []*lb.Endpoint{
		{Addr: "a", Weight: 1, Healthy: false, Zone: "z1"},
		{Addr: "b", Weight: 1, Healthy: false, Zone: "z1"},
		{Addr: "c", Weight: 1, Healthy: true, Zone: "z1"},
	}
	// only 33% healthy < 50% threshold → panic mode uses all
	p := lb.NewLocalityPicker(eps, "z1", "r1", lb.NewRoundRobin(nil))
	seen := map[string]bool{}
	for i := 0; i < 30; i++ {
		ep, done, err := p.Pick(lb.PickInfo{})
		if err != nil {
			t.Fatal(err)
		}
		seen[ep.Addr] = true
		done(lb.DoneInfo{})
	}
	if len(seen) < 2 {
		t.Fatalf("panic mode should include unhealthy endpoints, seen=%v", seen)
	}
}
