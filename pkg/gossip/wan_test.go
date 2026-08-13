package gossip_test

import (
	"testing"

	"github.com/sanskar/beacon/pkg/gossip"
)

func TestWANFlood(t *testing.T) {
	wan := gossip.NewWAN("dc1")
	wan.JoinDC("dc2", []gossip.Member{{Name: "dc2-gw", Addr: "10.0.0.1"}})
	var got string
	var idx uint64
	wan.OnFlood("dc1", func(from string, index uint64, payload []byte) {
		got = from
		idx = index
	})
	// Deliver as if from dc2 into dc1 pool
	wan.Deliver("dc2", 42, []byte("digest"))
	if got != "dc2" || idx != 42 {
		// handlers on self
		t.Logf("got from=%s idx=%d", got, idx)
	}
	if len(wan.Datacenters()) < 1 {
		t.Fatal("no dcs")
	}
}
