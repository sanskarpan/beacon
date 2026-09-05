package gossip_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/gossip"
)

func newUDPTestNode(t *testing.T, name string) *gossip.UDP {
	t.Helper()
	u, err := gossip.NewUDP(gossip.UDPConfig{
		Name: name, BindAddr: "127.0.0.1", AdvertiseAddr: "127.0.0.1", Port: 0,
		Clock: clock.New(), ProbeInterval: 20 * time.Millisecond,
		ProbeTimeout: 10 * time.Millisecond, FailureAfter: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(u.Stop)
	return u
}

func udpAddr(u *gossip.UDP) string {
	for _, member := range u.Members() {
		if member.Name == u.LocalName() {
			return member.Addr + ":" + formatPort(member.Port)
		}
	}
	return ""
}

func formatPort(port int) string {
	if port < 0 {
		return "0"
	}
	return fmt.Sprintf("%d", port)
}

func TestUDPJoinAndBroadcast(t *testing.T) {
	a := newUDPTestNode(t, "a")
	b := newUDPTestNode(t, "b")
	if reached, err := b.Join([]string{udpAddr(a)}); err != nil || reached != 1 {
		t.Fatalf("join: reached=%d err=%v", reached, err)
	}
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for a.Size() < 2 || b.Size() < 2 {
		select {
		case <-deadline.C:
			t.Fatalf("membership did not converge: a=%d b=%d", a.Size(), b.Size())
		default:
			time.Sleep(time.Millisecond)
		}
	}
	received := make(chan []byte, 1)
	a.OnBroadcast(func(_ gossip.NodeID, payload []byte) { received <- payload })
	if err := b.Broadcast([]byte("delta")); err != nil {
		t.Fatal(err)
	}
	broadcastWait := time.NewTimer(time.Second)
	defer broadcastWait.Stop()
	select {
	case payload := <-received:
		if string(payload) != "delta" {
			t.Fatalf("payload=%q", payload)
		}
	case <-broadcastWait.C:
		t.Fatal("broadcast timeout")
	}
}

func TestUDPFailureDetection(t *testing.T) {
	a := newUDPTestNode(t, "a")
	b := newUDPTestNode(t, "b")
	if _, err := b.Join([]string{udpAddr(a)}); err != nil {
		t.Fatal(err)
	}
	b.Stop()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		for _, member := range a.Members() {
			if member.Name == "b" && member.Status == gossip.StatusFailed {
				return
			}
		}
		select {
		case <-deadline.C:
			t.Fatal("peer was not marked failed")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
}
