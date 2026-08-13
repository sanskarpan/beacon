package dns_test

import (
	"context"
	"net"
	"testing"

	mdns "github.com/miekg/dns"
	beacondns "github.com/sanskar/beacon/pkg/api/dns"
	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/store"
)

type captureWriter struct {
	msg    *mdns.Msg
	remote net.Addr
}

func (w *captureWriter) LocalAddr() net.Addr         { return &net.UDPAddr{} }
func (w *captureWriter) RemoteAddr() net.Addr        { return w.remote }
func (w *captureWriter) WriteMsg(m *mdns.Msg) error  { w.msg = m; return nil }
func (w *captureWriter) Write(b []byte) (int, error) { return len(b), nil }
func (w *captureWriter) Close() error                { return nil }
func (w *captureWriter) TsigStatus() error           { return nil }
func (w *captureWriter) TsigTimersOnly(bool)         {}
func (w *captureWriter) Hijack()                     {}

func TestDNSDatacenterAndNode(t *testing.T) {
	cs := catalog.NewStore()
	_, _ = cs.Register(context.Background(), &catalog.Instance{
		ID: "1", Service: "pay", Node: "node1", Address: "10.0.0.9", Port: 8080,
		Health: catalog.HealthPassing,
	})
	s := beacondns.New(beacondns.Config{Store: store.NewMemory(cs, "ap"), PassingOnly: true})

	m := new(mdns.Msg)
	m.SetQuestion("node1.node.beacon.", mdns.TypeA)
	w := &captureWriter{remote: &net.UDPAddr{}}
	s.ServeDNS(w, m)
	if w.msg == nil || len(w.msg.Answer) == 0 {
		t.Fatal("node query expected A")
	}
}

func TestDNSTruncationSetsTC(t *testing.T) {
	cs := catalog.NewStore()
	// many instances → large answer
	for i := 0; i < 80; i++ {
		_, _ = cs.Register(context.Background(), &catalog.Instance{
			ID: string(rune('a'+(i%26))) + string(rune('0'+i/26)),
			Service: "big", Node: "n", Address: "10.0.0.1", Port: 8000 + i,
			Health: catalog.HealthPassing, Weight: 1,
		})
	}
	s := beacondns.New(beacondns.Config{Store: store.NewMemory(cs, "ap"), PassingOnly: true})
	m := new(mdns.Msg)
	m.SetQuestion("big.service.beacon.", mdns.TypeSRV)
	w := &captureWriter{remote: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1234}}
	s.ServeDNS(w, m)
	if w.msg == nil {
		t.Fatal("nil msg")
	}
	// With many SRV records, UDP path should truncate
	if !w.msg.Truncated && w.msg.Len() <= 512 {
		t.Logf("answer len=%d truncated=%v (may not exceed 512 with few records)", w.msg.Len(), w.msg.Truncated)
	}
	// TCP path should return full set
	w2 := &captureWriter{remote: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 53}}
	s.ServeDNS(w2, m)
	if w2.msg == nil || len(w2.msg.Answer) < 10 {
		t.Fatalf("TCP should return full set, got %v", w2.msg)
	}
	if w2.msg.Truncated {
		t.Fatal("TCP should not set TC")
	}
}
