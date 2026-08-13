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

type rrWriter struct {
	msg *mdns.Msg
}

func (w *rrWriter) LocalAddr() net.Addr  { return &net.UDPAddr{} }
func (w *rrWriter) RemoteAddr() net.Addr { return &net.UDPAddr{} }
func (w *rrWriter) WriteMsg(m *mdns.Msg) error {
	w.msg = m
	return nil
}
func (w *rrWriter) Write([]byte) (int, error) { return 0, nil }
func (w *rrWriter) Close() error              { return nil }
func (w *rrWriter) TsigStatus() error         { return nil }
func (w *rrWriter) TsigTimersOnly(bool)       {}
func (w *rrWriter) Hijack()                   {}

func TestDNSASRV(t *testing.T) {
	cs := catalog.NewStore()
	_, _ = cs.Register(context.Background(), &catalog.Instance{
		ID: "1", Service: "payments", Node: "n1", Address: "10.0.0.5", Port: 8080,
		Health: catalog.HealthPassing, Weight: 2, Tags: []string{"v2"},
	})
	s := beacondns.New(beacondns.Config{Store: store.NewMemory(cs, "ap"), PassingOnly: true})

	m := new(mdns.Msg)
	m.SetQuestion("payments.service.beacon.", mdns.TypeA)
	w := &rrWriter{}
	s.ServeDNS(w, m)
	if w.msg == nil || len(w.msg.Answer) == 0 {
		t.Fatal("expected A answer")
	}

	m.SetQuestion("payments.service.beacon.", mdns.TypeSRV)
	w = &rrWriter{}
	s.ServeDNS(w, m)
	if w.msg == nil || len(w.msg.Answer) == 0 {
		t.Fatal("expected SRV answer")
	}
	srv, ok := w.msg.Answer[0].(*mdns.SRV)
	if !ok || srv.Port != 8080 {
		t.Fatalf("bad srv: %#v", w.msg.Answer[0])
	}

	m.SetQuestion("v2.payments.service.beacon.", mdns.TypeA)
	w = &rrWriter{}
	s.ServeDNS(w, m)
	if w.msg == nil || len(w.msg.Answer) == 0 {
		t.Fatal("tag filter should match")
	}
}
