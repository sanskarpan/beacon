package dns_test

import (
	"fmt"
	"net"
	"testing"

	miedns "github.com/miekg/dns"
	beacondns "github.com/sanskar/beacon/pkg/api/dns"
	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/store"
)

// TODO-024: Full datacenter DNS semantics.

func TestDNS_ServiceGlobally(t *testing.T) {
	cs := catalog.NewStore()
	for i := 0; i < 5; i++ {
		_, _ = cs.Register(nil, &catalog.Instance{
			ID: fmt.Sprintf("web-%d", i), Service: "web",
			Address: "10.0.0.1", Port: 8080 + i,
			Health: catalog.HealthPassing, Node: fmt.Sprintf("node-%d", i),
		})
	}

	srv := beacondns.New(beacondns.Config{Store: store.NewMemory(cs, "ap"), Domain: "beacon"})
	msg := new(miedns.Msg)
	msg.SetQuestion("web.service.beacon.", miedns.TypeA)
	w := &fakeWriter{remote: &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 53}}
	srv.ServeDNS(w, msg)

	if w.msg.Rcode != miedns.RcodeSuccess {
		t.Fatalf("expected NOERROR, got %d", w.msg.Rcode)
	}
	if len(w.msg.Answer) != 5 {
		t.Fatalf("expected 5 A records, got %d", len(w.msg.Answer))
	}
}

func TestDNS_NonexistentService_NODATA(t *testing.T) {
	cs := catalog.NewStore()
	srv := beacondns.New(beacondns.Config{Store: store.NewMemory(cs, "ap"), Domain: "beacon"})
	msg := new(miedns.Msg)
	msg.SetQuestion("nonexistent.service.beacon.", miedns.TypeA)
	w := &fakeWriter{remote: &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 53}}
	srv.ServeDNS(w, msg)

	if w.msg.Rcode != miedns.RcodeSuccess {
		t.Fatalf("expected NODATA (NOERROR), got rcode=%d", w.msg.Rcode)
	}
	if len(w.msg.Answer) != 0 {
		t.Fatalf("expected empty answer, got %d records", len(w.msg.Answer))
	}
}

func TestDNS_InvalidFormat_NODATA(t *testing.T) {
	cs := catalog.NewStore()
	srv := beacondns.New(beacondns.Config{Store: store.NewMemory(cs, "ap"), Domain: "beacon"})

	msg := new(miedns.Msg)
	msg.SetQuestion("foo.bar.beacon.", miedns.TypeA)
	w := &fakeWriter{remote: &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 53}}
	srv.ServeDNS(w, msg)

	if w.msg.Rcode != miedns.RcodeSuccess {
		t.Fatalf("expected NODATA for invalid format, got rcode=%d", w.msg.Rcode)
	}
}

func TestDNS_WrongDomain_NXDOMAIN(t *testing.T) {
	cs := catalog.NewStore()
	srv := beacondns.New(beacondns.Config{Store: store.NewMemory(cs, "ap"), Domain: "beacon"})
	msg := new(miedns.Msg)
	msg.SetQuestion("web.service.other.", miedns.TypeA)
	w := &fakeWriter{remote: &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 53}}
	srv.ServeDNS(w, msg)

	if w.msg.Rcode != miedns.RcodeNameError {
		t.Fatalf("expected NXDOMAIN for wrong domain, got rcode=%d", w.msg.Rcode)
	}
}

func TestDNS_TagFilter(t *testing.T) {
	cs := catalog.NewStore()
	_, _ = cs.Register(nil, &catalog.Instance{
		ID: "api-v1", Service: "api", Address: "10.0.1.1", Port: 8080,
		Health: catalog.HealthPassing, Node: "node-0", Tags: []string{"v1"},
	})
	_, _ = cs.Register(nil, &catalog.Instance{
		ID: "api-v2", Service: "api", Address: "10.0.2.1", Port: 8080,
		Health: catalog.HealthPassing, Node: "node-1", Tags: []string{"v2"},
	})

	srv := beacondns.New(beacondns.Config{Store: store.NewMemory(cs, "ap"), Domain: "beacon"})
	msg := new(miedns.Msg)
	msg.SetQuestion("v1.api.service.beacon.", miedns.TypeA)
	w := &fakeWriter{remote: &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 53}}
	srv.ServeDNS(w, msg)

	if w.msg.Rcode != miedns.RcodeSuccess {
		t.Fatalf("expected NOERROR, got %d", w.msg.Rcode)
	}
	if len(w.msg.Answer) != 1 {
		t.Fatalf("expected 1 A record for tag v1, got %d", len(w.msg.Answer))
	}
}

func TestDNS_SRVRecord(t *testing.T) {
	cs := catalog.NewStore()
	_, _ = cs.Register(nil, &catalog.Instance{
		ID: "web-1", Service: "web", Address: "10.0.0.1", Port: 8080,
		Health: catalog.HealthPassing, Node: "node-0", Weight: 5,
	})

	srv := beacondns.New(beacondns.Config{Store: store.NewMemory(cs, "ap"), Domain: "beacon"})
	msg := new(miedns.Msg)
	msg.SetQuestion("web.service.beacon.", miedns.TypeSRV)
	w := &fakeWriter{remote: &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 53}}
	srv.ServeDNS(w, msg)

	if w.msg.Rcode != miedns.RcodeSuccess {
		t.Fatalf("expected NOERROR, got %d", w.msg.Rcode)
	}
	if len(w.msg.Answer) != 1 {
		t.Fatalf("expected 1 SRV record, got %d", len(w.msg.Answer))
	}
	srvRec, ok := w.msg.Answer[0].(*miedns.SRV)
	if !ok {
		t.Fatal("answer is not SRV record")
	}
	if srvRec.Port != 8080 {
		t.Fatalf("expected port 8080, got %d", srvRec.Port)
	}
	if srvRec.Weight != 5 {
		t.Fatalf("expected weight 5, got %d", srvRec.Weight)
	}
}

func TestDNS_NodeQuery(t *testing.T) {
	cs := catalog.NewStore()
	_, _ = cs.Register(nil, &catalog.Instance{
		ID: "svc-1", Service: "web", Address: "10.0.0.1", Port: 8080,
		Health: catalog.HealthPassing, Node: "web-node-1",
	})
	_, _ = cs.Register(nil, &catalog.Instance{
		ID: "svc-2", Service: "api", Address: "10.0.0.2", Port: 9090,
		Health: catalog.HealthPassing, Node: "web-node-1",
	})

	srv := beacondns.New(beacondns.Config{Store: store.NewMemory(cs, "ap"), Domain: "beacon"})
	msg := new(miedns.Msg)
	msg.SetQuestion("web-node-1.node.beacon.", miedns.TypeA)
	w := &fakeWriter{remote: &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 53}}
	srv.ServeDNS(w, msg)

	if w.msg.Rcode != miedns.RcodeSuccess {
		t.Fatalf("expected NOERROR, got %d", w.msg.Rcode)
	}
	if len(w.msg.Answer) != 2 {
		t.Fatalf("expected 2 A records for node, got %d", len(w.msg.Answer))
	}
}
