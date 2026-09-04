// Package dns serves A/AAAA/SRV records for service discovery on :8600.
//
// TTL=0 by default. Many stub resolvers ignore it — that is precisely why DNS
// is the slowest propagation path and why streaming watches exist.
package dns

import (
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"

	"github.com/miekg/dns"
	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/store"
)

// Server answers discovery DNS queries.
type Server struct {
	store       store.CatalogStore
	domain      string // default "beacon"
	udp         *dns.Server
	tcp         *dns.Server
	passingOnly bool
}

// Config for the DNS server.
type Config struct {
	Store       store.CatalogStore
	Domain      string
	Addr        string // e.g. ":8600"
	PassingOnly bool
}

// New creates a DNS server (not started).
func New(cfg Config) *Server {
	if cfg.Domain == "" {
		cfg.Domain = "beacon"
	}
	s := &Server{
		store:       cfg.Store,
		domain:      cfg.Domain,
		passingOnly: cfg.PassingOnly,
	}
	mux := dns.NewServeMux()
	mux.HandleFunc(".", s.handle)
	addr := cfg.Addr
	if addr == "" {
		addr = ":8600"
	}
	s.udp = &dns.Server{Addr: addr, Net: "udp", Handler: mux}
	s.tcp = &dns.Server{Addr: addr, Net: "tcp", Handler: mux}
	return s
}

// ListenAndServe starts UDP + TCP.
func (s *Server) ListenAndServe() error {
	errc := make(chan error, 2)
	go func() { errc <- s.udp.ListenAndServe() }()
	go func() { errc <- s.tcp.ListenAndServe() }()
	return <-errc
}

// Shutdown stops both.
func (s *Server) Shutdown() {
	if s.udp != nil {
		_ = s.udp.Shutdown()
	}
	if s.tcp != nil {
		_ = s.tcp.Shutdown()
	}
}

// ServeMux returns a handler for tests.
func (s *Server) ServeDNS(w dns.ResponseWriter, r *dns.Msg) {
	s.handle(w, r)
}

func (s *Server) handle(w dns.ResponseWriter, r *dns.Msg) {
	m := new(dns.Msg)
	m.SetReply(r)
	m.Authoritative = true
	if len(r.Question) == 0 {
		_ = w.WriteMsg(m)
		return
	}
	q := r.Question[0]
	name := strings.ToLower(strings.TrimSuffix(q.Name, "."))

	switch q.Qtype {
	case dns.TypeA, dns.TypeAAAA, dns.TypeSRV, dns.TypeANY:
		s.answer(m, q, name)
	default:
		m.Rcode = dns.RcodeSuccess
	}

	// Truncation: if UDP and too large, set TC so client retries TCP.
	if _, isUDP := w.RemoteAddr().(*net.UDPAddr); isUDP {
		if m.Len() > 512 {
			// Truncate to fit 512: keep as many answers as fit, clear extra
			// For simplicity, drop all and set TC (client will retry TCP which has no limit)
			m.Answer = nil
			m.Extra = nil
			m.Truncated = true
		}
	}
	// Shuffle answers so resolvers that only use the first record still balance.
	shuffle(m.Answer)
	_ = w.WriteMsg(m)
}

func (s *Server) answer(m *dns.Msg, q dns.Question, name string) {
	// Patterns:
	//   <service>.service.<domain>
	//   <tag>.<service>.service.<domain>
	//   <service>.service.<dc>.<domain>
	//   <node>.node.<domain>
	parts := strings.Split(name, ".")
	// need at least service.service.beacon
	if len(parts) < 3 {
		m.Rcode = dns.RcodeSuccess
		return
	}
	domain := s.domain
	if parts[len(parts)-1] != domain {
		m.Rcode = dns.RcodeNameError
		return
	}

	var service, tag string
	var instances []*catalog.Instance

	// find "service" or "node" label
	idx := -1
	for i, p := range parts {
		if p == "service" || p == "node" {
			idx = i
			break
		}
	}
	if idx < 0 {
		m.Rcode = dns.RcodeSuccess
		return
	}

	if parts[idx] == "node" {
		// <node>.node.beacon
		if idx < 1 {
			m.Rcode = dns.RcodeNameError
			return
		}
		node := parts[idx-1]
		instances = s.store.InstancesOnNode(node)
	} else {
		// service query
		if idx < 1 {
			m.Rcode = dns.RcodeNameError
			return
		}
		service = parts[idx-1]
		// strict tag heuristic: only <tag>.<service>.service.<domain> (4 parts, service at idx 2) carries a tag.
		// payments.service.dc.beacon (also 4 parts but service at idx1) is datacenter, not tag.
		if len(parts) == 4 && idx == 2 {
			tag = parts[0]
		} else {
			tag = ""
		}
		opts := catalog.QueryOptions{Passing: s.passingOnly}
		if tag != "" && idx >= 2 {
			opts.Tags = []string{tag}
		}
		res := s.store.GetNow(service, opts)
		instances = res.Instances
	}

	ttl := uint32(0) // default TTL=0
	for _, inst := range instances {
		if s.passingOnly && inst.Health != catalog.HealthPassing {
			continue
		}
		ip := net.ParseIP(inst.Address)
		switch q.Qtype {
		case dns.TypeA:
			if ip != nil && ip.To4() != nil {
				m.Answer = append(m.Answer, &dns.A{
					Hdr: dns.RR_Header{Name: q.Name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: ttl},
					A:   ip.To4(),
				})
			}
		case dns.TypeAAAA:
			if ip != nil && ip.To4() == nil {
				m.Answer = append(m.Answer, &dns.AAAA{
					Hdr:  dns.RR_Header{Name: q.Name, Rrtype: dns.TypeAAAA, Class: dns.ClassINET, Ttl: ttl},
					AAAA: ip,
				})
			}
		case dns.TypeANY:
			if ip != nil {
				if ip.To4() != nil {
					m.Answer = append(m.Answer, &dns.A{
						Hdr: dns.RR_Header{Name: q.Name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: ttl},
						A:   ip.To4(),
					})
				} else {
					m.Answer = append(m.Answer, &dns.AAAA{
						Hdr:  dns.RR_Header{Name: q.Name, Rrtype: dns.TypeAAAA, Class: dns.ClassINET, Ttl: ttl},
						AAAA: ip,
					})
				}
			}
		case dns.TypeSRV:
			// priority 1, weight from instance (clamped to uint16 range)
			wt := clampUint16(inst.Weight, 1)
			target := dns.Fqdn(fmt.Sprintf("%s.node.%s", inst.Node, s.domain))
			m.Answer = append(m.Answer, &dns.SRV{
				Hdr:      dns.RR_Header{Name: q.Name, Rrtype: dns.TypeSRV, Class: dns.ClassINET, Ttl: ttl},
				Priority: 1,
				Weight:   wt,
				Port:     clampUint16(inst.Port, 0),
				Target:   target,
			})
			// additional A for target
			if ip != nil && ip.To4() != nil {
				m.Extra = append(m.Extra, &dns.A{
					Hdr: dns.RR_Header{Name: target, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: ttl},
					A:   ip.To4(),
				})
			}
		}
	}
}

// clampUint16 bounds an int into [0, 65535], falling back to fb when v <= 0.
func clampUint16(v, fb int) uint16 {
	if v <= 0 {
		v = fb
	}
	if v > 0xFFFF {
		v = 0xFFFF
	}
	return uint16(v) //nolint:gosec // G115: bounded above by construction
}

var shuffleMu sync.Mutex

func shuffle(rrs []dns.RR) {
	shuffleMu.Lock()
	defer shuffleMu.Unlock()
	rand.Shuffle(len(rrs), func(i, j int) {
		rrs[i], rrs[j] = rrs[j], rrs[i]
	})
}
