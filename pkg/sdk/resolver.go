package sdk

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/events"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
)

const scheme = "beacon"

var (
	weightKey   = "beacon-weight"
	localityKey = "beacon-locality"
)

// Builder implements resolver.Builder for beacon:// URLs.
//
//	beacon:///payments?tag=v2&zone=us-east-1a&policy=p2c
type Builder struct {
	client *Client
}

// NewBuilder creates a resolver builder.
func NewBuilder(c *Client) *Builder { return &Builder{client: c} }

// Scheme returns "beacon".
func (b *Builder) Scheme() string { return scheme }

// Build starts a long-lived watch.
func (b *Builder) Build(target resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	service, opts := parseTarget(target)
	r := &beaconResolver{
		client:  b.client,
		cc:      cc,
		service: service,
		opts:    opts,
		ctx:     context.Background(),
	}
	r.ctx, r.cancel = context.WithCancel(r.ctx)
	go r.watch()
	return r, nil
}

func parseTarget(target resolver.Target) (string, catalog.QueryOptions) {
	// Endpoint is the service name; URL query holds filters.
	service := target.Endpoint()
	opts := catalog.QueryOptions{Passing: true}
	u, err := url.Parse(target.URL.String())
	if err == nil {
		q := u.Query()
		if t := q.Get("tag"); t != "" {
			opts.Tags = []string{t}
		}
		if z := q.Get("zone"); z != "" {
			opts.Locality = &catalog.Locality{Zone: z}
		}
		if service == "" {
			service = strings.TrimPrefix(u.Path, "/")
		}
	}
	return service, opts
}

type beaconResolver struct {
	client  *Client
	cc      resolver.ClientConn
	service string
	opts    catalog.QueryOptions
	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.Mutex
	lastGood []resolver.Address
	attempt  int
	lastIdx  uint64
}

func (r *beaconResolver) ResolveNow(resolver.ResolveNowOptions) {
	go r.oneShot()
}

func (r *beaconResolver) Close() { r.cancel() }

func (r *beaconResolver) watch() {
	for {
		select {
		case <-r.ctx.Done():
			return
		default:
		}
		opts := r.opts
		opts.MinIndex = r.lastIdx
		opts.Wait = 30 * time.Second
		res, err := r.client.reg.Get(r.ctx, r.service, opts)
		if err != nil {
			r.cc.ReportError(err)
			d := BackoffWithJitter(r.attempt, r.client.rng)
			r.attempt++
			select {
			case <-r.ctx.Done():
				return
			case <-r.client.clk.After(d):
			}
			continue
		}
		r.attempt = 0
		if res != nil {
			r.lastIdx = res.Index
			insts := make([]catalog.Instance, 0, len(res.Instances))
			for _, in := range res.Instances {
				insts = append(insts, *in)
			}
			r.update(insts)
		}
	}
}

func (r *beaconResolver) oneShot() {
	insts, err := r.client.Resolve(r.ctx, r.service, r.opts)
	if err != nil {
		r.cc.ReportError(err)
		return
	}
	r.update(insts)
}

func (r *beaconResolver) update(instances []catalog.Instance) {
	addrs := make([]resolver.Address, 0, len(instances))
	for _, inst := range instances {
		if inst.Health != catalog.HealthPassing && inst.Health != catalog.HealthWarning {
			continue
		}
		if r.client.outlier.IsEjected(inst.Addr()) {
			continue
		}
		attrs := attributes.New(weightKey, inst.Weight).WithValue(localityKey, inst.Locality)
		addrs = append(addrs, resolver.Address{
			Addr:       net.JoinHostPort(inst.Address, strconv.Itoa(inst.Port)),
			Attributes: attrs,
		})
	}

	// ★ NEVER PUSH AN EMPTY ADDRESS LIST.
	if len(addrs) == 0 {
		r.mu.Lock()
		last := r.lastGood
		r.mu.Unlock()
		if len(last) == 0 {
			r.cc.ReportError(fmt.Errorf("no instances available and no cached set for %s", r.service))
			return
		}
		addrs = last
		if r.client.bus != nil {
			r.client.bus.Publish(events.Event{
				Kind:    events.EvPanicModeEntered,
				Service: r.service,
				Detail:  fmt.Sprintf("0 passing; serving last known good set of %d", len(addrs)),
			})
		}
	} else {
		r.mu.Lock()
		r.lastGood = addrs
		r.mu.Unlock()
	}
	_ = r.cc.UpdateState(resolver.State{Addresses: addrs})
}

// RegisterResolver registers the beacon:// scheme globally.
func RegisterResolver(c *Client) {
	resolver.Register(NewBuilder(c))
}
