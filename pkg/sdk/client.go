// Package sdk is the beacon client: registration helpers, resolver, interceptors.
//
// Extends the gRPC-interceptors project: its interceptor chain is reused; beacon
// adds OutcomeReporter that feeds passive health checking.
package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
	"github.com/sanskar/beacon/pkg/health/outlier"
	"github.com/sanskar/beacon/pkg/lb"
	"github.com/sanskar/beacon/pkg/store"
	"github.com/sanskar/beacon/pkg/trace"
	"google.golang.org/grpc"
)

// Registry is the minimal client-side view of discovery.
type Registry interface {
	Register(ctx context.Context, inst *catalog.Instance) (uint64, error)
	Deregister(ctx context.Context, id string) (uint64, error)
	GetNow(service string, opts catalog.QueryOptions) *catalog.Result
	Get(ctx context.Context, service string, opts catalog.QueryOptions) (*catalog.Result, error)
}

// StoreAdapter adapts store.CatalogStore to Registry.
type StoreAdapter struct{ S store.CatalogStore }

func (a StoreAdapter) Register(ctx context.Context, inst *catalog.Instance) (uint64, error) {
	return a.S.Register(ctx, inst)
}
func (a StoreAdapter) Deregister(ctx context.Context, id string) (uint64, error) {
	return a.S.Deregister(ctx, id)
}
func (a StoreAdapter) GetNow(service string, opts catalog.QueryOptions) *catalog.Result {
	return a.S.GetNow(service, opts)
}
func (a StoreAdapter) Get(ctx context.Context, service string, opts catalog.QueryOptions) (*catalog.Result, error) {
	return a.S.Get(ctx, service, opts)
}

// Client is the application-facing SDK.
type Client struct {
	reg      Registry
	clk      clock.Clock
	bus      *events.Bus
	outlier  *outlier.Detector
	cacheDir string
	mu       sync.Mutex
	// last good address sets per service
	lastGood map[string][]catalog.Instance
	// renew cancel funcs
	renewers map[string]context.CancelFunc
	// rng is guarded by the package-level backoffMu in BackoffWithJitter.
	rng *rand.Rand
}

// Config for the client.
type Config struct {
	Registry Registry
	Clock    clock.Clock
	Bus      *events.Bus
	Outlier  *outlier.Detector
	CacheDir string
}

// New creates a client.
func New(cfg Config) *Client {
	if cfg.Clock == nil {
		cfg.Clock = clock.New()
	}
	if cfg.Outlier == nil {
		cfg.Outlier = outlier.New(outlier.DefaultConfig(), cfg.Clock, cfg.Bus)
	}
	return &Client{
		reg:      cfg.Registry,
		clk:      cfg.Clock,
		bus:      cfg.Bus,
		outlier:  cfg.Outlier,
		cacheDir: cfg.CacheDir,
		lastGood: make(map[string][]catalog.Instance),
		renewers: make(map[string]context.CancelFunc),
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Outlier returns the passive health detector.
func (c *Client) Outlier() *outlier.Detector { return c.outlier }

// Register registers an instance and starts automatic lease renewal if a lease TTL is set.
func (c *Client) Register(ctx context.Context, inst *catalog.Instance) (*catalog.Instance, error) {
	if inst.TraceID == "" {
		inst.TraceID = trace.NewID()
	}
	ctx = events.ContextWithTrace(ctx, inst.TraceID)
	_, err := c.reg.Register(ctx, inst)
	if err != nil {
		return nil, err
	}
	if inst.Lease != nil && inst.Lease.TTL > 0 {
		c.startRenew(inst)
	}
	return inst, nil
}

// Deregister stops renewal and removes the instance.
func (c *Client) Deregister(ctx context.Context, id string) error {
	c.mu.Lock()
	if cancel, ok := c.renewers[id]; ok {
		cancel()
		delete(c.renewers, id)
	}
	c.mu.Unlock()
	_, err := c.reg.Deregister(ctx, id)
	return err
}

func (c *Client) startRenew(inst *catalog.Instance) {
	ctx, cancel := context.WithCancel(context.Background())
	c.mu.Lock()
	if old, ok := c.renewers[inst.ID]; ok {
		old()
	}
	c.renewers[inst.ID] = cancel
	c.mu.Unlock()

	ttl := inst.Lease.TTL
	interval := ttl / 2
	if interval < time.Second {
		interval = time.Second
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-c.clk.After(interval):
				// re-register as cheap renewal when dedicated renew API not on Registry
				_, _ = c.reg.Register(ctx, inst)
			}
		}
	}()
}

// Resolve returns instances for a service, with caching and never-empty semantics.
func (c *Client) Resolve(ctx context.Context, service string, opts catalog.QueryOptions) ([]catalog.Instance, error) {
	if c.bus != nil {
		c.bus.Publish(events.Event{Kind: events.EvResolveRequest, Service: service})
	}
	var res *catalog.Result
	var err error
	if opts.MinIndex > 0 || opts.Wait > 0 {
		res, err = c.reg.Get(ctx, service, opts)
	} else {
		res = c.reg.GetNow(service, opts)
	}
	if err != nil {
		// serve disk/memory cache
		if cached := c.loadCache(service); len(cached) > 0 {
			if c.bus != nil {
				c.bus.Publish(events.Event{Kind: events.EvStaleEndpointUsed, Service: service, Detail: "registry error; serving cache"})
			}
			return cached, nil
		}
		return nil, err
	}

	passing := make([]catalog.Instance, 0, len(res.Instances))
	for _, inst := range res.Instances {
		if opts.Passing && inst.Health != catalog.HealthPassing {
			continue
		}
		// filter ejected by outlier
		if c.outlier.IsEjected(inst.Addr()) {
			continue
		}
		if inst.Health == catalog.HealthPassing || inst.Health == catalog.HealthWarning {
			passing = append(passing, *inst)
		}
	}

	// ★ NEVER PUSH AN EMPTY ADDRESS LIST.
	if len(passing) == 0 {
		c.mu.Lock()
		last := c.lastGood[service]
		c.mu.Unlock()
		if len(last) == 0 {
			last = c.loadCache(service)
		}
		if len(last) > 0 {
			if c.bus != nil {
				c.bus.Publish(events.Event{
					Kind:    events.EvPanicModeEntered,
					Service: service,
					Detail:  fmt.Sprintf("0 passing; serving last known good set of %d", len(last)),
				})
			}
			return last, nil
		}
		return nil, fmt.Errorf("no instances available for %s", service)
	}

	c.mu.Lock()
	c.lastGood[service] = passing
	c.mu.Unlock()
	c.persistCache(service, passing)
	return passing, nil
}

// Pick selects one endpoint using a policy.
func (c *Client) Pick(ctx context.Context, service, policy string, opts catalog.QueryOptions) (*catalog.Instance, func(error), error) {
	insts, err := c.Resolve(ctx, service, opts)
	if err != nil {
		return nil, nil, err
	}
	eps := make([]*lb.Endpoint, len(insts))
	for i := range insts {
		eps[i] = &lb.Endpoint{
			Addr:    insts[i].Addr(),
			Weight:  insts[i].Weight,
			Healthy: insts[i].Health == catalog.HealthPassing,
			Zone:    insts[i].Locality.Zone,
			Region:  insts[i].Locality.Region,
		}
	}
	picker := lb.NewPicker(policy, eps)
	if p2c, ok := picker.(*lb.P2C); ok {
		p2c.OnDone = func(addr string, err error) {
			c.outlier.Record(addr, err, 0)
		}
	}
	ep, done, err := picker.Pick(lb.PickInfo{})
	if err != nil {
		return nil, nil, err
	}
	var chosen *catalog.Instance
	for i := range insts {
		if insts[i].Addr() == ep.Addr {
			chosen = &insts[i]
			break
		}
	}
	return chosen, func(e error) {
		if done != nil {
			done(lb.DoneInfo{Err: e})
		}
	}, nil
}

// OutcomeReporter returns a unary client interceptor that feeds outlier detection.
// This is the seam with the gRPC-interceptors project.
func (c *Client) OutcomeReporter() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any,
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		start := c.clk.Now()
		err := invoker(ctx, method, req, reply, cc, opts...)
		target := method
		if cc != nil {
			target = cc.Target()
		}
		c.outlier.Record(target, err, c.clk.Now().Sub(start))
		return err
	}
}

// StreamOutcomeReporter is the stream counterpart for passive health.
func (c *Client) StreamOutcomeReporter() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		start := c.clk.Now()
		cs, err := streamer(ctx, desc, cc, method, opts...)
		target := method
		if cc != nil {
			target = cc.Target()
		}
		c.outlier.Record(target, err, c.clk.Now().Sub(start))
		return cs, err
	}
}

var backoffMu sync.Mutex

// BackoffWithJitter returns full-jitter reconnect delay.
func BackoffWithJitter(attempt int, rng *rand.Rand) time.Duration {
	if rng == nil {
		rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	base := time.Duration(math.Min(
		float64(time.Second)*math.Pow(2, float64(attempt)),
		float64(30*time.Second),
	))
	if base <= 0 {
		return 0
	}
	backoffMu.Lock()
	v := rng.Int63n(int64(base))
	backoffMu.Unlock()
	return time.Duration(v)
}

func (c *Client) persistCache(service string, insts []catalog.Instance) {
	if c.cacheDir == "" {
		return
	}
	_ = os.MkdirAll(c.cacheDir, 0o750)
	// Path is SDK-configured cacheDir joined with the resolver service name, not remote input.
	path := filepath.Join(c.cacheDir, service+".json")
	b, _ := json.Marshal(insts)
	_ = os.WriteFile(path, b, 0o600) //nolint:gosec // G304: cache file under configured cacheDir
}

func (c *Client) loadCache(service string) []catalog.Instance {
	c.mu.Lock()
	if v, ok := c.lastGood[service]; ok && len(v) > 0 {
		c.mu.Unlock()
		return v
	}
	c.mu.Unlock()
	if c.cacheDir == "" {
		return nil
	}
	b, err := os.ReadFile(filepath.Join(c.cacheDir, service+".json")) //nolint:gosec // G304: cache file under configured cacheDir
	if err != nil {
		return nil
	}
	var insts []catalog.Instance
	if json.Unmarshal(b, &insts) != nil {
		return nil
	}
	return insts
}

// GracefulShutdown deregisters all tracked instances.
func (c *Client) GracefulShutdown(ctx context.Context) {
	c.mu.Lock()
	ids := make([]string, 0, len(c.renewers))
	for id, cancel := range c.renewers {
		cancel()
		ids = append(ids, id)
	}
	c.renewers = make(map[string]context.CancelFunc)
	c.mu.Unlock()
	for _, id := range ids {
		_, _ = c.reg.Deregister(ctx, id)
	}
}
