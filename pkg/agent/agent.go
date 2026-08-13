// Package agent implements the node agent: local state, health checks, anti-entropy.
//
// The agent's local state is AUTHORITATIVE for services registered with it.
// The catalog is a replica. Consequences:
//  1. Catalog data loss → agents repopulate within one anti-entropy interval.
//  2. Operator deletes an agent-owned instance from the catalog → agent puts it back.
//  3. Sync interval scales with cluster size and is jittered per agent.
package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
	"github.com/sanskar/beacon/pkg/health"
	"github.com/sanskar/beacon/pkg/trace"
)

// CatalogClient is the agent's view of the control-plane catalog.
type CatalogClient interface {
	Register(ctx context.Context, inst *catalog.Instance) (uint64, error)
	Deregister(ctx context.Context, id string) (uint64, error)
	NodeServices(ctx context.Context, node string) (map[string]*catalog.Instance, error)
	UpdateHealth(ctx context.Context, id string, h catalog.HealthStatus) (uint64, error)
}

// LocalClient wraps an in-process catalog.Store as CatalogClient.
type LocalClient struct {
	Store *catalog.Store
	Node  string
}

func (c *LocalClient) Register(ctx context.Context, inst *catalog.Instance) (uint64, error) {
	return c.Store.Register(ctx, inst)
}
func (c *LocalClient) Deregister(ctx context.Context, id string) (uint64, error) {
	return c.Store.Deregister(ctx, id)
}
func (c *LocalClient) UpdateHealth(ctx context.Context, id string, h catalog.HealthStatus) (uint64, error) {
	return c.Store.UpdateHealth(ctx, id, h)
}
func (c *LocalClient) NodeServices(ctx context.Context, node string) (map[string]*catalog.Instance, error) {
	_ = ctx
	list := c.Store.InstancesOnNode(node)
	out := make(map[string]*catalog.Instance, len(list))
	for _, inst := range list {
		out[inst.ID] = inst
	}
	return out, nil
}

// ReadClient resolves remote catalog reads (optional).
type ReadClient interface {
	GetNow(service string, opts catalog.QueryOptions) *catalog.Result
}

// Agent owns local registrations and runs their checks.
type Agent struct {
	mu          sync.RWMutex
	nodeName    string
	local       map[string]*catalog.Instance // authoritative local state
	client      CatalogClient
	reader      ReadClient
	runner      *health.Runner
	bus         *events.Bus
	clk         clock.Clock
	dataDir     string
	rng         *rand.Rand
	localCh     chan struct{}
	clusterSize func() int
	// rate limit immediate syncs
	lastSync time.Time
	minSync  time.Duration
	// read cache with staleness allowance
	readCache    map[string]cachedRead
	maxStale     time.Duration
	serveStale   bool
}

type cachedRead struct {
	result *catalog.Result
	at     time.Time
}

// Config for constructing an agent.
type Config struct {
	NodeName    string
	Client      CatalogClient
	Reader      ReadClient // optional remote catalog reads
	Store       *catalog.Store // used for local health updates when Client is LocalClient
	Bus         *events.Bus
	Clock       clock.Clock
	DataDir     string
	ClusterSize func() int
	// MaxStale is how long a cached read may be served when the server is unreachable.
	MaxStale time.Duration
}

// New creates an agent.
func New(cfg Config) *Agent {
	if cfg.Clock == nil {
		cfg.Clock = clock.New()
	}
	if cfg.ClusterSize == nil {
		cfg.ClusterSize = func() int { return 1 }
	}
	maxStale := cfg.MaxStale
	if maxStale <= 0 {
		maxStale = 30 * time.Second
	}
	a := &Agent{
		nodeName:    cfg.NodeName,
		local:       make(map[string]*catalog.Instance),
		client:      cfg.Client,
		reader:      cfg.Reader,
		bus:         cfg.Bus,
		clk:         cfg.Clock,
		dataDir:     cfg.DataDir,
		rng:         rand.New(rand.NewSource(time.Now().UnixNano())),
		localCh:     make(chan struct{}, 1),
		clusterSize: cfg.ClusterSize,
		minSync:     100 * time.Millisecond,
		readCache:   make(map[string]cachedRead),
		maxStale:    maxStale,
		serveStale:  true,
	}
	store := cfg.Store
	if store == nil {
		// runner still needs a store for UpdateCheckStatus; use a private one if missing
		store = catalog.NewStore(catalog.WithClock(cfg.Clock), catalog.WithBus(cfg.Bus))
	}
	a.runner = health.NewRunner(store, cfg.Clock, 32, health.WithRunnerBus(cfg.Bus))
	a.runner.OnCriticalLong = func(id string) {
		_ = a.Deregister(context.Background(), id)
	}
	_ = a.load()
	return a
}

// NodeName returns the agent node name.
func (a *Agent) NodeName() string { return a.nodeName }

// Services returns a copy of local registrations.
func (a *Agent) Services() map[string]*catalog.Instance {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make(map[string]*catalog.Instance, len(a.local))
	for k, v := range a.local {
		out[k] = v.Clone()
	}
	return out
}

// Register adds a local service and triggers immediate sync.
func (a *Agent) Register(ctx context.Context, inst *catalog.Instance) error {
	if inst.ID == "" {
		inst.ID = fmt.Sprintf("%s-%s", inst.Service, trace.NewID()[:8])
	}
	inst.Node = a.nodeName
	if inst.TraceID == "" {
		inst.TraceID = events.TraceFrom(ctx)
	}
	if inst.TraceID == "" {
		inst.TraceID = trace.NewID()
	}
	cp := inst.Clone()

	a.mu.Lock()
	a.local[cp.ID] = cp
	a.mu.Unlock()

	a.runner.Add(cp)
	_ = a.persist()
	a.signalLocal()

	// push immediately
	_, err := a.client.Register(events.ContextWithTrace(ctx, cp.TraceID), cp)
	return err
}

// Deregister removes a local service.
func (a *Agent) Deregister(ctx context.Context, id string) error {
	a.mu.Lock()
	delete(a.local, id)
	a.mu.Unlock()
	a.runner.Remove(id)
	_ = a.persist()
	a.signalLocal()
	_, err := a.client.Deregister(ctx, id)
	return err
}

// TTLPass pushes a TTL check result.
func (a *Agent) TTLPass(instanceID, checkID string, status catalog.HealthStatus, output string) error {
	return a.runner.TTLPass(instanceID, catalog.CheckID(checkID), status, output)
}

// StartAntiEntropy runs the sync loop until ctx is done.
func (a *Agent) StartAntiEntropy(ctx context.Context) {
	go a.antiEntropyLoop(ctx)
}

// Sync runs one reconciliation (exported for tests).
func (a *Agent) Sync(ctx context.Context) error {
	return a.sync(ctx)
}

func (a *Agent) antiEntropyLoop(ctx context.Context) {
	for {
		base := SyncInterval(a.clusterSize())
		var jitter time.Duration
		if base > 0 {
			jitter = time.Duration(a.rng.Int63n(int64(base / 4)))
		}
		select {
		case <-a.clk.After(base + jitter):
			if err := a.sync(ctx); err != nil && a.bus != nil {
				a.bus.Publish(events.Event{Kind: events.EvAntiEntropySync, Detail: err.Error(), Node: a.nodeName})
			}
		case <-a.localCh:
			// Immediate sync on local change, rate-limited.
			a.mu.Lock()
			since := a.clk.Now().Sub(a.lastSync)
			a.mu.Unlock()
			if since < a.minSync {
				select {
				case <-a.clk.After(a.minSync - since):
				case <-ctx.Done():
					return
				}
			}
			_ = a.sync(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (a *Agent) sync(ctx context.Context) error {
	local := a.Services()
	remote, err := a.client.NodeServices(ctx, a.nodeName)
	if err != nil {
		return err
	}

	var adds, removes, updates int

	// Anything we own that the catalog is missing or wrong → push.
	for id, svc := range local {
		r, inCatalog := remote[id]
		if !inCatalog {
			if _, err := a.client.Register(ctx, svc); err != nil {
				return err
			}
			adds++
		} else if !svc.Equal(r) {
			if _, err := a.client.Register(ctx, svc); err != nil {
				return err
			}
			updates++
		}
	}

	// Catalog thinks is on this node that we don't own → remove.
	// This makes the agent authoritative rather than merely a writer.
	for id := range remote {
		if _, ours := local[id]; !ours {
			if _, err := a.client.Deregister(ctx, id); err != nil {
				return err
			}
			removes++
		}
	}

	a.mu.Lock()
	a.lastSync = a.clk.Now()
	a.mu.Unlock()

	if a.bus != nil {
		a.bus.Publish(events.Event{
			Kind:    events.EvAntiEntropySync,
			Node:    a.nodeName,
			Adds:    adds,
			Removes: removes,
			Updates: updates,
		})
	}
	return nil
}

func (a *Agent) signalLocal() {
	select {
	case a.localCh <- struct{}{}:
	default:
	}
}

// SyncInterval scales with cluster size.
//
//	≤128: 1m · ≤512: 5m · ≤2048: 10m · ≤8192: 20m · else 30m
func SyncInterval(clusterSize int) time.Duration {
	switch {
	case clusterSize <= 128:
		return time.Minute
	case clusterSize <= 512:
		return 5 * time.Minute
	case clusterSize <= 2048:
		return 10 * time.Minute
	case clusterSize <= 8192:
		return 20 * time.Minute
	default:
		return 30 * time.Minute
	}
}

func (a *Agent) persist() error {
	if a.dataDir == "" {
		return nil
	}
	if err := os.MkdirAll(a.dataDir, 0o755); err != nil {
		return err
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	path := filepath.Join(a.dataDir, "services.json")
	f, err := os.Create(path + ".tmp")
	if err != nil {
		return err
	}
	if err := json.NewEncoder(f).Encode(a.local); err != nil {
		f.Close()
		return err
	}
	f.Close()
	return os.Rename(path+".tmp", path)
}

func (a *Agent) load() error {
	if a.dataDir == "" {
		return nil
	}
	path := filepath.Join(a.dataDir, "services.json")
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	var m map[string]*catalog.Instance
	if err := json.NewDecoder(f).Decode(&m); err != nil {
		return err
	}
	a.mu.Lock()
	a.local = m
	a.mu.Unlock()
	for _, inst := range m {
		a.runner.Add(inst)
	}
	return nil
}

// Stop stops health checks.
func (a *Agent) Stop() {
	a.runner.Stop()
}

// ResolveService reads a service from the control plane (or cache).
// When the server is unreachable, serves a cached result if fresher than MaxStale.
func (a *Agent) ResolveService(ctx context.Context, service string, opts catalog.QueryOptions) (*catalog.Result, error) {
	_ = ctx
	if a.reader != nil {
		res := a.reader.GetNow(service, opts)
		if res != nil {
			a.mu.Lock()
			a.readCache[service] = cachedRead{result: res, at: a.clk.Now()}
			a.mu.Unlock()
			return res, nil
		}
	}
	// try local catalog instances we own as a last resort
	a.mu.RLock()
	cached, ok := a.readCache[service]
	a.mu.RUnlock()
	if ok && a.serveStale && a.clk.Now().Sub(cached.at) <= a.maxStale {
		out := *cached.result
		out.Stale = true
		out.LastContact = a.clk.Now().Sub(cached.at)
		if a.bus != nil {
			a.bus.Publish(events.Event{
				Kind:    events.EvStaleEndpointUsed,
				Service: service,
				Detail:  "serving cached catalog read",
			})
		}
		return &out, nil
	}
	// build from local state only
	a.mu.RLock()
	var insts []*catalog.Instance
	for _, inst := range a.local {
		if inst.Service == service {
			insts = append(insts, inst.Clone())
		}
	}
	a.mu.RUnlock()
	return &catalog.Result{Service: service, Instances: insts, Stale: true}, nil
}

// SetReader attaches a remote catalog reader.
func (a *Agent) SetReader(r ReadClient) {
	a.mu.Lock()
	a.reader = r
	a.mu.Unlock()
}
