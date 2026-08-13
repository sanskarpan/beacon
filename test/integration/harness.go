// Package integration runs end-to-end tests against a real HTTP + catalog +
// gossip + watch stack (no mocks of the control plane).
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	"github.com/sanskar/beacon/pkg/agent"
	"github.com/sanskar/beacon/pkg/api/dns"
	"github.com/sanskar/beacon/pkg/api/httpapi"
	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
	"github.com/sanskar/beacon/pkg/gossip"
	"github.com/sanskar/beacon/pkg/sdk"
	"github.com/sanskar/beacon/pkg/store"
	gstore "github.com/sanskar/beacon/pkg/store/gossip"
	rstore "github.com/sanskar/beacon/pkg/store/raft"
	"github.com/sanskar/beacon/pkg/watch"
	"github.com/sanskar/beacon/pkg/xds"
)

// Stack is a fully wired single-node (or multi-node) beacon deployment for tests.
type Stack struct {
	Name       string
	Clock      clock.Clock
	Bus        *events.Bus
	Catalog    *catalog.Store
	Watch      *watch.Registry
	Store      store.CatalogStore
	Membership gossip.Membership
	HTTP       *httptest.Server
	DNS        *dns.Server
	Agent      *agent.Agent
	SDK        *sdk.Client
	XDS        *xds.Server
	Leases     *catalog.LeaseManager
	// CP-only
	RaftCluster *rstore.Cluster
	// AP multi-node
	GossipCluster *gossip.Cluster
}

// Options for NewStack.
type Options struct {
	Name        string
	Mode        string // "ap" | "cp"
	Clock       clock.Clock
	DataDir     string
	WithAgent   bool
	ClusterSize int // CP bootstrap size
	// Shared AP fabric — if set, join this cluster instead of creating one.
	GossipCluster *gossip.Cluster
	JoinSeeds     []string
}

// NewStack boots an in-process beacon node with real HTTP.
func NewStack(opts Options) *Stack {
	if opts.Name == "" {
		opts.Name = "server-1"
	}
	if opts.Mode == "" {
		opts.Mode = "ap"
	}
	clk := opts.Clock
	if clk == nil {
		clk = clock.New()
	}
	bus := events.NewBus(clk)
	cs := catalog.NewStore(catalog.WithClock(clk), catalog.WithBus(bus))
	wr := watch.NewRegistry(cs, watch.WithWatchClock(clk), watch.WithWatchBus(bus))
	leases := catalog.NewLeaseManager(cs, clk, catalog.WithLeaseBus(bus))
	leases.Start(context.Background())

	st := &Stack{
		Name:    opts.Name,
		Clock:   clk,
		Bus:     bus,
		Catalog: cs,
		Watch:   wr,
		Leases:  leases,
	}

	switch opts.Mode {
	case "cp":
		n := opts.ClusterSize
		if n < 1 {
			n = 3
		}
		ids := make([]string, n)
		for i := 0; i < n; i++ {
			ids[i] = fmt.Sprintf("server-%d", i+1)
		}
		ids[0] = opts.Name
		cluster := rstore.NewCluster(ids, clk, bus)
		st.RaftCluster = cluster
		st.Store = rstore.NewStore(cluster.Node(opts.Name))
	default:
		gc := opts.GossipCluster
		if gc == nil {
			gc = gossip.NewCluster(clk)
		}
		st.GossipCluster = gc
		mem := gossip.NewMemory(gc, opts.Name, "127.0.0.1", 7946)
		if len(opts.JoinSeeds) > 0 {
			_, _ = mem.Join(opts.JoinSeeds)
		}
		st.Membership = mem
		gs := gstore.New(gstore.Config{
			Local: cs, Membership: mem, Bus: bus, Watch: wr,
		})
		st.Store = gs
	}

	st.XDS = xds.New(st.Store, bus)

	var ag *agent.Agent
	if opts.WithAgent {
		client := &agent.LocalClient{Store: cs, Node: opts.Name}
		// Prefer writing through CatalogStore when possible
		ag = agent.New(agent.Config{
			NodeName: opts.Name,
			Client:   &storeAgentClient{S: st.Store, Node: opts.Name},
			Store:    cs,
			Bus:      bus,
			Clock:    clk,
			DataDir:  opts.DataDir,
			ClusterSize: func() int {
				if st.Membership != nil {
					return st.Membership.Size()
				}
				return 1
			},
		})
		ag.StartAntiEntropy(context.Background())
		st.Agent = ag
		_ = client
	}

	httpCfg := httpapi.Config{
		Store:      st.Store,
		Agent:      ag,
		Bus:        bus,
		Clock:      clk,
		Watch:      wr,
		RPS:        100000,
		Burst:      100000,
		Membership: st.Membership,
		OnRegister: func(inst *catalog.Instance, idx uint64) {
			if wr != nil {
				wr.Notify(inst.Service, watch.Event{
					Kind:      "add",
					Service:   inst.Service,
					Instances: []*catalog.Instance{inst},
					Index:     idx,
					TraceID:   inst.TraceID,
				})
			}
		},
		OnDeregister: func(id, service string, idx uint64) {
			if wr != nil && service != "" {
				wr.Notify(service, watch.Event{
					Kind:    "remove",
					Service: service,
					Index:   idx,
				})
			}
		},
	}
	srv := httpapi.New(httpCfg)
	st.HTTP = httptest.NewServer(srv.Handler())

	st.DNS = dns.New(dns.Config{
		Store:       st.Store,
		PassingOnly: true,
		// no listen — ServeDNS for tests
	})

	st.SDK = sdk.New(sdk.Config{
		Registry: sdk.StoreAdapter{S: st.Store},
		Clock:    clk,
		Bus:      bus,
	})
	return st
}

// Close shuts down the stack.
func (s *Stack) Close() {
	if s.HTTP != nil {
		s.HTTP.Close()
	}
	if s.Agent != nil {
		s.Agent.Stop()
	}
	if s.DNS != nil {
		s.DNS.Shutdown()
	}
}

// URL returns the HTTP base URL.
func (s *Stack) URL() string { return s.HTTP.URL }

// PUTJSON issues a PUT with a JSON body.
func (s *Stack) PUTJSON(path string, body any) (*http.Response, error) {
	b, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPut, s.URL()+path, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return http.DefaultClient.Do(req)
}

// GET issues a GET.
func (s *Stack) GET(path string) (*http.Response, error) {
	return http.Get(s.URL() + path)
}

// RegisterHTTP registers via the HTTP API.
func (s *Stack) RegisterHTTP(inst map[string]any) error {
	resp, err := s.PUTJSON("/v1/agent/service/register", inst)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("register %d: %s", resp.StatusCode, b)
	}
	return nil
}

// storeAgentClient routes agent ops through CatalogStore.
type storeAgentClient struct {
	S    store.CatalogStore
	Node string
}

func (c *storeAgentClient) Register(ctx context.Context, inst *catalog.Instance) (uint64, error) {
	return c.S.Register(ctx, inst)
}
func (c *storeAgentClient) Deregister(ctx context.Context, id string) (uint64, error) {
	return c.S.Deregister(ctx, id)
}
func (c *storeAgentClient) UpdateHealth(ctx context.Context, id string, h catalog.HealthStatus) (uint64, error) {
	return c.S.UpdateHealth(ctx, id, h)
}
func (c *storeAgentClient) NodeServices(ctx context.Context, node string) (map[string]*catalog.Instance, error) {
	_ = ctx
	list := c.S.InstancesOnNode(node)
	out := make(map[string]*catalog.Instance, len(list))
	for _, inst := range list {
		out[inst.ID] = inst
	}
	return out, nil
}

// FreePort finds an available TCP port.
func FreePort() int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// CollectEvents drains bus into a slice until timeout.
func CollectEvents(bus *events.Bus, d time.Duration) []events.Event {
	ch, unsub := bus.Subscribe(1024)
	defer unsub()
	var out []events.Event
	deadline := time.After(d)
	for {
		select {
		case <-deadline:
			return out
		case ev, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, ev)
		}
	}
}

// WaitFor waits until cond is true or timeout.
func WaitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

// MultiNodeAP builds N gossip-connected AP stacks sharing one fabric.
func MultiNodeAP(n int, clk clock.Clock) []*Stack {
	if clk == nil {
		clk = clock.New()
	}
	gc := gossip.NewCluster(clk)
	stacks := make([]*Stack, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("n%d", i)
		var seeds []string
		if i > 0 {
			seeds = []string{"n0"}
		}
		stacks[i] = NewStack(Options{
			Name:          name,
			Mode:          "ap",
			Clock:         clk,
			GossipCluster: gc,
			JoinSeeds:     seeds,
		})
	}
	return stacks
}

// CloseAll closes stacks.
func CloseAll(stacks []*Stack) {
	var wg sync.WaitGroup
	for _, s := range stacks {
		wg.Add(1)
		go func(s *Stack) {
			defer wg.Done()
			s.Close()
		}(s)
	}
	wg.Wait()
}
