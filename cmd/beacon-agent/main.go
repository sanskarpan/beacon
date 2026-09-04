// Command beacon-agent is the per-node agent: local state, health checks, anti-entropy.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sanskar/beacon/pkg/agent"
	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
)

// remoteClient talks to beacon-server over HTTP.
type remoteClient struct {
	base string
	node string
	hc   *http.Client
}

func (c *remoteClient) Register(ctx context.Context, inst *catalog.Instance) (uint64, error) {
	b, _ := json.Marshal(inst)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.base+"/v1/agent/service/register", bytes.NewReader(b))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.hc.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return 0, fmt.Errorf("register status %d", resp.StatusCode)
	}
	return 0, nil
}

func (c *remoteClient) Deregister(ctx context.Context, id string) (uint64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.base+"/v1/agent/service/deregister/"+id, nil)
	if err != nil {
		return 0, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	return 0, nil
}

func (c *remoteClient) UpdateHealth(ctx context.Context, id string, h catalog.HealthStatus) (uint64, error) {
	path := "/v1/agent/check/pass/"
	switch h {
	case catalog.HealthWarning:
		path = "/v1/agent/check/warn/"
	case catalog.HealthCritical:
		path = "/v1/agent/check/fail/"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.base+path+id, nil)
	if err != nil {
		return 0, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	return 0, nil
}

func (c *remoteClient) NodeServices(ctx context.Context, node string) (map[string]*catalog.Instance, error) {
	// List via health/catalog is not node-filtered in simple API; return empty to allow agent push.
	_ = ctx
	_ = node
	return map[string]*catalog.Instance{}, nil
}

func main() {
	var (
		node     = flag.String("node", "agent-1", "node name")
		server   = flag.String("server", "http://127.0.0.1:8500", "beacon-server base URL")
		dataDir  = flag.String("data-dir", "./data/agent", "local state directory")
		httpAddr = flag.String("http", ":8501", "agent local HTTP API")
	)
	flag.Parse()

	clk := clock.New()
	bus := events.NewBus(clk)
	client := &remoteClient{
		base: *server,
		node: *node,
		hc:   &http.Client{Timeout: 10 * time.Second},
	}
	// local store for health runner updates
	localStore := catalog.NewStore(catalog.WithClock(clk), catalog.WithBus(bus))

	a := agent.New(agent.Config{
		NodeName:    *node,
		Client:      client,
		Store:       localStore,
		Bus:         bus,
		Clock:       clk,
		DataDir:     *dataDir,
		ClusterSize: func() int { return 3 },
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a.StartAntiEntropy(ctx)

	// local agent HTTP for register
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/agent/service/register", func(w http.ResponseWriter, r *http.Request) {
		var inst catalog.Instance
		if err := json.NewDecoder(r.Body).Decode(&inst); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if err := a.Register(r.Context(), &inst); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.WriteHeader(200)
	})
	mux.HandleFunc("/v1/agent/service/deregister/", func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Path[len("/v1/agent/service/deregister/"):]
		_ = a.Deregister(r.Context(), id)
		w.WriteHeader(200)
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})

	agentSrv := &http.Server{
		Addr:              *httpAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	go func() {
		log.Printf("agent %s HTTP on %s (server=%s)", *node, *httpAddr, *server)
		if err := agentSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	cancel()
	a.Stop()
	log.Println("agent stopped")
}
