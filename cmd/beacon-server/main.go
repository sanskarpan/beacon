// Command beacon-server is the control-plane process.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/sanskar/beacon/pkg/api/dns"
	"github.com/sanskar/beacon/pkg/api/httpapi"
	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
	"github.com/sanskar/beacon/pkg/gossip"
	"github.com/sanskar/beacon/pkg/store"
	gstore "github.com/sanskar/beacon/pkg/store/gossip"
	rstore "github.com/sanskar/beacon/pkg/store/raft"
	"github.com/sanskar/beacon/pkg/watch"
	"github.com/sanskar/beacon/pkg/xds"
)

func main() {
	var (
		httpAddr     = flag.String("http", ":8500", "HTTP API listen address")
		dnsAddr      = flag.String("dns", ":8600", "DNS listen address")
		consistency  = flag.String("consistency", "ap", "catalog backend: ap|cp")
		nodeName     = flag.String("node", "server-1", "node name")
		join         = flag.String("join", "", "seed node for gossip")
		bootstrap    = flag.Int("bootstrap-expect", 1, "CP cluster size hint")
	)
	flag.Parse()

	clk := clock.New()
	bus := events.NewBus(clk)
	cs := catalog.NewStore(catalog.WithClock(clk), catalog.WithBus(bus), catalog.WithBatchWindow(0))
	// disable batcher for interactive server by not using window 0 oddly — use default without batch for simplicity
	cs = catalog.NewStore(catalog.WithClock(clk), catalog.WithBus(bus))

	wr := watch.NewRegistry(cs, watch.WithWatchClock(clk), watch.WithWatchBus(bus))

	var catalogStore store.CatalogStore
	switch *consistency {
	case "cp":
		ids := make([]string, *bootstrap)
		for i := 0; i < *bootstrap; i++ {
			ids[i] = fmt.Sprintf("server-%d", i+1)
		}
		if *bootstrap < 1 {
			ids = []string{*nodeName}
		}
		// ensure our name is first leader if matches
		ids[0] = *nodeName
		cluster := rstore.NewCluster(ids, clk, bus)
		catalogStore = rstore.NewStore(cluster.Node(*nodeName))
		log.Printf("CP mode: raft cluster %v leader=%s", ids, *nodeName)
	default:
		gcluster := gossip.NewCluster(clk)
		mem := gossip.NewMemory(gcluster, *nodeName, "127.0.0.1", 7946)
		if *join != "" {
			if _, err := mem.Join([]string{*join}); err != nil {
				log.Printf("join warning: %v", err)
			}
		}
		gs := gstore.New(gstore.Config{Local: cs, Membership: mem, Bus: bus, Watch: wr})
		catalogStore = gs
		log.Printf("AP mode: gossip membership as %s", *nodeName)
	}

	_ = xds.New(catalogStore, bus) // available for mesh clients

	httpSrv := httpapi.New(httpapi.Config{
		Store: catalogStore,
		Bus:   bus,
		Clock: clk,
		Watch: wr,
	})

	dnsSrv := dns.New(dns.Config{
		Store:       catalogStore,
		Addr:        *dnsAddr,
		PassingOnly: true,
	})

	go func() {
		log.Printf("HTTP API on %s", *httpAddr)
		if err := http.ListenAndServe(*httpAddr, httpSrv.Handler()); err != nil {
			log.Fatal(err)
		}
	}()
	go func() {
		log.Printf("DNS on %s", *dnsAddr)
		if err := dnsSrv.ListenAndServe(); err != nil {
			log.Printf("dns: %v", err)
		}
	}()

	// optional gRPC listen for future proto wiring
	go func() {
		lis, err := net.Listen("tcp", ":8502")
		if err != nil {
			log.Printf("grpc listen: %v", err)
			return
		}
		log.Printf("gRPC placeholder on %s", lis.Addr())
		<-context.Background().Done()
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("shutting down")
	dnsSrv.Shutdown()
}
