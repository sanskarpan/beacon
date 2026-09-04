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
	"time"

	"github.com/sanskar/beacon/pkg/api/dns"
	"github.com/sanskar/beacon/pkg/api/grpcapi"
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
		httpAddr    = flag.String("http", ":8500", "HTTP API listen address")
		dnsAddr     = flag.String("dns", ":8600", "DNS listen address")
		grpcAddr    = flag.String("grpc", ":8502", "gRPC Discovery listen address (empty disables)")
		consistency = flag.String("consistency", "ap", "catalog backend: ap|cp")
		nodeName    = flag.String("node", "server-1", "node name")
		join        = flag.String("join", "", "seed node for gossip")
		bootstrap   = flag.Int("bootstrap-expect", 1, "CP cluster size hint")
	)
	flag.Parse()

	clk := clock.New()
	bus := events.NewBus(clk)
	// Default store (batching enabled); interactive latency stays low because
	// the batch window only coalesces writes within 50ms.
	cs := catalog.NewStore(catalog.WithClock(clk), catalog.WithBus(bus))

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

	// http.Server with timeouts (G114 Slowloris-safe) + graceful shutdown.
	httpServer := &http.Server{
		Addr:              *httpAddr,
		Handler:           httpSrv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	go func() {
		log.Printf("HTTP API on %s", *httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()
	go func() {
		log.Printf("DNS on %s", *dnsAddr)
		if err := dnsSrv.ListenAndServe(); err != nil {
			log.Printf("dns: %v", err)
		}
	}()

	// gRPC Discovery API (Watch/WatchMulti/Register) over real wire stubs.
	var protoSrv *grpcapi.ProtoServer
	if *grpcAddr != "" {
		lis, err := net.Listen("tcp", *grpcAddr)
		if err != nil {
			log.Fatalf("grpc listen %s: %v", *grpcAddr, err)
		}
		protoSrv = grpcapi.NewProtoServer(catalogStore, wr, bus, nil)
		go func() {
			log.Printf("gRPC Discovery on %s", lis.Addr())
			if err := protoSrv.Serve(lis); err != nil {
				log.Printf("grpc serve: %v", err)
			}
		}()
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		log.Printf("http shutdown: %v", err)
	}
	if protoSrv != nil {
		protoSrv.GracefulStop()
	}
	dnsSrv.Shutdown()
}
