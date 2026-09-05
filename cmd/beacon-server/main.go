// Command beacon-server is the control-plane process.
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/sanskar/beacon/pkg/api/dns"
	"github.com/sanskar/beacon/pkg/api/grpcapi"
	"github.com/sanskar/beacon/pkg/api/httpapi"
	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
	"github.com/sanskar/beacon/pkg/gossip"
	"github.com/sanskar/beacon/pkg/mesh"
	"github.com/sanskar/beacon/pkg/sdk"
	"github.com/sanskar/beacon/pkg/store"
	gstore "github.com/sanskar/beacon/pkg/store/gossip"
	consensus "github.com/sanskar/beacon/pkg/store/raft/consensus"
	"github.com/sanskar/beacon/pkg/telemetry"
	"github.com/sanskar/beacon/pkg/watch"
	"github.com/sanskar/beacon/pkg/xds"
)

var version = "dev"

func main() {
	var (
		httpAddr     = flag.String("http", envOr("BEACON_HTTP", ":8500"), "HTTP API listen address")
		dnsAddr      = flag.String("dns", envOr("BEACON_DNS", ":8600"), "DNS listen address")
		grpcAddr     = flag.String("grpc", envOr("BEACON_GRPC", ":8502"), "gRPC Discovery listen address (empty disables)")
		consistency  = flag.String("consistency", envOr("BEACON_CONSISTENCY", "ap"), "catalog backend: ap|cp")
		nodeName     = flag.String("node", envOr("BEACON_NODE", "server-1"), "node name")
		join         = flag.String("join", envOr("BEACON_JOIN", ""), "seed node for gossip")
		gossipAddr   = flag.String("gossip", envOr("BEACON_GOSSIP", ":7946"), "gossip UDP listen address")
		advertise    = flag.String("advertise", envOr("BEACON_ADVERTISE_ADDR", ""), "gossip advertise address")
		bootstrap    = flag.Int("bootstrap-expect", envInt("BEACON_BOOTSTRAP_EXPECT", 1), "CP cluster size hint")
		raftAddr     = flag.String("raft", envOr("BEACON_RAFT", ":8300"), "CP Raft TCP listen address")
		raftPeers    = flag.String("raft-peers", envOr("BEACON_RAFT_PEERS", ""), "CP peers as id=host:port,id=host:port")
		dataDir      = flag.String("data-dir", envOr("BEACON_DATA_DIR", "./data"), "CP data directory")
		otelEndpoint = flag.String("otel-endpoint", envOr("BEACON_OTEL_ENDPOINT", ""), "OTLP gRPC tracing endpoint")
		authToken    = flag.String("auth-token", envOr("BEACON_AUTH_TOKEN", ""), "Bearer token for control-plane HTTP and metrics")
		tlsCert      = flag.String("tls-cert", envOr("BEACON_TLS_CERT", ""), "TLS certificate file")
		tlsKey       = flag.String("tls-key", envOr("BEACON_TLS_KEY", ""), "TLS private key file")
		tlsClientCA  = flag.String("tls-client-ca", envOr("BEACON_TLS_CLIENT_CA", ""), "optional client CA file for mTLS")
		enableLab    = flag.Bool("enable-lab", envBool("BEACON_ENABLE_LAB", false), "enable synthetic consistency lab endpoints")
		showVersion  = flag.Bool("version", false, "print version and exit")
	)
	flag.Parse()
	if *otelEndpoint != "" {
		if err := os.Setenv("BEACON_OTEL_ENDPOINT", *otelEndpoint); err != nil {
			log.Fatalf("set OTLP endpoint: %v", err)
		}
	}
	if *showVersion {
		fmt.Println(version)
		return
	}
	if *consistency != "ap" && *consistency != "cp" {
		log.Fatalf("invalid consistency %q: want ap or cp", *consistency)
	}
	if *bootstrap < 1 {
		log.Fatal("bootstrap-expect must be positive")
	}

	clk := clock.New()
	bus := events.NewBus(clk)
	telemetry.Init(*nodeName, version)
	// Default store (batching enabled); interactive latency stays low because
	// the batch window only coalesces writes within 50ms.
	cs := catalog.NewStore(catalog.WithClock(clk), catalog.WithBus(bus))

	wr := watch.NewRegistry(cs, watch.WithWatchClock(clk), watch.WithWatchBus(bus))

	var catalogStore store.CatalogStore
	var membership gossip.Membership
	var udpMembership *gossip.UDP
	var cpNode *consensus.Node
	switch *consistency {
	case "cp":
		peers, err := parseRaftPeers(*raftPeers)
		if err != nil {
			log.Fatalf("invalid raft peers: %v", err)
		}
		if len(peers) == 0 {
			if *bootstrap != 1 {
				log.Fatal("BEACON_RAFT_PEERS is required when bootstrap-expect is greater than one")
			}
			peers = []consensus.Peer{{ID: *nodeName, Address: *raftAddr}}
		} else if *bootstrap > 1 && len(peers) != *bootstrap {
			log.Fatalf("raft peers has %d members, bootstrap-expect is %d", len(peers), *bootstrap)
		}
		var listenAddress string
		for _, peer := range peers {
			if peer.ID == *nodeName {
				listenAddress = peer.Address
				break
			}
		}
		if listenAddress == "" {
			log.Fatalf("node %q is not in raft peers", *nodeName)
		}
		cpNode, err = consensus.NewProcessNode(consensus.ProcessConfig{
			ID:            *nodeName,
			ListenAddress: listenAddress,
			Peers:         peers,
			DataDir:       *dataDir,
		}, clk, bus)
		if err != nil {
			log.Fatalf("CP Raft startup: %v", err)
		}
		catalogStore = consensus.NewStore(cpNode)
		ids := make([]string, 0, len(peers))
		for _, peer := range peers {
			ids = append(ids, peer.ID)
		}
		log.Printf("CP mode: network Raft cluster %v, node=%s, raft=%s", ids, *nodeName, listenAddress)
	default:
		bindHost, portText, err := net.SplitHostPort(*gossipAddr)
		if err != nil {
			log.Fatalf("invalid gossip address %q: %v", *gossipAddr, err)
		}
		port, err := strconv.Atoi(portText)
		if err != nil || port < 0 || port > 65535 {
			log.Fatalf("invalid gossip port %q", portText)
		}
		udpMembership, err = gossip.NewUDP(gossip.UDPConfig{
			Name: *nodeName, BindAddr: bindHost, AdvertiseAddr: *advertise, Port: port, Clock: clk,
		})
		if err != nil {
			log.Fatalf("gossip listen: %v", err)
		}
		membership = udpMembership
		if *join != "" {
			if _, err := membership.Join([]string{*join}); err != nil {
				log.Printf("join warning: %v", err)
			}
		}
		gs := gstore.New(gstore.Config{Local: cs, Membership: membership, Bus: bus, Watch: wr})
		catalogStore = gs
		log.Printf("AP mode: UDP gossip membership as %s on %s", *nodeName, udpMembership.LocalName())
	}

	xdsSrv := xds.New(catalogStore, bus)
	var ready atomic.Bool
	ready.Store(true)
	var tlsConfig *tls.Config
	if *tlsCert != "" || *tlsKey != "" {
		var err error
		tlsConfig, err = mesh.ServerTLSFromFiles(*tlsCert, *tlsKey, *tlsClientCA)
		if err != nil {
			log.Fatalf("TLS configuration: %v", err)
		}
	}

	httpSrv := httpapi.New(httpapi.Config{
		Store:      catalogStore,
		Bus:        bus,
		Clock:      clk,
		Watch:      wr,
		Membership: membership,
		XDS:        xdsSrv,
		AuthToken:  *authToken,
		ReadyCheck: ready.Load,
		EnableLab:  *enableLab,
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
	if tlsConfig != nil {
		httpServer.TLSConfig = tlsConfig.Clone()
	}
	httpLis, err := net.Listen("tcp", *httpAddr)
	if err != nil {
		log.Fatalf("http listen %s: %v", *httpAddr, err)
	}
	go func() {
		log.Printf("HTTP API on %s", *httpAddr)
		var err error
		if tlsConfig == nil {
			err = httpServer.Serve(httpLis)
		} else {
			err = httpServer.ServeTLS(httpLis, "", "")
		}
		if err != nil && err != http.ErrServerClosed {
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
		protoSrv = grpcapi.NewProtoServerWithInterceptors(catalogStore, wr, bus,
			sdk.DefaultServerUnaryInterceptors(), sdk.DefaultServerStreamInterceptors(), tlsConfig, *authToken)
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
	ready.Store(false)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		log.Printf("http shutdown: %v", err)
	}
	if protoSrv != nil {
		protoSrv.GracefulStop()
	}
	dnsSrv.Shutdown()
	if membership != nil {
		_ = membership.Leave()
	}
	if udpMembership != nil {
		udpMembership.Stop()
	}
	if cpNode != nil {
		if err := cpNode.Shutdown(); err != nil {
			log.Printf("raft shutdown: %v", err)
		}
	}
}

func parseRaftPeers(raw string) ([]consensus.Peer, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	peers := make([]consensus.Peer, 0, len(parts))
	seen := make(map[string]bool, len(parts))
	for _, part := range parts {
		id, address, ok := strings.Cut(strings.TrimSpace(part), "=")
		id = strings.TrimSpace(id)
		address = strings.TrimSpace(address)
		if !ok || id == "" || address == "" {
			return nil, fmt.Errorf("want id=host:port, got %q", part)
		}
		if seen[id] {
			return nil, fmt.Errorf("duplicate peer id %q", id)
		}
		seen[id] = true
		peers = append(peers, consensus.Peer{ID: id, Address: address})
	}
	return peers, nil
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	value := os.Getenv(key)
	parsed, err := strconv.Atoi(value)
	if value == "" || err != nil {
		return fallback
	}
	return parsed
}

func envBool(key string, fallback bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}
