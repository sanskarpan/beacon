// Command demo runs a 3-service mesh: web → api → db with beacon registration.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
	"github.com/sanskar/beacon/pkg/sdk"
	"github.com/sanskar/beacon/pkg/store"
)

func main() {
	clk := clock.New()
	bus := events.NewBus(clk)
	cs := catalog.NewStore(catalog.WithClock(clk), catalog.WithBus(bus))
	client := sdk.New(sdk.Config{
		Registry: sdk.StoreAdapter{S: store.NewMemory(cs, "ap")},
		Clock:    clk,
		Bus:      bus,
	})
	ctx := context.Background()

	// Register three services
	services := []struct {
		id, name string
		port     int
	}{
		{"web-1", "web", 8080},
		{"api-1", "api", 8081},
		{"db-1", "db", 5432},
	}
	for _, s := range services {
		_, err := client.Register(ctx, &catalog.Instance{
			ID: s.id, Service: s.name, Node: "demo", Address: "127.0.0.1", Port: s.port,
			Health: catalog.HealthPassing, Weight: 1,
			Tags: []string{"demo", "v1"},
			Locality: catalog.Locality{Region: "local", Zone: "z1"},
			Lease:    &catalog.Lease{TTL: 30 * time.Second},
		})
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("registered %s on :%d", s.name, s.port)
	}

	// Simple HTTP handlers that resolve downstream via SDK
	mux := http.NewServeMux()
	mux.HandleFunc("/web", func(w http.ResponseWriter, r *http.Request) {
		// web → api
		insts, err := client.Resolve(r.Context(), "api", catalog.QueryOptions{Passing: true})
		if err != nil || len(insts) == 0 {
			http.Error(w, "api unavailable", 503)
			return
		}
		fmt.Fprintf(w, "web → api@%s → ok\n", insts[0].Addr())
	})
	mux.HandleFunc("/api", func(w http.ResponseWriter, r *http.Request) {
		insts, err := client.Resolve(r.Context(), "db", catalog.QueryOptions{Passing: true})
		if err != nil || len(insts) == 0 {
			http.Error(w, "db unavailable", 503)
			return
		}
		fmt.Fprintf(w, "api → db@%s → ok\n", insts[0].Addr())
	})
	mux.HandleFunc("/services", func(w http.ResponseWriter, r *http.Request) {
		for _, name := range []string{"web", "api", "db"} {
			insts, _ := client.Resolve(r.Context(), name, catalog.QueryOptions{})
			fmt.Fprintf(w, "%s: %d instances\n", name, len(insts))
		}
	})

	srv := &http.Server{Addr: ":8090", Handler: mux}
	go func() {
		log.Printf("demo listening on :8090  (GET /web /api /services)")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	client.GracefulShutdown(ctx)
	_ = srv.Shutdown(context.Background())
	log.Println("demo stopped")
}
