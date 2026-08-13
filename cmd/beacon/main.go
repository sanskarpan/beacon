// Command beacon is the operator CLI.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/sim"
)

// strings used for yaml suffix checks
var _ = strings.HasSuffix

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "register":
		cmdRegister(os.Args[2:])
	case "deregister":
		cmdDeregister(os.Args[2:])
	case "services":
		cmdServices(os.Args[2:])
	case "instances", "health":
		cmdInstances(os.Args[2:])
	case "watch":
		cmdWatch(os.Args[2:])
	case "resolve":
		cmdResolve(os.Args[2:])
	case "sim":
		cmdSim(os.Args[2:])
	case "bench":
		cmdBench(os.Args[2:])
	case "prepared-query", "pq":
		fmt.Println("prepared queries: use the query.Store API or HTTP extension; see pkg/query")
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `beacon — service discovery CLI

Usage:
  beacon register   --name SVC --port N [--addr A] [--tag T] [--server URL]
  beacon deregister --id ID [--server URL]
  beacon services   [--server URL]
  beacon instances  NAME [--passing] [--server URL]
  beacon watch      NAME [--server URL]
  beacon resolve    NAME [--server URL]
  beacon sim        [propagate|partition|storm|flap|herd|cascade|all]
  beacon bench      propagate

`)
}

func serverFlag(fs *flag.FlagSet) *string {
	return fs.String("server", envOr("BEACON_HTTP", "http://127.0.0.1:8500"), "beacon-server URL")
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func cmdRegister(args []string) {
	fs := flag.NewFlagSet("register", flag.ExitOnError)
	name := fs.String("name", "", "service name")
	port := fs.Int("port", 0, "port")
	addr := fs.String("addr", "127.0.0.1", "address")
	tag := fs.String("tag", "", "tag")
	id := fs.String("id", "", "instance id")
	srv := serverFlag(fs)
	_ = fs.Parse(args)
	if *name == "" || *port == 0 {
		fs.Usage()
		os.Exit(2)
	}
	inst := catalog.Instance{
		ID: *id, Service: *name, Address: *addr, Port: *port,
		Health: catalog.HealthPassing, Weight: 1,
	}
	if *tag != "" {
		inst.Tags = []string{*tag}
	}
	if inst.ID == "" {
		inst.ID = fmt.Sprintf("%s-%d", *name, *port)
	}
	b, _ := json.Marshal(inst)
	resp, err := http.Post(*srv+"/v1/agent/service/register", "application/json", bytes.NewReader(b))
	// server expects PUT
	req, _ := http.NewRequest(http.MethodPut, *srv+"/v1/agent/service/register", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	fmt.Printf("%s %s\n", resp.Status, body)
}

func cmdDeregister(args []string) {
	fs := flag.NewFlagSet("deregister", flag.ExitOnError)
	id := fs.String("id", "", "instance id")
	srv := serverFlag(fs)
	_ = fs.Parse(args)
	if *id == "" {
		fs.Usage()
		os.Exit(2)
	}
	req, _ := http.NewRequest(http.MethodPut, *srv+"/v1/agent/service/deregister/"+*id, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fatal(err)
	}
	defer resp.Body.Close()
	fmt.Println(resp.Status)
}

func cmdServices(args []string) {
	fs := flag.NewFlagSet("services", flag.ExitOnError)
	srv := serverFlag(fs)
	_ = fs.Parse(args)
	resp, err := http.Get(*srv + "/v1/catalog/services")
	if err != nil {
		fatal(err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(os.Stdout, resp.Body)
	fmt.Println()
}

func cmdInstances(args []string) {
	fs := flag.NewFlagSet("instances", flag.ExitOnError)
	passing := fs.Bool("passing", false, "only passing")
	srv := serverFlag(fs)
	_ = fs.Parse(args)
	if fs.NArg() < 1 {
		fs.Usage()
		os.Exit(2)
	}
	name := fs.Arg(0)
	url := *srv + "/v1/health/service/" + name
	if *passing {
		url += "?passing=true"
	}
	resp, err := http.Get(url)
	if err != nil {
		fatal(err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(os.Stdout, resp.Body)
	fmt.Println()
}

func cmdWatch(args []string) {
	fs := flag.NewFlagSet("watch", flag.ExitOnError)
	srv := serverFlag(fs)
	_ = fs.Parse(args)
	if fs.NArg() < 1 {
		fs.Usage()
		os.Exit(2)
	}
	name := fs.Arg(0)
	var index uint64
	for {
		url := fmt.Sprintf("%s/v1/health/service/%s?index=%d&wait=30s", *srv, name, index)
		resp, err := http.Get(url)
		if err != nil {
			fmt.Println("err:", err)
			time.Sleep(time.Second)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if v := resp.Header.Get("X-Beacon-Index"); v != "" {
			fmt.Sscanf(v, "%d", &index)
		}
		fmt.Printf("index=%d %s\n", index, body)
	}
}

func cmdResolve(args []string) {
	cmdInstances(args)
}

func cmdSim(args []string) {
	which := "all"
	if len(args) > 0 {
		which = args[0]
	}
	r := sim.NewRunner("tmp/sim")
	defer r.Close()
	var results []sim.Result
	switch which {
	case "propagate":
		results = []sim.Result{r.Propagate(10)}
	case "partition":
		results = []sim.Result{r.Partition()}
	case "storm":
		results = []sim.Result{r.Storm(1000)}
	case "flap":
		results = []sim.Result{r.Flap()}
	case "herd":
		results = []sim.Result{r.Herd(100)}
	case "cascade":
		results = []sim.Result{r.Cascade(100)}
	case "rollout":
		results = []sim.Result{r.Rollout(20)}
	case "zone-failure", "zone":
		results = []sim.Result{r.ZoneFailure()}
	default:
		if strings.HasSuffix(which, ".yaml") || strings.HasSuffix(which, ".yml") {
			res, err := r.RunYAMLFile(which)
			if err != nil {
				fatal(err)
			}
			results = []sim.Result{res}
		} else {
			results = r.RunAll()
		}
	}
	b, _ := json.MarshalIndent(results, "", "  ")
	fmt.Println(string(b))
	_ = sim.WriteJSON("tmp/sim/results.json", results)
}

func cmdBench(args []string) {
	if len(args) == 0 || args[0] == "propagate" {
		results := sim.MeasurePropagation(20, 10)
		fmt.Println(sim.MarkdownTable(results))
		b, _ := json.MarshalIndent(results, "", "  ")
		_ = os.WriteFile("tmp/sim/propagation.json", b, 0o644)
		return
	}
	fmt.Println("unknown bench", args[0])
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

// silence unused
var _ = strings.TrimSpace
var _ = context.Background
