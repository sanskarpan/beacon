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
		cmdPreparedQuery(os.Args[2:])
	case "intentions":
		cmdIntentions(os.Args[2:])
	case "xds":
		cmdXDS(os.Args[2:])
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
  beacon bench      [propagate|contrast]
  beacon prepared-query list|create|delete|execute [--server URL]
  beacon intentions list|create|delete [--server URL]
  beacon xds status [--node NODE] [--server URL]

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
	// Legacy POST probe (server expects PUT); drain and close to avoid leaking the connection.
	if postResp, postErr := http.Post(*srv+"/v1/agent/service/register", "application/json", bytes.NewReader(b)); postErr == nil {
		_, _ = io.Copy(io.Discard, postResp.Body)
		_ = postResp.Body.Close()
	}
	req, _ := http.NewRequest(http.MethodPut, *srv+"/v1/agent/service/register", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
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
	defer func() { _ = resp.Body.Close() }()
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
	defer func() { _ = resp.Body.Close() }()
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
	defer func() { _ = resp.Body.Close() }()
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
		_ = resp.Body.Close()
		if v := resp.Header.Get("X-Beacon-Index"); v != "" {
			_, _ = fmt.Sscanf(v, "%d", &index)
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
		_ = os.WriteFile("tmp/sim/propagation.json", b, 0o600)
		return
	}
	if args[0] == "contrast" {
		c := sim.MeasureGossipContrast(10, 5, 30*time.Second)
		fmt.Println(sim.ContrastMarkdown(c))
		if err := sim.WriteContrastJSON("tmp/sim", c); err != nil {
			fatal(err)
		}
		return
	}
	fmt.Println("unknown bench", args[0])
}

// doJSON performs one HTTP call and streams the response body to stdout.
func doJSON(method, url string, body any) {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			fatal(err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		fatal(fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(b))))
	}
	_, _ = io.Copy(os.Stdout, resp.Body)
	fmt.Println()
}

func cmdPreparedQuery(args []string) {
	fs := flag.NewFlagSet("prepared-query", flag.ExitOnError)
	srv := serverFlag(fs)
	id := fs.String("id", "", "query id (defaults to --name)")
	name := fs.String("name", "", "query name")
	service := fs.String("service", "", "service to resolve")
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "usage: beacon prepared-query list|create|delete|execute [--id ID] [--name NAME] [--service SVC]")
		os.Exit(2)
	}
	command := args[0]
	_ = fs.Parse(args[1:])
	switch command {
	case "list":
		doJSON(http.MethodGet, *srv+"/v1/query", nil)
	case "create":
		if *service == "" {
			fmt.Fprintln(os.Stderr, "create requires --service")
			os.Exit(2)
		}
		qid := *id
		if qid == "" {
			qid = *name
		}
		doJSON(http.MethodPut, *srv+"/v1/query", map[string]any{
			"id": qid, "name": *name, "service": *service,
		})
	case "delete":
		if *id == "" && *name == "" {
			fmt.Fprintln(os.Stderr, "delete requires --id or --name")
			os.Exit(2)
		}
		target := *id
		if target == "" {
			target = *name
		}
		doJSON(http.MethodDelete, *srv+"/v1/query/"+target, nil)
	case "execute":
		if *id == "" && *name == "" {
			fmt.Fprintln(os.Stderr, "execute requires --id or --name")
			os.Exit(2)
		}
		target := *id
		if target == "" {
			target = *name
		}
		doJSON(http.MethodGet, *srv+"/v1/query/"+target+"/execute", nil)
	default:
		fmt.Fprintln(os.Stderr, "want list|create|delete|execute")
		os.Exit(2)
	}
}

func cmdIntentions(args []string) {
	fs := flag.NewFlagSet("intentions", flag.ExitOnError)
	srv := serverFlag(fs)
	src := fs.String("source", "", "source service (or *)")
	dst := fs.String("destination", "", "destination service")
	action := fs.String("action", "allow", "allow|deny")
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "usage: beacon intentions list|create|delete [--source S] [--destination D] [--action allow|deny]")
		os.Exit(2)
	}
	command := args[0]
	_ = fs.Parse(args[1:])
	switch command {
	case "list":
		doJSON(http.MethodGet, *srv+"/v1/connect/intentions", nil)
	case "create":
		if *src == "" || *dst == "" {
			fmt.Fprintln(os.Stderr, "create requires --source and --destination")
			os.Exit(2)
		}
		doJSON(http.MethodPut, *srv+"/v1/connect/intentions", map[string]any{
			"Source": *src, "Destination": *dst, "Action": *action,
		})
	case "delete":
		if *src == "" || *dst == "" {
			fmt.Fprintln(os.Stderr, "delete requires --source and --destination")
			os.Exit(2)
		}
		doJSON(http.MethodDelete, *srv+"/v1/connect/intentions/"+*src+"/"+*dst, nil)
	default:
		fmt.Fprintln(os.Stderr, "want list|create|delete")
		os.Exit(2)
	}
}

func cmdXDS(args []string) {
	fs := flag.NewFlagSet("xds", flag.ExitOnError)
	srv := serverFlag(fs)
	node := fs.String("node", "", "proxy node id (empty = all)")
	if len(args) < 1 || args[0] != "status" {
		fmt.Fprintln(os.Stderr, "usage: beacon xds status [--node NODE]")
		os.Exit(2)
	}
	_ = fs.Parse(args[1:])
	url := *srv + "/v1/xds/status"
	if *node != "" {
		url += "?node=" + *node
	}
	doJSON(http.MethodGet, url, nil)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

// silence unused
var _ = strings.TrimSpace
var _ = context.Background
