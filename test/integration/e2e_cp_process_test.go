package integration_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
)

type beaconProcess struct {
	cmd    *exec.Cmd
	stderr bytes.Buffer
	url    string
}

func TestE2E_CPThreeProcesses(t *testing.T) {
	root := repoRoot(t)
	bin := filepath.Join(t.TempDir(), "beacon-server")
	build := exec.Command("go", "build", "-o", bin, "./cmd/beacon-server")
	build.Dir = root
	build.Stdout = io.Discard
	build.Stderr = io.Discard
	if err := build.Run(); err != nil {
		t.Fatalf("build beacon-server: %v", err)
	}

	dataDir := t.TempDir()
	const nodeCount = 3
	raftAddrs := make([]string, nodeCount)
	for i := range raftAddrs {
		raftAddrs[i] = freeTCPAddress(t)
	}
	peerParts := make([]string, nodeCount)
	for i, addr := range raftAddrs {
		peerParts[i] = fmt.Sprintf("n%d=%s", i+1, addr)
	}
	peerSpec := strings.Join(peerParts, ",")

	processes := make([]*beaconProcess, 0, nodeCount)
	t.Cleanup(func() {
		for _, process := range processes {
			process.stop()
			if t.Failed() {
				t.Logf("%s stderr:\n%s", process.url, process.stderr.String())
			}
		}
	})
	for i := 0; i < nodeCount; i++ {
		process := startBeaconProcess(t, bin, dataDir, fmt.Sprintf("n%d", i+1), peerSpec, raftAddrs[i])
		processes = append(processes, process)
	}

	for _, process := range processes {
		waitFor(t, 15*time.Second, func() bool {
			resp, err := http.Get(process.url + "/v1/catalog/services")
			if err != nil {
				return false
			}
			defer resp.Body.Close()
			return resp.StatusCode == http.StatusOK
		}, "beacon HTTP readiness")
	}

	instance := catalog.Instance{
		ID: "process-1", Service: "process", Node: "integration",
		Address: "127.0.0.1", Port: 18080, Health: catalog.HealthPassing,
	}
	body, err := json.Marshal(instance)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 2 * time.Second}
	waitFor(t, 15*time.Second, func() bool {
		for _, process := range processes {
			req, err := http.NewRequest(http.MethodPut, process.url+"/v1/agent/service/register", bytes.NewReader(body))
			if err != nil {
				continue
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(req)
			if err != nil {
				continue
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return true
			}
		}
		return false
	}, "CP leader accepts a write")

	for _, process := range processes {
		waitForProcessInstance(t, client, process, instance.ID)
	}

	// A restart must recover from the node's WAL/stable store, not from another
	// in-process node or a test-only memory snapshot.
	processes[2].stop()
	processes[2] = startBeaconProcess(t, bin, dataDir, "n3", peerSpec, raftAddrs[2])
	waitFor(t, 15*time.Second, func() bool {
		resp, err := http.Get(processes[2].url + "/v1/catalog/services")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, "restarted process HTTP readiness")
	waitFor(t, 15*time.Second, func() bool {
		return processHasInstance(client, processes[2].url, instance.ID)
	}, "restarted process recovers the catalog")
}

func waitForProcessInstance(t *testing.T, client *http.Client, process *beaconProcess, id string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if processHasInstance(client, process.url, id) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	resp, err := client.Get(process.url + "/v1/catalog/service/process?stale=true")
	if err != nil {
		t.Fatalf("CP replication to %s timed out: %v", process.url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	t.Fatalf("CP replication to %s timed out: status=%d body=%s", process.url, resp.StatusCode, b)
}

func startBeaconProcess(t *testing.T, bin, dataDir, node, peers, raftAddr string) *beaconProcess {
	t.Helper()
	httpAddr := freeTCPAddress(t)
	process := &beaconProcess{url: "http://" + httpAddr}
	process.cmd = exec.Command(bin, "-grpc", "")
	process.cmd.Dir = repoRoot(t)
	process.cmd.Stdout = io.Discard
	process.cmd.Stderr = &process.stderr
	process.cmd.Env = append(os.Environ(),
		"BEACON_CONSISTENCY=cp",
		"BEACON_NODE="+node,
		"BEACON_BOOTSTRAP_EXPECT=3",
		"BEACON_RAFT="+raftAddr,
		"BEACON_RAFT_PEERS="+peers,
		"BEACON_DATA_DIR="+dataDir,
		"BEACON_HTTP="+httpAddr,
		"BEACON_DNS=127.0.0.1:0",
		"BEACON_GRPC=",
	)
	if err := process.cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", node, err)
	}
	return process
}

func (p *beaconProcess) stop() {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return
	}
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan error, 1)
	go func() { done <- p.cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		_ = p.cmd.Process.Kill()
		<-done
	}
	p.cmd.Process = nil
}

func processHasInstance(client *http.Client, baseURL, id string) bool {
	resp, err := client.Get(baseURL + "/v1/catalog/service/process?stale=true")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	var instances []catalog.Instance
	if err := json.NewDecoder(resp.Body).Decode(&instances); err != nil {
		return false
	}
	for _, instance := range instances {
		if instance.ID == id {
			return true
		}
	}
	return false
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal(what + " timed out")
}

func freeTCPAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}
