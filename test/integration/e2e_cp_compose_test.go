package integration_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
)

func TestE2E_CPComposePartition(t *testing.T) {
	if os.Getenv("BEACON_RUN_DOCKER_CP") != "1" {
		t.Skip("set BEACON_RUN_DOCKER_CP=1 to run the Docker Compose CP partition test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker is unavailable")
	}
	if err := exec.Command("docker", "compose", "version").Run(); err != nil {
		t.Skip("docker compose is unavailable")
	}

	root := repoRoot(t)
	project := fmt.Sprintf("beacon-cp-%d", time.Now().UnixNano())
	compose := []string{"compose", "-p", project, "-f", filepath.Join(root, "docker-compose.yml")}
	run := func(args ...string) (string, error) {
		cmd := exec.Command("docker", append(compose, args...)...)
		cmd.Env = append(os.Environ(), "BEACON_CONSISTENCY=cp")
		return stringOutput(cmd)
	}
	runDocker := func(args ...string) (string, error) {
		return stringOutput(exec.Command("docker", args...))
	}
	t.Cleanup(func() {
		out, err := run("down", "--volumes", "--remove-orphans")
		if err != nil {
			t.Logf("compose cleanup failed: %v\n%s", err, out)
		}
	})

	if out, err := run("up", "--build", "-d", "server-1", "server-2", "server-3"); err != nil {
		t.Fatalf("start CP compose cluster: %v\n%s", err, out)
	}

	client := &http.Client{Timeout: 3 * time.Second}
	majorityURLs := []string{"http://127.0.0.1:8500", "http://127.0.0.1:8501"}
	minorityURL := "http://127.0.0.1:8505"
	waitFor(t, 90*time.Second, func() bool {
		for _, baseURL := range append(append([]string{}, majorityURLs...), minorityURL) {
			resp, err := client.Get(baseURL + "/health")
			if err != nil {
				return false
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return false
			}
		}
		return true
	}, "CP compose HTTP readiness")

	baseline := catalog.Instance{
		ID: "compose-baseline", Service: "compose", Node: "integration",
		Address: "127.0.0.1", Port: 18080, Health: catalog.HealthPassing,
	}
	registerOnMajority(t, client, majorityURLs, baseline)
	for _, baseURL := range append(append([]string{}, majorityURLs...), minorityURL) {
		waitFor(t, 30*time.Second, func() bool {
			return composeHasInstance(client, baseURL, baseline.ID)
		}, "baseline CP replication to "+baseURL)
	}

	server3 := composeContainer(t, compose, "server-3")
	network := project + "_beacon"
	if out, err := runDocker("network", "disconnect", network, server3); err != nil {
		t.Fatalf("disconnect minority server: %v\n%s", err, out)
	}

	majorityWrite := catalog.Instance{
		ID: "compose-majority", Service: "compose", Node: "integration",
		Address: "127.0.0.1", Port: 18081, Health: catalog.HealthPassing,
	}
	registerOnMajority(t, client, majorityURLs, majorityWrite)

	minorityWrite := catalog.Instance{
		ID: "compose-minority", Service: "compose", Node: "integration",
		Address: "127.0.0.1", Port: 18082, Health: catalog.HealthPassing,
	}
	if out, err := composeRegister(server3, minorityWrite); err == nil || !strings.Contains(out, "500") {
		t.Fatalf("minority write unexpectedly succeeded or did not return HTTP 500: err=%v output=%s", err, out)
	}
	if out, err := composeGet(server3, "/v1/catalog/service/compose?stale=true"); err != nil {
		t.Fatalf("minority stale read: %v\n%s", err, out)
	} else {
		var instances []catalog.Instance
		if err := json.Unmarshal([]byte(out), &instances); err != nil {
			t.Fatalf("decode minority stale read: %v\n%s", err, out)
		}
		if len(instances) == 0 {
			t.Fatal("minority stale read returned no replicated instances")
		}
	}

	if out, err := runDocker("network", "connect", "--alias", "server-3", network, server3); err != nil {
		t.Fatalf("reconnect minority server: %v\n%s", err, out)
	}
	waitFor(t, 60*time.Second, func() bool {
		out, err := composeGet(server3, "/v1/catalog/service/compose?stale=true")
		if err != nil {
			return false
		}
		var instances []catalog.Instance
		if json.Unmarshal([]byte(out), &instances) != nil {
			return false
		}
		for _, instance := range instances {
			if instance.ID == majorityWrite.ID {
				return true
			}
		}
		return false
	}, "minority recovery after CP partition")
}

func registerOnMajority(t *testing.T, client *http.Client, baseURLs []string, instance catalog.Instance) {
	t.Helper()
	body, err := json.Marshal(instance)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 30*time.Second, func() bool {
		for _, baseURL := range baseURLs {
			req, err := http.NewRequest(http.MethodPut, baseURL+"/v1/agent/service/register", bytes.NewReader(body))
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
	}, "majority CP write")
}

func composeHasInstance(client *http.Client, baseURL, id string) bool {
	resp, err := client.Get(baseURL + "/v1/catalog/service/compose?stale=true")
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

func composeContainer(t *testing.T, compose []string, service string) string {
	t.Helper()
	cmd := exec.Command("docker", append(compose, "ps", "-q", service)...)
	out, err := stringOutput(cmd)
	if err != nil {
		t.Fatalf("find %s container: %v\n%s", service, err, out)
	}
	container := strings.TrimSpace(out)
	if container == "" {
		t.Fatalf("%s container is empty", service)
	}
	return container
}

func composeRegister(container string, instance catalog.Instance) (string, error) {
	body, err := json.Marshal(instance)
	if err != nil {
		return "", err
	}
	return composeWget(container, "--header=Content-Type: application/json", "--post-data="+string(body), "-T", "5", "-S", "-O", "/dev/null", "http://127.0.0.1:8500/v1/agent/service/register")
}

func composeGet(container, path string) (string, error) {
	return composeWget(container, "-T", "5", "-q", "-O", "-", "http://127.0.0.1:8500"+path)
}

func composeWget(container string, args ...string) (string, error) {
	cmdArgs := append([]string{"exec", container, "wget"}, args...)
	return stringOutput(exec.Command("docker", cmdArgs...))
}

func stringOutput(cmd *exec.Cmd) (string, error) {
	out, err := cmd.CombinedOutput()
	return string(out), err
}
