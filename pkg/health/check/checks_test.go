package check_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/health/check"
)

func TestTCPCheck(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()
	st, _, err := (&check.TCPCheck{Addr: ln.Addr().String(), Timeout: time.Second}).Run(context.Background())
	if err != nil || st != catalog.HealthPassing {
		t.Fatalf("%s %v", st, err)
	}
	st, _, _ = (&check.TCPCheck{Addr: "127.0.0.1:1", Timeout: 100 * time.Millisecond}).Run(context.Background())
	if st != catalog.HealthCritical {
		t.Fatal(st)
	}
}

func TestTTLCheck(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(0, 0))
	ttl := check.NewTTL(clk, 5*time.Second)
	st, _, _ := ttl.Run(context.Background())
	if st != catalog.HealthCritical {
		t.Fatal("no push yet")
	}
	ttl.Set(catalog.HealthPassing, "ok")
	st, _, _ = ttl.Run(context.Background())
	if st != catalog.HealthPassing {
		t.Fatal(st)
	}
	clk.Advance(6 * time.Second)
	st, _, _ = ttl.Run(context.Background())
	if st != catalog.HealthCritical {
		t.Fatal("should expire")
	}
}

func TestAliasCheck(t *testing.T) {
	a := &check.AliasCheck{
		Service: "db",
		Lookup:  func(string) catalog.HealthStatus { return catalog.HealthWarning },
	}
	st, out, _ := a.Run(context.Background())
	if st != catalog.HealthWarning || out == "" {
		t.Fatal(st, out)
	}
}

func TestExecCheck(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("process groups differ on windows")
	}
	// exit 0
	st, _, _ := (&check.ExecCheck{Script: "true", Timeout: time.Second}).Run(context.Background())
	if st != catalog.HealthPassing {
		t.Fatal(st)
	}
	// exit 1 → warning
	st, _, _ = (&check.ExecCheck{Script: "sh", Args: []string{"-c", "exit 1"}, Timeout: time.Second}).Run(context.Background())
	if st != catalog.HealthWarning {
		t.Fatal(st)
	}
	// exit 2 → critical
	st, _, _ = (&check.ExecCheck{Script: "sh", Args: []string{"-c", "exit 2"}, Timeout: time.Second}).Run(context.Background())
	if st != catalog.HealthCritical {
		t.Fatal(st)
	}
}

func TestExecTimeoutKillsProcessGroup(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip()
	}
	// Script that spawns a long sleep child, then sleeps itself.
	// On timeout, the whole process group must die (no orphan sleep).
	dir := t.TempDir()
	marker := filepath.Join(dir, "child.pid")
	script := filepath.Join(dir, "check.sh")
	content := "#!/bin/sh\n(sleep 60 & echo $! > '" + marker + "')\nsleep 60\n"
	if err := os.WriteFile(script, []byte(content), 0o755); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	st, out, _ := (&check.ExecCheck{Script: script, Timeout: 200 * time.Millisecond}).Run(context.Background())
	if st != catalog.HealthCritical || out != "timeout" {
		t.Fatalf("want timeout critical, got %s %q", st, out)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("timeout took too long")
	}
	// give OS a moment
	time.Sleep(100 * time.Millisecond)
	b, err := os.ReadFile(marker)
	if err != nil {
		// child may not have started — still ok if parent was killed
		t.Log("no child pid marker:", err)
		return
	}
	var pid int
	_, _ = parsePID(string(b), &pid)
	if pid > 0 {
		// check if process still exists
		err := exec.Command("kill", "-0", itoa(pid)).Run()
		if err == nil {
			// still alive — try kill for cleanup and fail
			_ = exec.Command("kill", "-9", itoa(pid)).Run()
			t.Fatalf("child pid %d still alive after process-group kill", pid)
		}
	}
}

func parsePID(s string, pid *int) (int, error) {
	n := 0
	for _, c := range s {
		if c >= '0' && c <= '9' {
			n = n*10 + int(c-'0')
		}
	}
	*pid = n
	return n, nil
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [16]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func TestHTTPNoRedirect(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redir" {
			http.Redirect(w, r, "/ok", http.StatusFound)
			return
		}
		w.WriteHeader(200)
	}))
	defer ts.Close()
	// Following redirects is disabled — 302 is not 2xx → critical
	st, _, _ := (&check.HTTPCheck{URL: ts.URL + "/redir"}).Run(context.Background())
	if st != catalog.HealthCritical {
		t.Fatalf("redirect should be critical (no follow), got %s", st)
	}
}
