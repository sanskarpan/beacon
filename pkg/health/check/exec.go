package check

import (
	"bytes"
	"context"
	"os/exec"
	"syscall"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
)

// ExecCheck runs a command. exit 0=passing, 1=warning, else critical.
//
// On timeout we kill the process GROUP (negative PID), not just the process.
// Killing only the process leaves children orphaned — a health script that
// spawns curl and times out would leak one curl every interval forever.
type ExecCheck struct {
	Script  string
	Args    []string
	Timeout time.Duration
}

// Run executes the script.
func (c *ExecCheck) Run(ctx context.Context) (catalog.HealthStatus, string, error) {
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, c.Script, c.Args...)
	// Put the child in its own process group so we can signal the whole tree.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Start(); err != nil {
		return catalog.HealthCritical, err.Error(), nil
	}

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	select {
	case <-ctx.Done():
		// Negative PID = the whole process group.
		if cmd.Process != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		<-done
		return catalog.HealthCritical, "timeout", nil
	case err := <-done:
		code := exitCode(err)
		snippet := Truncate(out.String(), 200)
		switch code {
		case 0:
			return catalog.HealthPassing, snippet, nil
		case 1:
			return catalog.HealthWarning, snippet, nil
		default:
			return catalog.HealthCritical, snippet, nil
		}
	}
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	if ee, ok := err.(*exec.ExitError); ok {
		if status, ok := ee.Sys().(syscall.WaitStatus); ok {
			return status.ExitStatus()
		}
	}
	return 2
}
