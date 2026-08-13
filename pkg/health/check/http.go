package check

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
)

// HTTPCheck GETs a URL. 2xx=passing, 429=warning, else critical. No redirects.
type HTTPCheck struct {
	URL     string
	Timeout time.Duration
	Client  *http.Client
}

// Run executes the HTTP probe.
func (c *HTTPCheck) Run(ctx context.Context) (catalog.HealthStatus, string, error) {
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	client := c.Client
	if client == nil {
		client = &http.Client{
			Timeout: timeout,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.URL, nil)
	if err != nil {
		return catalog.HealthCritical, err.Error(), nil
	}
	resp, err := client.Do(req)
	if err != nil {
		return catalog.HealthCritical, err.Error(), nil
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	out := fmt.Sprintf("HTTP %d: %s", resp.StatusCode, Truncate(string(body), 200))
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return catalog.HealthPassing, out, nil
	case resp.StatusCode == 429:
		return catalog.HealthWarning, out, nil
	default:
		return catalog.HealthCritical, out, nil
	}
}
