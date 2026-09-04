package xds

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// BootstrapConfig is a minimal Envoy bootstrap for connecting to beacon ADS.
type BootstrapConfig struct {
	NodeID     string `json:"node_id"`
	Cluster    string `json:"cluster"`
	ADSAddress string `json:"ads_address"` // host:port of beacon xDS
	ADSPort    int    `json:"ads_port"`
	AdminPort  int    `json:"admin_port"`
}

// GenerateBootstrap returns Envoy bootstrap JSON (static_resources + dynamic ADS).
func GenerateBootstrap(cfg BootstrapConfig) ([]byte, error) {
	if cfg.NodeID == "" {
		cfg.NodeID = "envoy-1"
	}
	if cfg.Cluster == "" {
		cfg.Cluster = "beacon"
	}
	if cfg.ADSAddress == "" {
		cfg.ADSAddress = "127.0.0.1"
	}
	if cfg.ADSPort == 0 {
		cfg.ADSPort = 18000
	}
	if cfg.AdminPort == 0 {
		cfg.AdminPort = 9901
	}
	// Hand-built structure matching Envoy bootstrap v3 subset.
	doc := map[string]any{
		"node": map[string]any{
			"id":      cfg.NodeID,
			"cluster": cfg.Cluster,
		},
		"admin": map[string]any{
			"address": map[string]any{
				"socket_address": map[string]any{
					"address":    "127.0.0.1",
					"port_value": cfg.AdminPort,
				},
			},
		},
		"dynamic_resources": map[string]any{
			"lds_config": map[string]any{"ads": map[string]any{}, "resource_api_version": "V3"},
			"cds_config": map[string]any{"ads": map[string]any{}, "resource_api_version": "V3"},
			"ads_config": map[string]any{
				"api_type":              "GRPC",
				"transport_api_version": "V3",
				"grpc_services": []any{
					map[string]any{
						"envoy_grpc": map[string]any{"cluster_name": "beacon_ads"},
					},
				},
				"set_node_on_first_message_only": true,
			},
		},
		"static_resources": map[string]any{
			"clusters": []any{
				map[string]any{
					"name":                   "beacon_ads",
					"type":                   "STATIC",
					"connect_timeout":        "1s",
					"http2_protocol_options": map[string]any{},
					"load_assignment": map[string]any{
						"cluster_name": "beacon_ads",
						"endpoints": []any{
							map[string]any{
								"lb_endpoints": []any{
									map[string]any{
										"endpoint": map[string]any{
											"address": map[string]any{
												"socket_address": map[string]any{
													"address":    cfg.ADSAddress,
													"port_value": cfg.ADSPort,
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	return json.MarshalIndent(doc, "", "  ")
}

// Debouncer coalesces rapid catalog changes into one xDS push.
type Debouncer struct {
	window time.Duration
	timer  *time.Timer
	fn     func()
	mu     sync.Mutex
}

// NewDebouncer creates a push debouncer (default 50ms).
func NewDebouncer(window time.Duration, fn func()) *Debouncer {
	if window <= 0 {
		window = 50 * time.Millisecond
	}
	return &Debouncer{window: window, fn: fn}
}

// Touch schedules a push after window; repeated Touch resets the timer.
func (d *Debouncer) Touch() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.timer != nil {
		d.timer.Stop()
	}
	d.timer = time.AfterFunc(d.window, d.fn)
}

// RBACFilter builds an Envoy RBAC network filter config from intentions.
func RBACFilter(rules []RBACRule) map[string]any {
	policies := map[string]any{}
	for i, r := range rules {
		name := fmt.Sprintf("rule_%d", i)
		action := "ALLOW"
		if r.Action == "deny" {
			action = "DENY"
		}
		policies[name] = map[string]any{
			"permissions": []any{map[string]any{"any": true}},
			"principals": []any{
				map[string]any{
					"authenticated": map[string]any{
						"principal_name": map[string]any{
							"exact": r.SourceSPIFFE,
						},
					},
				},
			},
			"action": action,
		}
	}
	return map[string]any{
		"name": "envoy.filters.network.rbac",
		"typed_config": map[string]any{
			"@type":       "type.googleapis.com/envoy.extensions.filters.network.rbac.v3.RBAC",
			"stat_prefix": "rbac",
			"rules": map[string]any{
				"action":   "ALLOW",
				"policies": policies,
			},
		},
	}
}

// RBACRule is one intention mapped to Envoy RBAC.
type RBACRule struct {
	SourceSPIFFE string
	DestService  string
	Action       string // allow | deny
}
