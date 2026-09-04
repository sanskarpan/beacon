// Package catalog is the service/instance store with monotonic indexing.
package catalog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
)

// HealthStatus is the aggregate health of an instance.
type HealthStatus string

const (
	HealthPassing  HealthStatus = "passing"
	HealthWarning  HealthStatus = "warning"  // serve traffic, but flag it
	HealthCritical HealthStatus = "critical" // remove from the pool
	HealthMaint    HealthStatus = "maintenance"
)

// Severity returns a comparable rank (higher = worse).
func (h HealthStatus) Severity() int {
	switch h {
	case HealthPassing:
		return 0
	case HealthWarning:
		return 1
	case HealthMaint:
		return 2
	case HealthCritical:
		return 3
	default:
		return 3
	}
}

// Aggregate returns the worst status among checks.
// An instance is as healthy as its WORST check: one critical makes the
// instance critical regardless of the others.
func Aggregate(statuses []HealthStatus) HealthStatus {
	if len(statuses) == 0 {
		return HealthPassing
	}
	worst := HealthPassing
	for _, s := range statuses {
		if s.Severity() > worst.Severity() {
			worst = s
		}
	}
	return worst
}

// Locality drives zone-aware routing and failover priority.
type Locality struct {
	Region  string `json:"region,omitempty"`
	Zone    string `json:"zone,omitempty"`
	SubZone string `json:"sub_zone,omitempty"`
}

// Lease binds a registration to a TTL. Without renewal the instance is
// marked critical on expiry, then removed after DeregisterAfter.
//
// A brief network blip should take an instance out of the serving pool, not
// erase its registration and metadata.
type Lease struct {
	ID              string        `json:"id"`
	TTL             time.Duration `json:"ttl"`
	GrantedAt       time.Time     `json:"granted_at"`
	ExpiresAt       time.Time     `json:"expires_at"`
	DeregisterAfter time.Duration `json:"deregister_after"`
	InstanceID      string        `json:"instance_id"`
}

// CheckType enumerates active/passive check kinds.
type CheckType string

const (
	CheckHTTP  CheckType = "http"
	CheckTCP   CheckType = "tcp"
	CheckGRPC  CheckType = "grpc"
	CheckExec  CheckType = "exec"
	CheckTTL   CheckType = "ttl"
	CheckAlias CheckType = "alias"
)

// CheckID uniquely identifies a check.
type CheckID string

// Check is a health check definition attached to an instance.
type Check struct {
	ID                      CheckID           `json:"id"`
	Name                    string            `json:"name,omitempty"`
	Type                    CheckType         `json:"type"`
	Status                  HealthStatus      `json:"status"`
	Output                  string            `json:"output,omitempty"`
	Interval                time.Duration     `json:"interval,omitempty"`
	Timeout                 time.Duration     `json:"timeout,omitempty"`
	HTTP                    string            `json:"http,omitempty"`
	TCP                     string            `json:"tcp,omitempty"`
	GRPC                    string            `json:"grpc,omitempty"`
	GRPCServiceName         string            `json:"grpc_service_name,omitempty"`
	Exec                    string            `json:"exec,omitempty"`
	Args                    []string          `json:"args,omitempty"`
	TTL                     time.Duration     `json:"ttl,omitempty"`
	AliasService            string            `json:"alias_service,omitempty"`
	FailuresBeforeCritical  int               `json:"failures_before_critical,omitempty"`
	SuccessesBeforePassing  int               `json:"successes_before_passing,omitempty"`
	DeregisterCriticalAfter time.Duration     `json:"deregister_critical_after,omitempty"`
	Meta                    map[string]string `json:"meta,omitempty"`
}

// Defaults fills zero thresholds.
func (c *Check) Defaults() {
	if c.FailuresBeforeCritical <= 0 {
		c.FailuresBeforeCritical = 3
	}
	if c.SuccessesBeforePassing <= 0 {
		c.SuccessesBeforePassing = 2
	}
	if c.Interval <= 0 && c.Type != CheckTTL && c.Type != CheckAlias {
		c.Interval = 10 * time.Second
	}
	if c.Timeout <= 0 {
		c.Timeout = 5 * time.Second
	}
	if c.Status == "" {
		c.Status = HealthCritical // unknown until first result
	}
}

// Service is a logical name. Instances implement it.
type Service struct {
	Name        string            `json:"name"`
	Namespace   string            `json:"namespace,omitempty"`
	Tags        []string          `json:"tags,omitempty"`
	Meta        map[string]string `json:"meta,omitempty"`
	ModifyIndex uint64            `json:"modify_index"`
}

// Instance is a concrete endpoint implementing a service.
type Instance struct {
	ID       string            `json:"id"`
	Service  string            `json:"service"`
	Node     string            `json:"node"`
	Address  string            `json:"address"`
	Port     int               `json:"port"`
	Meta     map[string]string `json:"meta,omitempty"`
	Tags     []string          `json:"tags,omitempty"`
	Locality Locality          `json:"locality,omitempty"`
	Weight   int               `json:"weight,omitempty"`
	Checks   []Check           `json:"checks,omitempty"`
	Health   HealthStatus      `json:"health"`
	// LastKnownHealth is restored when a failed node rejoins (before re-verify).
	LastKnownHealth HealthStatus `json:"last_known_health,omitempty"`
	Lease           *Lease       `json:"lease,omitempty"`
	// Incarnation is the per-origin counter for gossip conflict resolution.
	Incarnation  uint64 `json:"incarnation,omitempty"`
	Deregistered bool   `json:"deregistered,omitempty"`
	CreateIndex  uint64 `json:"create_index"`
	ModifyIndex  uint64 `json:"modify_index"`
	TraceID      string `json:"trace_id,omitempty"`
}

// Clone returns a deep copy.
func (i *Instance) Clone() *Instance {
	if i == nil {
		return nil
	}
	cp := *i
	if i.Meta != nil {
		cp.Meta = make(map[string]string, len(i.Meta))
		for k, v := range i.Meta {
			cp.Meta[k] = v
		}
	}
	if i.Tags != nil {
		cp.Tags = append([]string(nil), i.Tags...)
	}
	if i.Checks != nil {
		cp.Checks = make([]Check, len(i.Checks))
		copy(cp.Checks, i.Checks)
		for ci := range cp.Checks {
			if i.Checks[ci].Args != nil {
				cp.Checks[ci].Args = append([]string(nil), i.Checks[ci].Args...)
			}
			if i.Checks[ci].Meta != nil {
				cp.Checks[ci].Meta = make(map[string]string, len(i.Checks[ci].Meta))
				for k, v := range i.Checks[ci].Meta {
					cp.Checks[ci].Meta[k] = v
				}
			}
		}
	}
	if i.Lease != nil {
		l := *i.Lease
		cp.Lease = &l
	}
	return &cp
}

// Equal reports whether two instances carry the same catalog-relevant fields.
func (i *Instance) Equal(o *Instance) bool {
	if i == nil || o == nil {
		return i == o
	}
	// Fast path via JSON for correctness over micro-opts.
	a, _ := json.Marshal(i.canonical())
	b, _ := json.Marshal(o.canonical())
	return bytes.Equal(a, b)
}

func (i *Instance) canonical() *Instance {
	c := i.Clone()
	// strip volatile fields (ModifyIndex/CreateIndex/TraceID/Incarnation are per-origin counters)
	c.ModifyIndex = 0
	c.CreateIndex = 0
	c.TraceID = ""
	c.Incarnation = 0
	c.LastKnownHealth = ""
	return c
}

// Addr returns host:port.
func (i *Instance) Addr() string {
	return fmt.Sprintf("%s:%d", i.Address, i.Port)
}

// QueryOptions filter catalog reads and support blocking queries.
type QueryOptions struct {
	// MinIndex: 0 = return now; N = block until service ModifyIndex > N.
	MinIndex  uint64
	Tags      []string
	Health    []HealthStatus // empty = all
	Passing   bool           // shorthand: only HealthPassing
	Locality  *Locality
	Meta      map[string]string
	Filter    string // simple expression: Meta.version == "v2"
	Namespace string
	// Consistent requests linearizable read (CP mode).
	Consistent bool
	// Stale allows serving from any server.
	Stale bool
	// Wait is the max block duration (caller applies jitter).
	Wait time.Duration
}

// Result is a catalog query response.
type Result struct {
	Service     string        `json:"service"`
	Instances   []*Instance   `json:"instances"`
	Index       uint64        `json:"index"`
	Stale       bool          `json:"stale,omitempty"`
	LastContact time.Duration `json:"last_contact,omitempty"`
}

// Snapshot is a full catalog serialization for Raft/CP and tests.
type Snapshot struct {
	Index     uint64               `json:"index"`
	Services  map[string]*Service  `json:"services"`
	Instances map[string]*Instance `json:"instances"`
}
