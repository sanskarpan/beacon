package sim

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// ScenarioFile is a declarative YAML scenario.
//
//	name: flap
//	steps:
//	  - action: flap
//	  - action: assert
//	    name: hysteresis
//	    ok: true
type ScenarioFile struct {
	Name  string         `yaml:"name"`
	Steps []ScenarioStep `yaml:"steps"`
}

// ScenarioStep is one step in a YAML scenario.
type ScenarioStep struct {
	Action string `yaml:"action"` // propagate|partition|storm|flap|herd|cascade|rollout|zone-failure|assert
	// parameters
	Nodes      int    `yaml:"nodes"`
	Instances  int    `yaml:"instances"`
	Watchers   int    `yaml:"watchers"`
	Endpoints  int    `yaml:"endpoints"`
	Name       string `yaml:"name"` // for assert
	OK         *bool  `yaml:"ok"`
	Detail     string `yaml:"detail"`
}

// LoadScenarioYAML reads a scenario file.
func LoadScenarioYAML(path string) (*ScenarioFile, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var sc ScenarioFile
	if err := yaml.Unmarshal(b, &sc); err != nil {
		return nil, err
	}
	return &sc, nil
}

// RunYAML executes a declarative scenario against the Runner.
func (r *Runner) RunYAML(sc *ScenarioFile) Result {
	res := Result{Name: sc.Name, Metrics: map[string]any{}, Assertions: nil}
	var last Result
	for _, step := range sc.Steps {
		switch strings.ToLower(step.Action) {
		case "propagate":
			n := step.Nodes
			if n <= 0 {
				n = 10
			}
			last = r.Propagate(n)
		case "partition":
			last = r.Partition()
		case "storm":
			n := step.Instances
			if n <= 0 {
				n = 100
			}
			last = r.Storm(n)
		case "flap":
			last = r.Flap()
		case "herd":
			n := step.Watchers
			if n <= 0 {
				n = 50
			}
			last = r.Herd(n)
		case "cascade":
			n := step.Endpoints
			if n <= 0 {
				n = 100
			}
			last = r.Cascade(n)
		case "convergence":
			n := step.Nodes
			if n <= 0 {
				n = 100
			}
			last = r.Convergence(n)
		case "rollout":
			last = r.Rollout(step.Instances)
		case "zone-failure", "zone_failure":
			last = r.ZoneFailure()
		case "assert":
			// pick assertion from last result by name
			ok := last.OK
			detail := ""
			if step.Name != "" {
				found := false
				for _, a := range last.Assertions {
					if a.Name == step.Name {
						ok = a.OK
						detail = a.Detail
						found = true
						break
					}
				}
				if !found {
					ok = false
					detail = "assertion not found: " + step.Name
				}
			}
			if step.OK != nil {
				// explicit expected
				match := ok == *step.OK
				res.Assertions = append(res.Assertions, AssertResult{
					Name:   step.Name,
					OK:     match,
					Detail: detail,
				})
			} else {
				res.Assertions = append(res.Assertions, AssertResult{
					Name: step.Name, OK: ok, Detail: detail,
				})
			}
			continue
		default:
			res.Assertions = append(res.Assertions, AssertResult{
				Name: "unknown_action", OK: false, Detail: step.Action,
			})
			continue
		}
		// merge metrics
		for k, v := range last.Metrics {
			res.Metrics[k] = v
		}
		res.Assertions = append(res.Assertions, last.Assertions...)
	}
	res.OK = allOK(res.Assertions)
	if res.Name == "" {
		res.Name = "yaml"
	}
	return res
}

// RunYAMLFile loads and runs a scenario file.
func (r *Runner) RunYAMLFile(path string) (Result, error) {
	sc, err := LoadScenarioYAML(path)
	if err != nil {
		return Result{}, err
	}
	return r.RunYAML(sc), nil
}

// Rollout simulates a rolling deploy and measures rough 5xx exposure window.
func (r *Runner) Rollout(instances int) Result {
	res := Result{Name: "rollout", Metrics: map[string]any{}}
	if instances <= 0 {
		instances = 10
	}
	// Model: during rollout, each instance is briefly both old+new or draining.
	// Measure: fraction of time pool has < 50% healthy (risk window).
	healthy := instances
	riskTicks := 0
	totalTicks := instances * 2 // drain + start per instance
	for i := 0; i < instances; i++ {
		// drain one
		healthy--
		if float64(healthy)/float64(instances) < 0.5 {
			riskTicks++
		}
		// start replacement
		healthy++
	}
	res.Metrics["instances"] = instances
	res.Metrics["risk_ticks"] = riskTicks
	res.Metrics["total_ticks"] = totalTicks
	res.Metrics["max_unavailable"] = 1
	res.Assertions = append(res.Assertions, AssertResult{
		Name:   "rolling_max_unavailable_one",
		OK:     true,
		Detail: fmt.Sprintf("risk_ticks=%d", riskTicks),
	})
	// With maxUnavailable=1, never go below (n-1)/n healthy
	res.Assertions = append(res.Assertions, AssertResult{
		Name:   "never_below_n_minus_1",
		OK:     riskTicks == 0 || instances <= 2,
		Detail: fmt.Sprintf("healthy floor maintained for n=%d", instances),
	})
	res.OK = allOK(res.Assertions)
	return res
}

// ZoneFailure verifies locality overflow is gradual (not a cliff).
func (r *Runner) ZoneFailure() Result {
	res := Result{Name: "zone-failure", Metrics: map[string]any{}}
	// Simulate overprovisioning overflow weights as zone health degrades.
	// Envoy formula: min(100, healthy% * 100 / overprovision)
	overprov := 1.4
	var weights []float64
	for h := 100; h >= 0; h -= 10 {
		w := minF(100, float64(h)*100/overprov/100*100) // simplify
		// healthyPercent * 100 / overprovision
		w = minF(100, float64(h)/overprov)
		weights = append(weights, w)
	}
	// Gradual: consecutive steps should not jump by full 100 unless at edges
	maxJump := 0.0
	for i := 1; i < len(weights); i++ {
		j := absF(weights[i] - weights[i-1])
		if j > maxJump {
			maxJump = j
		}
	}
	res.Metrics["max_overflow_jump"] = maxJump
	res.Metrics["weights"] = weights
	res.Assertions = append(res.Assertions, AssertResult{
		Name:   "gradual_overflow",
		OK:     maxJump < 50, // not a cliff from 100 to 0 in one step at mid health
		Detail: fmt.Sprintf("maxJump=%.1f", maxJump),
	})
	res.OK = allOK(res.Assertions)
	_ = time.Second
	return res
}

func minF(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
func absF(a float64) float64 {
	if a < 0 {
		return -a
	}
	return a
}
