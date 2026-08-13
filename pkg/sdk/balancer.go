package sdk

import (
	"sync"

	"github.com/sanskar/beacon/pkg/lb"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/connectivity"
)

// RegisterBalancers registers beacon LB policies with gRPC:
// round_robin (stdlib), plus beacon_p2c, beacon_wrr, beacon_maglev, beacon_ring_hash.
func RegisterBalancers() {
	registerOnce.Do(func() {
		balancer.Register(newBuilder("beacon_p2c", "p2c"))
		balancer.Register(newBuilder("beacon_wrr", "weighted_round_robin"))
		balancer.Register(newBuilder("beacon_maglev", "maglev"))
		balancer.Register(newBuilder("beacon_ring_hash", "ring_hash"))
		balancer.Register(newBuilder("beacon_least_request", "least_request"))
	})
}

var registerOnce sync.Once

func newBuilder(name, policy string) balancer.Builder {
	return base.NewBalancerBuilder(name, &pickerBuilder{policy: policy}, base.Config{HealthCheck: true})
}

type pickerBuilder struct {
	policy string
}

func (b *pickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	eps := make([]*lb.Endpoint, 0, len(info.ReadySCs))
	scByAddr := map[string]balancer.SubConn{}
	for sc, scInfo := range info.ReadySCs {
		addr := scInfo.Address.Addr
		eps = append(eps, &lb.Endpoint{Addr: addr, Weight: 1, Healthy: true})
		scByAddr[addr] = sc
	}
	inner := lb.NewPicker(b.policy, eps)
	return &grpcPicker{inner: inner, scByAddr: scByAddr}
}

type grpcPicker struct {
	inner    lb.Picker
	scByAddr map[string]balancer.SubConn
}

func (p *grpcPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	if len(p.scByAddr) == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}
	ep, done, err := p.inner.Pick(lb.PickInfo{HashKey: info.FullMethodName})
	if err != nil {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}
	sc, ok := p.scByAddr[ep.Addr]
	if !ok {
		// fall back to any
		for _, s := range p.scByAddr {
			sc = s
			break
		}
	}
	return balancer.PickResult{
		SubConn: sc,
		Done: func(di balancer.DoneInfo) {
			if done != nil {
				done(lb.DoneInfo{Err: di.Err})
			}
		},
	}, nil
}

// Ensure connectivity import used for health-aware builds.
var _ = connectivity.Ready
