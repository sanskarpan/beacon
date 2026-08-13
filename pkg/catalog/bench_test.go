package catalog_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/sanskar/beacon/pkg/catalog"
)

func BenchmarkGet10k(b *testing.B) {
	s := catalog.NewStore()
	ctx := context.Background()
	for i := 0; i < 10000; i++ {
		_, _ = s.Register(ctx, &catalog.Instance{
			ID: fmt.Sprintf("i%d", i), Service: "svc", Node: "n",
			Address: "10.0.0.1", Port: 8000 + (i % 1000), Health: catalog.HealthPassing,
		})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.GetNow("svc", catalog.QueryOptions{Passing: true})
	}
}

func BenchmarkRegister(b *testing.B) {
	s := catalog.NewStore()
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = s.Register(ctx, &catalog.Instance{
			ID: fmt.Sprintf("r%d", i), Service: "s", Node: "n",
			Address: "1.1.1.1", Port: 1, Health: catalog.HealthPassing,
		})
	}
}
