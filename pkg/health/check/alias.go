package check

import (
	"context"

	"github.com/sanskar/beacon/pkg/catalog"
)

// AliasLookup resolves another service's aggregate health.
type AliasLookup func(service string) catalog.HealthStatus

// AliasCheck mirrors another service's health.
type AliasCheck struct {
	Service string
	Lookup  AliasLookup
}

// Run returns the aliased status.
func (a *AliasCheck) Run(ctx context.Context) (catalog.HealthStatus, string, error) {
	_ = ctx
	if a.Lookup == nil {
		return catalog.HealthCritical, "no alias lookup", nil
	}
	st := a.Lookup(a.Service)
	return st, "alias:" + a.Service, nil
}
