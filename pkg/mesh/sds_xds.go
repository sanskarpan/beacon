package mesh

import (
	"encoding/json"

	"github.com/sanskar/beacon/pkg/xds"
)

// SDSXDS adapts SDS for xDS TypeSecret serving.
type SDSXDS struct {
	sds *SDS
}

// NewSDSXDS creates an SDS-backed secret source.
func NewSDSXDS(s *SDS) *SDSXDS { return &SDSXDS{sds: s} }

// Get returns a SDSResource for workload+uri, enforcing entitlements.
func (a *SDSXDS) Get(workload, uri string) (*SDSResource, error) {
	id, err := IdentityFromURI(uri)
	if err != nil {
		return nil, err
	}
	return a.sds.Fetch(workload, id)
}

// GetSecret implements xds.SecretSource.
func (a *SDSXDS) GetSecret(nodeID, uri string) (*xds.Resource, error) {
	res, err := a.Get(nodeID, uri)
	if err != nil {
		return nil, err
	}
	body, _ := json.Marshal(map[string]any{
		"name": uri,
		"cert": string(res.CertChain),
		"key":  string(res.PrivateKey),
	})
	return &xds.Resource{
		Name:    uri,
		TypeURL: xds.TypeSecret,
		Body:    body,
	}, nil
}

// Ensure SDSXDS implements xds.SecretSource.
var _ xds.SecretSource = (*SDSXDS)(nil)
