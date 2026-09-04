// Package xds implements a minimal ADS control plane (SotW + Delta).
//
// ADS ordering (make before break):
//
//	ADD:    CDS → EDS → LDS → RDS
//	REMOVE: LDS → RDS → EDS → CDS
//
// NACK is detected by the PRESENCE of error_detail, not version comparison.
// A NACK must NOT trigger a resend of the same config.
package xds

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/events"
	"github.com/sanskar/beacon/pkg/store"
)

// Resource type URLs (Envoy-compatible names).
const (
	TypeCluster  = "type.googleapis.com/envoy.config.cluster.v3.Cluster"
	TypeEndpoint = "type.googleapis.com/envoy.config.endpoint.v3.ClusterLoadAssignment"
	TypeListener = "type.googleapis.com/envoy.config.listener.v3.Listener"
	TypeRoute    = "type.googleapis.com/envoy.config.route.v3.RouteConfiguration"
	TypeSecret   = "type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.Secret"
)

// AddOrder is bottom-up for new config.
var AddOrder = []string{TypeCluster, TypeEndpoint, TypeListener, TypeRoute}

// RemoveOrder is the reverse.
var RemoveOrder = []string{TypeListener, TypeRoute, TypeEndpoint, TypeCluster}

// Resource is a versioned config blob.
type Resource struct {
	Name    string `json:"name"`
	TypeURL string `json:"type_url"`
	Body    []byte `json:"body"`
}

// Snapshot is the full config for a node.
type Snapshot struct {
	Version   string
	Resources map[string][]Resource // typeURL → resources
}

// DiscoveryRequest is what a proxy sends.
type DiscoveryRequest struct {
	NodeID        string
	TypeURL       string
	VersionInfo   string
	ResponseNonce string
	ResourceNames []string
	// ErrorDetail non-nil means NACK.
	ErrorDetail *ErrorDetail
	// Delta fields
	ResourceNamesSubscribe   []string
	ResourceNamesUnsubscribe []string
}

// ErrorDetail indicates a NACK.
type ErrorDetail struct {
	Message string
}

// DiscoveryResponse is what we send.
type DiscoveryResponse struct {
	VersionInfo string
	Nonce       string
	TypeURL     string
	Resources   []Resource
	// Delta
	RemovedResources []string
	// Byte accounting
	Bytes int
}

// StreamState tracks one ADS stream.
type StreamState struct {
	mu           sync.Mutex
	NodeID       string
	Subscribed   map[string]map[string]struct{} // typeURL → names (empty = wildcard)
	LastVersion  map[string]string
	LastNonce    map[string]string
	Acked        map[string]string
	Nacked       map[string]string
	NonceCounter uint64
}

// SecretSource provides SDS secrets over ADS.
type SecretSource interface {
	GetSecret(nodeID, spiffeURI string) (*Resource, error)
}

// Server is the xDS control plane.
type Server struct {
	mu        sync.Mutex
	store     store.CatalogStore
	bus       *events.Bus
	snapshots map[string]*Snapshot // nodeID → snap
	streams   map[string]*StreamState
	// last pushed config hash per type to detect unchanged NACK resend attempts
	lastPushHash map[string]string
	debounce     time.Duration
	secretSource SecretSource
}

// New creates an xDS server.
func New(st store.CatalogStore, bus *events.Bus) *Server {
	return &Server{
		store:        st,
		bus:          bus,
		snapshots:    make(map[string]*Snapshot),
		streams:      make(map[string]*StreamState),
		lastPushHash: make(map[string]string),
		debounce:     50 * time.Millisecond,
	}
}

// WithSDS attaches a secret source multiplexed on this ADS stream (TODO-029).
func (s *Server) WithSDS(src SecretSource) *Server {
	s.mu.Lock()
	s.secretSource = src
	s.mu.Unlock()
	return s
}

// BuildSnapshot constructs CDS/EDS/LDS/RDS from the catalog.
func (s *Server) BuildSnapshot(nodeID string) *Snapshot {
	services := s.store.ListServices()
	resources := map[string][]Resource{
		TypeCluster:  {},
		TypeEndpoint: {},
		TypeListener: {},
		TypeRoute:    {},
	}
	// deterministic order
	names := make([]string, 0, len(services))
	for n := range services {
		names = append(names, n)
	}
	sortStrings(names)

	for _, name := range names {
		res := s.store.GetNow(name, catalog.QueryOptions{Passing: true})
		// CDS
		clusterBody, _ := json.Marshal(map[string]any{"name": name, "type": "EDS"})
		resources[TypeCluster] = append(resources[TypeCluster], Resource{
			Name: name, TypeURL: TypeCluster, Body: clusterBody,
		})
		// EDS: one resource per endpoint so Delta only ships the changed instance
		// (SotW still lists all; single-endpoint change is O(1) bytes in Delta).
		for _, in := range res.Instances {
			edsBody, _ := json.Marshal(map[string]any{
				"cluster": name, "id": in.ID,
				"address": in.Address, "port": in.Port, "weight": in.Weight,
				"zone": in.Locality.Zone,
			})
			resources[TypeEndpoint] = append(resources[TypeEndpoint], Resource{
				Name: name + "/" + in.ID, TypeURL: TypeEndpoint, Body: edsBody,
			})
		}
		// LDS
		lisBody, _ := json.Marshal(map[string]any{"name": name + "_listener", "port": 80, "route": name + "_route"})
		resources[TypeListener] = append(resources[TypeListener], Resource{
			Name: name + "_listener", TypeURL: TypeListener, Body: lisBody,
		})
		// RDS
		routeBody, _ := json.Marshal(map[string]any{"name": name + "_route", "cluster": name})
		resources[TypeRoute] = append(resources[TypeRoute], Resource{
			Name: name + "_route", TypeURL: TypeRoute, Body: routeBody,
		})
	}
	// version = content hash
	h := sha256.New()
	for _, t := range AddOrder {
		for _, r := range resources[t] {
			h.Write([]byte(r.Name))
			h.Write(r.Body)
		}
	}
	ver := hex.EncodeToString(h.Sum(nil))[:12]
	snap := &Snapshot{Version: ver, Resources: resources}
	s.mu.Lock()
	s.snapshots[nodeID] = snap
	s.mu.Unlock()
	return snap
}

// HandleRequest processes an ADS request and returns ordered responses.
// Returns nil if nothing to send (NACK wait, already acked same version).
func (s *Server) HandleRequest(req *DiscoveryRequest) []*DiscoveryResponse {

	s.mu.Lock()
	st, ok := s.streams[req.NodeID]
	if !ok {
		st = &StreamState{
			NodeID:      req.NodeID,
			Subscribed:  make(map[string]map[string]struct{}),
			LastVersion: make(map[string]string),
			LastNonce:   make(map[string]string),
			Acked:       make(map[string]string),
			Nacked:      make(map[string]string),
		}
		s.streams[req.NodeID] = st
	}
	s.mu.Unlock()
	st.mu.Lock()
	defer st.mu.Unlock()

	// NACK detection: presence of error_detail — do NOT resend.
	if req.ErrorDetail != nil {
		st.Nacked[req.TypeURL] = req.ResponseNonce
		if s.bus != nil {
			s.bus.Publish(events.Event{
				Kind:   events.EvXDSNack,
				Node:   req.NodeID,
				Detail: req.ErrorDetail.Message,
				Meta:   map[string]any{"type": req.TypeURL, "version": req.VersionInfo},
			})
		}
		return nil
	}

	// ACK
	if req.ResponseNonce != "" && req.ResponseNonce == st.LastNonce[req.TypeURL] {
		st.Acked[req.TypeURL] = req.VersionInfo
		delete(st.Nacked, req.TypeURL)
		if s.bus != nil {
			s.bus.Publish(events.Event{
				Kind:   events.EvXDSAck,
				Node:   req.NodeID,
				Detail: req.TypeURL + " " + req.VersionInfo,
			})
		}
	} else if req.ResponseNonce != "" { //nolint:staticcheck // SA9003: stale nonce is a subscription change, not an ACK — explicit no-op by design (SPEC §12).
		// Stale nonce: not an ACK, just a subscription change
	}

	// Update subscriptions
	if len(req.ResourceNames) > 0 {
		if st.Subscribed[req.TypeURL] == nil {
			st.Subscribed[req.TypeURL] = make(map[string]struct{})
		}
		for _, n := range req.ResourceNames {
			st.Subscribed[req.TypeURL][n] = struct{}{}
		}
	}
	for _, n := range req.ResourceNamesSubscribe {
		if st.Subscribed[req.TypeURL] == nil {
			st.Subscribed[req.TypeURL] = make(map[string]struct{})
		}
		st.Subscribed[req.TypeURL][n] = struct{}{}
	}
	for _, n := range req.ResourceNamesUnsubscribe {
		delete(st.Subscribed[req.TypeURL], n)
	}

	snap := s.BuildSnapshot(req.NodeID)
	// Push in dependency order for full SotW (AddOrder for config growth,
	// RemoveOrder when previously-pushed types were deleted), or just the
	// requested type.
	types := []string{req.TypeURL}
	if req.TypeURL == "" || req.TypeURL == "ads" {
		types = OrderedTypes(snap, st.LastVersion)
	}

	var out []*DiscoveryResponse
	for _, t := range types {
		// SDS handling
		if t == TypeSecret && s.secretSource != nil {
			// Collect requested secrets
			names := map[string]struct{}{}
			for n := range st.Subscribed[t] {
				names[n] = struct{}{}
			}
			for _, n := range req.ResourceNames {
				if t == req.TypeURL {
					names[n] = struct{}{}
				}
			}
			if len(names) == 0 && len(req.ResourceNames) == 0 {
				// wildcard or no subscription yet: try to serve any subscribed
				// If still empty, skip push until explicit subscription
				continue
			}
			var secRes []Resource
			for n := range names {
				r, err := s.secretSource.GetSecret(req.NodeID, n)
				if err == nil && r != nil {
					secRes = append(secRes, *r)
				}
			}
			if len(secRes) == 0 {
				continue
			}
			// version from hash of secret bodies
			h := sha256.New()
			for _, r := range secRes {
				h.Write([]byte(r.Name))
				h.Write(r.Body)
			}
			ver := hex.EncodeToString(h.Sum(nil))[:12]
			hash := ver + ":" + t
			if st.LastVersion[t] == ver && st.Nacked[t] == "" {
				continue
			}
			if st.Acked[t] == ver && st.Nacked[t] == "" {
				continue
			}
			if st.Nacked[t] != "" && s.lastPushHash[req.NodeID+t] == hash {
				continue
			}
			st.NonceCounter++
			nonce := fmt.Sprintf("%s-%d", req.NodeID, st.NonceCounter)
			st.LastNonce[t] = nonce
			st.LastVersion[t] = ver
			s.lastPushHash[req.NodeID+t] = hash
			bytes := 0
			for _, r := range secRes {
				bytes += len(r.Body) + len(r.Name)
			}
			dr := &DiscoveryResponse{
				VersionInfo: ver,
				Nonce:       nonce,
				TypeURL:     t,
				Resources:   secRes,
				Bytes:       bytes,
			}
			out = append(out, dr)
			if s.bus != nil {
				s.bus.Publish(events.Event{
					Kind:   events.EvXDSPush,
					Node:   req.NodeID,
					Detail: fmt.Sprintf("%s v=%s resources=%d bytes=%d", t, ver, len(secRes), bytes),
				})
			}
			continue
		}
		if st.LastVersion[t] == snap.Version && st.Nacked[t] == "" {
			continue // already pushed this version (even if not yet ACKed)
		}
		if st.Acked[t] == snap.Version && st.Nacked[t] == "" {
			continue // already has this version
		}
		// If previously NACK'd this exact content, still don't resend same hash.
		hash := snap.Version + ":" + t
		if st.Nacked[t] != "" && s.lastPushHash[req.NodeID+t] == hash {
			continue
		}
		res := snap.Resources[t]
		// filter by subscription if not wildcard
		if subs := st.Subscribed[t]; len(subs) > 0 {
			filtered := res[:0]
			for _, r := range res {
				if _, ok := subs[r.Name]; ok {
					filtered = append(filtered, r)
				}
			}
			res = filtered
		}
		st.NonceCounter++
		nonce := fmt.Sprintf("%s-%d", req.NodeID, st.NonceCounter)
		st.LastNonce[t] = nonce
		st.LastVersion[t] = snap.Version
		s.lastPushHash[req.NodeID+t] = hash

		bytes := 0
		for _, r := range res {
			bytes += len(r.Body) + len(r.Name)
		}
		dr := &DiscoveryResponse{
			VersionInfo: snap.Version,
			Nonce:       nonce,
			TypeURL:     t,
			Resources:   res,
			Bytes:       bytes,
		}
		out = append(out, dr)
		if s.bus != nil {
			s.bus.Publish(events.Event{
				Kind:   events.EvXDSPush,
				Node:   req.NodeID,
				Detail: fmt.Sprintf("%s v=%s resources=%d bytes=%d", t, snap.Version, len(res), bytes),
			})
		}
	}
	return out
}

// DeltaResponse computes an incremental update vs previous snapshot.
func (s *Server) DeltaResponse(nodeID, typeURL string, prev, curr *Snapshot) *DiscoveryResponse {
	if curr == nil {
		return nil
	}
	prevNames := map[string]Resource{}
	if prev != nil {
		for _, r := range prev.Resources[typeURL] {
			prevNames[r.Name] = r
		}
	}
	var changed []Resource
	var removed []string
	currNames := map[string]struct{}{}
	for _, r := range curr.Resources[typeURL] {
		currNames[r.Name] = struct{}{}
		old, ok := prevNames[r.Name]
		if !ok || !bytes.Equal(old.Body, r.Body) {
			changed = append(changed, r)
		}
	}
	for name := range prevNames {
		if _, ok := currNames[name]; !ok {
			removed = append(removed, name)
		}
	}
	bytes := 0
	for _, r := range changed {
		bytes += len(r.Body)
	}
	return &DiscoveryResponse{
		VersionInfo:      curr.Version,
		TypeURL:          typeURL,
		Resources:        changed,
		RemovedResources: removed,
		Bytes:            bytes,
	}
}

// Status returns xDS status for console.
func (s *Server) Status(node string) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	if node != "" {
		if snap, ok := s.snapshots[node]; ok {
			return map[string]any{"node": node, "version": snap.Version, "resources": snap.Resources}
		}
		return map[string]any{"node": node, "found": false}
	}
	nodes := make([]string, 0, len(s.snapshots))
	for k := range s.snapshots {
		nodes = append(nodes, k)
	}
	return map[string]any{"nodes": nodes, "count": len(nodes)}
}

// SotWBytes returns total bytes for a full SotW push of typeURL.
func SotWBytes(snap *Snapshot, typeURL string) int {
	n := 0
	for _, r := range snap.Resources[typeURL] {
		n += len(r.Body) + len(r.Name)
	}
	return n
}

func sortStrings(s []string) { sort.Strings(s) }

// OrderedTypes returns the push order for a snapshot: AddOrder (make) and
// RemoveOrder (break). Types with zero resources that were previously pushed
// are ordered per RemoveOrder so Envoy drops listeners/routes before the
// clusters they reference (make-before-break, SPEC §12).
func OrderedTypes(snap *Snapshot, prev map[string]string) []string {
	removal := false
	for _, t := range RemoveOrder {
		if len(snap.Resources[t]) == 0 && prev[t] != "" {
			removal = true
			break
		}
	}
	if removal {
		return RemoveOrder
	}
	return AddOrder
}
