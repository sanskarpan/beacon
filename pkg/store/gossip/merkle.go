package gossip

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/gossip"
)

// InstanceLeaf is one leaf in the catalog Merkle tree (instance identity + version).
type InstanceLeaf struct {
	ID          string `json:"id"`
	Service     string `json:"service"`
	Incarnation uint64 `json:"incarnation"`
	Health      string `json:"health"`
	ModifyIndex uint64 `json:"modify_index"`
	// Hash is sha256 of the leaf content (all identity fields).
	Hash string `json:"hash"`
}

// Digest is a compact Merkle summary of the local catalog for anti-entropy.
//
// Algorithm (documented cost):
//  1. Sort instance IDs and hash each leaf: H(all identity fields).
//  2. Pairwise reduce (Merkle) to a single root; odd leaves are promoted.
//  3. Exchange roots. If equal → done (O(1) round-trip, O(0) payload).
//  4. If unequal, exchange leaf digests and transfer only missing/stale instances
//     (O(diff) bytes), not the full mesh dump of unrelated data.
//
// Cost: build O(n log n) sort + O(n) hash; exchange O(1) for root or O(n) leaf
// digests on mismatch; transfer O(k) for k differing instances.
type Digest struct {
	Root       string            `json:"root"`
	Count      int               `json:"count"`
	Leaves     []InstanceLeaf    `json:"leaves,omitempty"`
	Tombstones map[string]uint64 `json:"tombstones,omitempty"`
}

// BuildDigest constructs a Merkle digest from the local catalog.
// includeLeaves=false returns only the root (cheap equality check).
func (s *Store) BuildDigest(includeLeaves bool) Digest {
	snap := s.local.Snapshot()
	leaves := make([]InstanceLeaf, 0, len(snap.Instances))
	for _, inst := range snap.Instances {
		if inst == nil {
			continue
		}
		inc := inst.Incarnation
		s.mu.Lock()
		if mapInc := s.incarnation[inst.ID]; mapInc > inc {
			inc = mapInc
		}
		s.mu.Unlock()
		leaf := InstanceLeaf{
			ID:          inst.ID,
			Service:     inst.Service,
			Incarnation: inc,
			Health:      string(inst.Health),
			ModifyIndex: inst.ModifyIndex,
		}
		leaf.Hash = hashInstance(inst, inc)
		leaves = append(leaves, leaf)
	}
	sort.Slice(leaves, func(i, j int) bool { return leaves[i].ID < leaves[j].ID })
	root := merkleRoot(leaves)
	s.mu.Lock()
	tombCopy := make(map[string]uint64, len(s.tombstones))
	for k, v := range s.tombstones {
		tombCopy[k] = v
	}
	s.mu.Unlock()
	d := Digest{Root: root, Count: len(leaves), Tombstones: tombCopy}
	if includeLeaves {
		d.Leaves = leaves
	}
	// include tombstones in root hash
	if len(tombCopy) > 0 {
		keys := make([]string, 0, len(tombCopy))
		for k := range tombCopy {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		h := sha256.New()
		h.Write([]byte(root))
		for _, k := range keys {
			h.Write([]byte(fmt.Sprintf("|tomb:%s:%d", k, tombCopy[k])))
		}
		d.Root = hex.EncodeToString(h.Sum(nil))
	}
	return d
}

func hashLeaf(l InstanceLeaf) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s|%d|%s", l.ID, l.Incarnation, l.Health)))
	return hex.EncodeToString(h[:])
}

func hashInstance(inst *catalog.Instance, inc uint64) string {
	tags := append([]string(nil), inst.Tags...)
	sort.Strings(tags)
	tagsStr := strings.Join(tags, ",")
	metaKeys := make([]string, 0, len(inst.Meta))
	for k := range inst.Meta {
		metaKeys = append(metaKeys, k)
	}
	sort.Strings(metaKeys)
	metaStr := ""
	for _, k := range metaKeys {
		metaStr += fmt.Sprintf("%s=%s;", k, inst.Meta[k])
	}
	h := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%s|%d|%d|%s|%s|%d|%s", inst.ID, inst.Service, inst.Address, inst.Port, inst.Weight, tagsStr, metaStr, inc, inst.Health)))
	return hex.EncodeToString(h[:])
}

func merkleRoot(leaves []InstanceLeaf) string {
	if len(leaves) == 0 {
		return hex.EncodeToString(sha256.New().Sum(nil))
	}
	level := make([]string, len(leaves))
	for i, l := range leaves {
		level[i] = l.Hash
	}
	for len(level) > 1 {
		next := make([]string, 0, (len(level)+1)/2)
		for i := 0; i < len(level); i += 2 {
			if i+1 >= len(level) {
				next = append(next, level[i])
				continue
			}
			sum := sha256.Sum256([]byte(level[i] + level[i+1]))
			next = append(next, hex.EncodeToString(sum[:]))
		}
		level = next
	}
	return level[0]
}

// DiffLeaves returns instance IDs that are missing or stale on local relative to remote.
// "Stale" means remote has a higher incarnation (or equal incarnation with different hash).
func DiffLeaves(local, remote []InstanceLeaf) (need []string) {
	loc := make(map[string]InstanceLeaf, len(local))
	for _, l := range local {
		loc[l.ID] = l
	}
	for _, r := range remote {
		l, ok := loc[r.ID]
		if !ok {
			need = append(need, r.ID)
			continue
		}
		if r.Incarnation > l.Incarnation || (r.Incarnation == l.Incarnation && r.Hash != l.Hash) {
			need = append(need, r.ID)
		}
	}
	sort.Strings(need)
	return need
}

// SyncResult is the outcome of a Merkle anti-entropy round.
type SyncResult struct {
	RootEqual   bool
	Transferred int
	Needed      []string
	// BytesSaved estimates bytes not transferred vs a full snapshot dump.
	FullDumpBytes int
	SentBytes     int
}

// MerkleSync reconciles this store against a remote digest + optional instance map.
// When roots match, zero instances are transferred.
// When roots differ, only needed instances from remoteInstances are applied.
func (s *Store) MerkleSync(remote Digest, remoteInstances map[string]*catalog.Instance) SyncResult {
	local := s.BuildDigest(true)
	res := SyncResult{
		FullDumpBytes: estimateSnapshotBytes(remoteInstances),
	}
	if local.Root == remote.Root && local.Count == remote.Count {
		res.RootEqual = true
		res.SentBytes = len(remote.Root) // root-only exchange
		return res
	}
	// Need leaf digests from remote (if only root was sent, treat all remote as needed).
	remoteLeaves := remote.Leaves
	if len(remoteLeaves) == 0 && remoteInstances != nil {
		// reconstruct leaves from provided instances
		for _, inst := range remoteInstances {
			if inst == nil {
				continue
			}
			leaf := InstanceLeaf{
				ID: inst.ID, Service: inst.Service, Incarnation: inst.Incarnation, Health: string(inst.Health), ModifyIndex: inst.ModifyIndex,
			}
			leaf.Hash = hashInstance(inst, inst.Incarnation)
			remoteLeaves = append(remoteLeaves, leaf)
		}
	}
	need := DiffLeaves(local.Leaves, remoteLeaves)
	res.Needed = need
	for _, id := range need {
		inst, ok := remoteInstances[id]
		if !ok || inst == nil {
			continue
		}
		s.ApplyDelta(Delta{
			Type:        gossip.DeltaRegister,
			Instance:    inst.Clone(),
			InstanceID:  inst.ID,
			Incarnation: inst.Incarnation,
			Index:       inst.ModifyIndex,
			Health:      inst.Health,
		})
		res.Transferred++
		b, _ := json.Marshal(inst)
		res.SentBytes += len(b)
	}
	// Handle deletions via tombstones: remote tombstone should deregister local instance
	for id, tombInc := range remote.Tombstones {
		s.mu.Lock()
		localInc := s.incarnation[id]
		if tomb, ok := s.tombstones[id]; ok && tomb > localInc {
			localInc = tomb
		}
		if inst, ok := s.local.GetInstance(id); ok {
			if tombInc >= localInc && tombInc >= inst.Incarnation {
				s.mu.Unlock()
				s.ApplyDelta(Delta{
					Type:        gossip.DeltaDeregister,
					InstanceID:  id,
					Instance:    inst,
					Incarnation: tombInc,
					Index:       inst.ModifyIndex + 1,
				})
				res.Transferred++
				continue
			}
		} else {
			// local has tombstone or no instance, ensure we have tombstone
			if tombInc > localInc {
				s.tombstones[id] = tombInc
				s.incarnation[id] = tombInc
			}
		}
		s.mu.Unlock()
	}
	// Leaf digest exchange cost (approximate)
	if b, err := json.Marshal(remoteLeaves); err == nil {
		res.SentBytes += len(b)
	}
	if b, err := json.Marshal(remote.Tombstones); err == nil {
		res.SentBytes += len(b)
	}
	s.ClearPendingFull()
	return res
}

func estimateSnapshotBytes(m map[string]*catalog.Instance) int {
	if m == nil {
		return 0
	}
	n := 0
	for _, inst := range m {
		if inst == nil {
			continue
		}
		b, _ := json.Marshal(inst)
		n += len(b)
	}
	return n
}

// InstancesByIDs returns a map of local instances for the given IDs (for peer transfer).
func (s *Store) InstancesByIDs(ids []string) map[string]*catalog.Instance {
	out := make(map[string]*catalog.Instance, len(ids))
	for _, id := range ids {
		if inst, ok := s.local.GetInstance(id); ok {
			out[id] = inst.Clone()
		}
	}
	return out
}

// AllInstancesMap returns every local instance keyed by ID.
func (s *Store) AllInstancesMap() map[string]*catalog.Instance {
	snap := s.local.Snapshot()
	out := make(map[string]*catalog.Instance, len(snap.Instances))
	for _, inst := range snap.Instances {
		if inst != nil {
			out[inst.ID] = inst.Clone()
		}
	}
	return out
}

// DigestEqual is a fast root-only comparison helper.
func DigestEqual(a, b Digest) bool {
	return a.Root == b.Root && a.Count == b.Count
}

// FormatDiff is a short debug string of needed IDs.
func FormatDiff(ids []string) string {
	if len(ids) == 0 {
		return "(none)"
	}
	if len(ids) > 8 {
		return strings.Join(ids[:8], ",") + ",..."
	}
	return strings.Join(ids, ",")
}
