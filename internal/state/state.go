package state

import (
	"sort"
	"sync"
)

// SnapshotData is a point-in-time capture of the state store. Data and Applied
// are consistent copies of every committed key and every applied offset taken
// under the store lock, and Seq is the mutation sequence at the instant the
// copy was made. Because the copies and the sequence are read together under
// the same lock, a snapshot never mixes values from different mutation
// rounds: every key and every applied offset reflects the same prefix of the
// mutation sequence.
type SnapshotData struct {
	Data    map[string]int64
	Applied map[int64]bool
	Seq     int64
}

// Store is the keyed state backend. Every mutation runs under mu and raises
// seq, and every snapshot copies the maps under the same mu, so snapshots and
// increments converge on a single total order: a snapshot is always a faithful
// prefix of the mutation sequence, and a restore always resets to that prefix.
// There is no separate "increment journal" replayed on top of a snapshot; the
// snapshot already contains every increment up to Seq, and increments after
// Seq are re-applied by the source replay path.
type Store struct {
	mu      sync.Mutex
	data    map[string]int64
	latest  map[string]int64
	applied map[int64]bool
	seq     int64
}

// NewStore creates an empty state store.
func NewStore() *Store {
	return &Store{
		data:    make(map[string]int64),
		latest:  make(map[string]int64),
		applied: make(map[int64]bool),
	}
}

// Apply folds a delta into a key and raises the mutation sequence.
func (s *Store) Apply(key string, delta int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] += delta
	s.seq++
}

// ApplyRound folds a batch of deltas as one atomic round.
func (s *Store) ApplyRound(deltas map[string]int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, delta := range deltas {
		s.data[key] += delta
	}
	s.seq++
}

// Record sets the latest observed value for a key (replay path).
func (s *Store) Record(key string, value int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.latest[key] = value
	s.seq++
}

// Latest returns the latest observed value for a key.
func (s *Store) Latest(key string) (int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.latest[key]
	return value, ok
}

// Snapshot copies every committed key, every applied offset, and the current
// sequence under the store lock. Holding the lock for the whole copy fences
// out concurrent Apply rounds — operator updates effectively pause for the
// duration of the copy — so the capture is an atomic, self-consistent
// point-in-time view rather than a mix of values from before and after the
// capture. The returned maps are independent copies; callers may mutate them
// or retain them across later mutations without affecting the store.
func (s *Store) Snapshot() SnapshotData {
	s.mu.Lock()
	defer s.mu.Unlock()
	return SnapshotData{
		Data:    copyMap(s.data),
		Applied: copyApplied(s.applied),
		Seq:     s.seq,
	}
}

// RestoreSnapshot resets the committed state to a captured snapshot. The store
// lock is held for the whole replace, so no Apply round can interleave and
// produce a half-old, half-new map. Seq is rewound to the snapshot sequence
// and the applied-offset set is restored to the snapshot's set, so a subsequent
// source replay redoes exactly the offsets that arrived after the snapshot
// (not in snap.Applied) and skips the ones the snapshot already absorbed (in
// snap.Applied). The restored state therefore matches the operator state that
// produced the snapshot; increments after the snapshot are neither lost nor
// double-counted.
func (s *Store) RestoreSnapshot(snap SnapshotData) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = copyMap(snap.Data)
	s.applied = copyApplied(snap.Applied)
	s.seq = snap.Seq
}

// Get returns the committed value of a key.
func (s *Store) Get(key string) (int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.data[key]
	return value, ok
}

// Version returns the current mutation sequence.
func (s *Store) Version() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seq
}

// MarkApplied records that an offset has been durably applied by the operator
// chain. The topology uses this set to reconcile pause/resume buffers.
func (s *Store) MarkApplied(offset int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applied[offset] = true
}

// IsApplied reports whether an offset has already been applied.
func (s *Store) IsApplied(offset int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applied[offset]
}

// AppliedCount returns how many offsets have been applied.
func (s *Store) AppliedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.applied)
}

// Keys returns all committed keys in sorted order.
func (s *Store) Keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.data))
	for key := range s.data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// Size returns the number of committed keys.
func (s *Store) Size() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.data)
}

// LatestCount returns how many keys carry a latest observed value.
func (s *Store) LatestCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.latest)
}

func copyMap(src map[string]int64) map[string]int64 {
	dst := make(map[string]int64, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func copyApplied(src map[int64]bool) map[int64]bool {
	dst := make(map[int64]bool, len(src))
	for offset, ok := range src {
		if ok {
			dst[offset] = true
		}
	}
	return dst
}
