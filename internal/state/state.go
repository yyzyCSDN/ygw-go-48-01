package state

import (
	"sort"
	"sync"
)

// SnapshotData is a point-in-time capture of the state store plus the pending
// increment journal that must be replayed on top of a restored snapshot.
type SnapshotData struct {
	Data    map[string]int64
	Pending map[string]int64
	Seq     int64
}

// Store is the keyed state backend. It keeps committed values in Data and an
// increment journal in Pending. Every mutation raises Seq; snapshots record
// the sequence so a restore can replay increments that arrived after the
// snapshot was taken.
type Store struct {
	mu      sync.Mutex
	data    map[string]int64
	pending map[string]int64
	latest  map[string]int64
	applied map[int64]bool
	seq     int64
}

// NewStore creates an empty state store.
func NewStore() *Store {
	return &Store{
		data:    make(map[string]int64),
		pending: make(map[string]int64),
		latest:  make(map[string]int64),
		applied: make(map[int64]bool),
	}
}

// Apply folds a delta into a key and journals it as an unmerged increment.
func (s *Store) Apply(key string, delta int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] += delta
	s.pending[key] += delta
	s.seq++
}

// ApplyRound folds a batch of deltas as one atomic round.
func (s *Store) ApplyRound(deltas map[string]int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, delta := range deltas {
		s.data[key] += delta
		s.pending[key] += delta
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

// Snapshot captures the committed data, the pending journal, and the current
// sequence under one lock, then resets the journal. Callers must replay the
// returned pending map after restoring the data to avoid losing increments.
func (s *Store) Snapshot() SnapshotData {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := SnapshotData{
		Data:    copyMap(s.data),
		Pending: copyMap(s.pending),
		Seq:     s.seq,
	}
	s.pending = make(map[string]int64)
	return snap
}

// RestoreSnapshot replaces committed data with a captured snapshot and then
// replays every increment that arrived after the snapshot was taken, so keys
// updated concurrently with the restore are never lost.
func (s *Store) RestoreSnapshot(snap SnapshotData) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = copyMap(snap.Data)
	for key, delta := range s.pending {
		s.data[key] += delta
	}
	s.pending = make(map[string]int64)
	s.seq = snap.Seq + 1
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
