package agg

import (
	"sort"
	"sync"

	"streamengine/internal/model"
)

// Store owns the aggregated results of every window. A result becomes
// immutable once EmitResult publishes it: later updates for the same window
// are folded into the late-only accumulation and never overwrite the values
// downstream consumers already saw.
type Store struct {
	mu      sync.Mutex
	results map[model.WindowID]*model.WindowResult
}

// NewStore creates an empty aggregation store.
func NewStore() *Store {
	return &Store{results: make(map[model.WindowID]*model.WindowResult)}
}

// Update folds a value into a window's result. Emitted results are protected:
// the value lands in the late accumulation instead of mutating the published
// aggregate.
func (s *Store) Update(w model.WindowID, value int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.results[w]
	if r == nil {
		r = &model.WindowResult{Window: w}
		s.results[w] = r
	}
	if r.Emitted {
		r.ApplyLate(value)
		return
	}
	r.Apply(value)
}

// LateUpdate folds a value that arrived after its window already closed.
func (s *Store) LateUpdate(w model.WindowID, value int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.results[w]
	if r == nil {
		r = &model.WindowResult{Window: w}
		s.results[w] = r
	}
	r.ApplyLate(value)
}

// EmitResult publishes a window's accumulated values. A window that already
// fired can be emitted again, which surfaces a duplicate result downstream.
func (s *Store) EmitResult(w model.WindowID, values []int64) (*model.WindowResult, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.results[w]
	if r == nil {
		r = &model.WindowResult{Window: w}
		s.results[w] = r
	}
	for _, value := range values {
		r.Apply(value)
	}
	r.Emitted = true
	snap := r.Snapshot()
	return &snap, true
}

// Result returns a snapshot of a window's result, or false if unknown.
func (s *Store) Result(w model.WindowID) (model.WindowResult, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.results[w]
	if !ok {
		return model.WindowResult{}, false
	}
	return r.Snapshot(), true
}

// ResultCount returns the number of tracked results.
func (s *Store) ResultCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.results)
}

// Summary returns a stable view of every result for the monitoring page.
func (s *Store) Summary() []model.WindowResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]model.WindowResult, 0, len(s.results))
	for _, r := range s.results {
		out = append(out, r.Snapshot())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Window.String() < out[j].Window.String() })
	return out
}

// PartitionedStore keeps per-key aggregation state inside parallel partitions.
// A repartition must move existing keys between partitions so that every key
// lives in exactly one partition at any point in time.
type PartitionedStore struct {
	mu    sync.Mutex
	parts []map[string]int64
}

// NewPartitionedStore creates a store with the given initial partition count.
func NewPartitionedStore(partitions int) *PartitionedStore {
	parts := make([]map[string]int64, partitions)
	for i := range parts {
		parts[i] = make(map[string]int64)
	}
	return &PartitionedStore{parts: parts}
}

// Add folds a value into the given partition for a key.
func (p *PartitionedStore) Add(part model.PartitionID, key string, value int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.parts[int(part)][key] += value
}

// Value returns the state of a key inside one partition.
func (p *PartitionedStore) Value(part model.PartitionID, key string) (int64, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	value, ok := p.parts[int(part)][key]
	return value, ok
}

// Keys returns every key currently stored, deduplicated and sorted.
func (p *PartitionedStore) Keys() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	seen := make(map[string]bool)
	for _, part := range p.parts {
		for key := range part {
			seen[key] = true
		}
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// Repartition moves every key into its partition under the new count. The
// route callback must be deterministic for a given key and partition count.
func (p *PartitionedStore) Repartition(oldCount int, newCount int, route func(string, int) model.PartitionID) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if newCount == oldCount || len(p.parts) != oldCount {
		return 0
	}
	next := make([]map[string]int64, newCount)
	for i := range next {
		next[i] = make(map[string]int64)
	}
	moved := 0
	for _, part := range p.parts {
		for key, value := range part {
			next[int(route(key, newCount))][key] += value
			moved++
		}
	}
	p.parts = next
	return moved
}

// Total returns the combined value of a key across all partitions.
func (p *PartitionedStore) Total(key string) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	var total int64
	for _, part := range p.parts {
		total += part[key]
	}
	return total
}

// PartitionCount returns how many partitions hold a key.
func (p *PartitionedStore) PartitionCount(key string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	count := 0
	for _, part := range p.parts {
		if _, ok := part[key]; ok {
			count++
		}
	}
	return count
}

// Partitions returns the number of partitions in the store.
func (p *PartitionedStore) Partitions() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.parts)
}
