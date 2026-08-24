package sink

import (
	"sort"
	"sync"
)

// OutputRecord is one record handed to the sink.
type OutputRecord struct {
	Key   string
	Value int64
	Seq   int64
}

// Sink buffers output records and commits them up to an explicit sequence
// barrier. A checkpoint confirm only happens after CommitUpTo has flushed the
// barrier, so a restart never re-emits or loses records the checkpoint
// considers durable.
type Sink struct {
	mu        sync.Mutex
	pending   []OutputRecord
	committed []OutputRecord
	nextSeq   int64
	throttle  *Throttle
}

// New creates an empty sink.
func New() *Sink {
	return &Sink{nextSeq: 1, throttle: NewThrottle(128)}
}

// Write appends a record to the pending buffer.
func (s *Sink) Write(key string, value int64) OutputRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec := OutputRecord{Key: key, Value: value, Seq: s.nextSeq}
	s.nextSeq++
	s.pending = append(s.pending, rec)
	return rec
}

// CommitUpTo flushes every pending record whose sequence is not greater than
// the barrier into the committed list, preserving arrival order.
func (s *Sink) CommitUpTo(barrier int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	kept := s.pending[:0]
	for _, rec := range s.pending {
		if rec.Seq <= barrier {
			s.committed = append(s.committed, rec)
		} else {
			kept = append(kept, rec)
		}
	}
	s.pending = kept
}

// PendingCount returns the number of unflushed records.
func (s *Sink) PendingCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending)
}

// CommittedCount returns the number of flushed records.
func (s *Sink) CommittedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.committed)
}

// LastCommittedSeq returns the highest committed sequence, or zero.
func (s *Sink) LastCommittedSeq() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.committed) == 0 {
		return 0
	}
	return s.committed[len(s.committed)-1].Seq
}

// CommittedRecords returns a copy of all committed records sorted by sequence.
func (s *Sink) CommittedRecords() []OutputRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]OutputRecord(nil), s.committed...)
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out
}

// Snapshot returns a stable view of the sink for the monitoring page.
func (s *Sink) Snapshot() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[string]int{
		"pending":   len(s.pending),
		"committed": len(s.committed),
		"cap":       s.throttle.Cap(),
	}
}

// Backpressure reports whether the pending buffer exceeds the throttle cap.
func (s *Sink) Backpressure() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.throttle.UnderPressure(len(s.pending))
}
