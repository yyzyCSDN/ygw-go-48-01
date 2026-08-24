package source

import "sync"

// OffsetTracker records the consumer progress of the source. The committed
// offset is advanced only after a checkpoint confirms that everything up to
// that offset has been durably persisted.
type OffsetTracker struct {
	mu        sync.Mutex
	committed int64
}

// NewOffsetTracker creates an offset tracker starting at the given offset.
func NewOffsetTracker(start int64) *OffsetTracker {
	return &OffsetTracker{committed: start}
}

// Commit advances the committed offset; regressive commits are ignored.
func (o *OffsetTracker) Commit(offset int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if offset > o.committed {
		o.committed = offset
	}
}

// Committed returns the current committed offset.
func (o *OffsetTracker) Committed() int64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.committed
}

// Pending reports how many offsets after the committed point exist.
func (o *OffsetTracker) Pending(high int64) int64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	if high <= o.committed {
		return 0
	}
	return high - o.committed
}
