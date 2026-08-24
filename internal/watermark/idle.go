package watermark

import "sync"

// IdleDetector advances the watermark during quiet periods. When no events
// arrive for the idle gap, it pushes the tentative frontier forward so windows
// still fire on a sparse stream instead of stalling forever.
type IdleDetector struct {
	mu        sync.Mutex
	lastEvent int64
	gapMS     int64
	fired     int64
}

// NewIdleDetector creates an idle detector with the given quiet gap.
func NewIdleDetector(gapMS int64) *IdleDetector {
	return &IdleDetector{gapMS: gapMS}
}

// NoteEvent records an event arrival in engine time.
func (d *IdleDetector) NoteEvent(engineTime int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lastEvent = engineTime
}

// ShouldAdvance reports whether the gap since the last event exceeds the idle
// timeout and returns the timestamp the watermark should advance to.
func (d *IdleDetector) ShouldAdvance(now int64) (int64, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if now-d.lastEvent < d.gapMS {
		return 0, false
	}
	d.fired++
	return now, true
}

// Fired returns how many idle advances have been issued.
func (d *IdleDetector) Fired() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.fired
}
