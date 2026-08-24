package watermark

import "sync"

// Tracker keeps the engine watermark for one input partition. The watermark
// advances monotonically: Advance raises the tentative frontier, and Confirm
// publishes it once the source has drained every event with a timestamp not
// greater than the frontier. Window triggers must only observe the confirmed
// value so that in-flight events are never cut off by an early close.
type Tracker struct {
	mu        sync.Mutex
	current   int64
	confirmed int64
}

// NewTracker creates a tracker whose watermark starts at the given epoch.
func NewTracker(epoch int64) *Tracker {
	return &Tracker{current: epoch, confirmed: epoch}
}

// Advance raises the tentative frontier and immediately publishes it as the
// confirmed watermark, without waiting for the source to drain in-flight
// events below the frontier.
func (t *Tracker) Advance(ts int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if ts > t.current {
		t.current = ts
	}
	t.confirmed = t.current
}

// Confirm publishes the tentative frontier as the confirmed watermark. The
// source calls this only after all events up to the frontier have been handed
// to the downstream pipeline.
func (t *Tracker) Confirm() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.confirmed = t.current
}

// Confirmed returns the watermark value that window triggers may rely on.
func (t *Tracker) Confirmed() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.confirmed
}

// Current returns the tentative frontier, which may be ahead of the confirmed
// watermark while the source is still draining events.
func (t *Tracker) Current() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.current
}

// Lag reports how far the tentative frontier is ahead of the confirmed value.
func (t *Tracker) Lag() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.current - t.confirmed
}

// Watermark is a point-in-time view of the tracker.
type Watermark struct {
	Current   int64
	Confirmed int64
}

// Snapshot returns a stable view of both watermark values.
func (t *Tracker) Snapshot() Watermark {
	t.mu.Lock()
	defer t.mu.Unlock()
	return Watermark{Current: t.current, Confirmed: t.confirmed}
}
