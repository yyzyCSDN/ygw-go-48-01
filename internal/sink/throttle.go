package sink

import "sync"

// Throttle applies a soft cap to the pending buffer. When the pending count
// crosses the high watermark the throttle reports backpressure so the engine
// can slow down ingestion instead of growing the buffer without bound.
type Throttle struct {
	mu            sync.Mutex
	highWatermark int
}

// NewThrottle creates a backpressure tracker with the given cap.
func NewThrottle(highWatermark int) *Throttle {
	return &Throttle{highWatermark: highWatermark}
}

// UnderPressure reports whether a pending count exceeds the cap.
func (t *Throttle) UnderPressure(pending int) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return pending > t.highWatermark
}

// Cap returns the configured high watermark.
func (t *Throttle) Cap() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.highWatermark
}
