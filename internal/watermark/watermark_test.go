package watermark

import "testing"

// TestAdvanceDoesNotPublishConfirmed pins the contract that Advance only
// raises the tentative frontier. A window trigger observes the confirmed
// watermark, so publishing a frontier the source has not finished draining
// would let the trigger close a window while the interval's final events are
// still in flight.
func TestAdvanceDoesNotPublishConfirmed(t *testing.T) {
	tr := NewTracker(0)

	tr.Advance(100)

	if got := tr.Confirmed(); got != 0 {
		t.Fatalf("Advance published confirmed watermark early: got %d want 0", got)
	}
	if got := tr.Current(); got != 100 {
		t.Fatalf("Advance did not raise tentative frontier: got %d want 100", got)
	}
	if lag := tr.Lag(); lag != 100 {
		t.Fatalf("Lag after Advance without Confirm: got %d want 100", lag)
	}

	tr.Confirm()
	if got := tr.Confirmed(); got != 100 {
		t.Fatalf("Confirm did not publish confirmed watermark: got %d want 100", got)
	}
	if lag := tr.Lag(); lag != 0 {
		t.Fatalf("Lag after Confirm: got %d want 0", lag)
	}
}

// TestConfirmNeverRaisesCurrent pins that Confirm only publishes; it must not
// advance the frontier itself, otherwise the drain-before-confirm handshake
// the source relies on would be bypassed.
func TestConfirmNeverRaisesCurrent(t *testing.T) {
	tr := NewTracker(0)

	tr.Confirm()

	if got := tr.Current(); got != 0 {
		t.Fatalf("Confirm raised tentative frontier: got %d want 0", got)
	}
	if got := tr.Confirmed(); got != 0 {
		t.Fatalf("Confirm moved confirmed without an Advance: got %d want 0", got)
	}
}

// TestAdvanceMonotonicity ensures a stale Advance cannot move the watermark
// backwards, which would let a closed interval reopen.
func TestAdvanceMonotonicity(t *testing.T) {
	tr := NewTracker(0)
	tr.Advance(500)
	tr.Advance(100) // stale, must be ignored

	if got := tr.Current(); got != 500 {
		t.Fatalf("stale Advance moved frontier backwards: got %d want 500", got)
	}
}
