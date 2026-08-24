package window

import (
	"testing"

	"streamengine/internal/agg"
)

// TestSessionNotSplitByArrivingEvent reproduces the bug where an event arriving
// at the timeout boundary was treated as a timeout: the session end was frozen
// at creation, so the scan closed the session and the in-gap event opened a new
// one, splitting one continuous session into two.
func TestSessionNotSplitByArrivingEvent(t *testing.T) {
	const timeout = int64(10000)
	store := agg.NewStore()
	s := NewSessionManager(timeout, store)

	// First event at t=0 opens the session. End is frozen at 10000 by the old
	// buggy code.
	s.Touch("k", 0, 10, true)

	// A second event arrives inside the live gap, just before the first event's
	// fixed end would fire. Its event time (9000) is within one timeout of the
	// previous activity (0), so the session must stay continuous.
	s.Touch("k", 9000, 20, true)

	// A watermark advance to exactly the frozen end (10000) must NOT close the
	// session, because the last activity was at 9000 and the gap has not
	// elapsed yet (10000 - 9000 = 1000 < 10000).
	emitted := s.ScanTimeout(10000)
	if len(emitted) != 0 {
		t.Fatalf("session wrongly closed at the frozen end: %+v", emitted)
	}
	if !s.IsActive("k") {
		t.Fatalf("session must still be live after an in-gap event")
	}

	// Only after the full gap elapses past the last activity (9000 + 10000 =
	// 19000) may the session close, and it must contain BOTH values.
	emitted = s.ScanTimeout(19000)
	if len(emitted) != 1 {
		t.Fatalf("expected one session result, got %d", len(emitted))
	}
	if emitted[0].Count != 2 || emitted[0].Sum != 30 {
		t.Fatalf("session was split: expected count=2 sum=30, got %+v", emitted[0])
	}
	if s.IsActive("k") {
		t.Fatalf("session must close after the full idle gap")
	}
}

// TestSessionLateEventDoesNotReopenTimeout ensures a late, smaller-timestamp
// event after a long quiet stretch does not pull lastActive backward and defer
// a close that should already have happened.
func TestSessionLateEventDoesNotReopenTimeout(t *testing.T) {
	const timeout = int64(10000)
	store := agg.NewStore()
	s := NewSessionManager(timeout, store)

	s.Touch("k", 20000, 5, true)
	// A late event whose timestamp is older than the last activity must not
	// advance liveness or reopen the close window.
	s.Touch("k", 5000, 7, true)

	// The gap from the last real activity (20000) has elapsed at 30000.
	emitted := s.ScanTimeout(30000)
	if len(emitted) != 1 {
		t.Fatalf("expected the session to close, got %d results", len(emitted))
	}
	if emitted[0].Count != 2 || emitted[0].Sum != 12 {
		t.Fatalf("expected both values folded, got %+v", emitted[0])
	}
}

// TestSessionPingKeepsSessionAlive verifies a ping (no value) still refreshes
// the session end so an idle-close does not fire while pings are arriving.
func TestSessionPingKeepsSessionAlive(t *testing.T) {
	const timeout = int64(10000)
	store := agg.NewStore()
	s := NewSessionManager(timeout, store)

	s.Touch("k", 0, 0, true)
	// Pings every 5000ms stay within the gap; the session must not close.
	s.Touch("k", 5000, 0, false)
	s.Touch("k", 10000, 0, false)
	s.Touch("k", 15000, 0, false)

	if emitted := s.ScanTimeout(15000); len(emitted) != 0 {
		t.Fatalf("pings should keep the session live, got %+v", emitted)
	}
	if !s.IsActive("k") {
		t.Fatalf("session should still be live under steady pings")
	}
	// One step past the last ping + gap closes it.
	if emitted := s.ScanTimeout(25001); len(emitted) != 1 {
		t.Fatalf("expected close after pings stop, got %d", len(emitted))
	} else if emitted[0].Count != 1 || emitted[0].Sum != 0 {
		t.Fatalf("only the data event should be folded, got %+v", emitted[0])
	}
}
