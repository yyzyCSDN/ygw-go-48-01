package window

import (
	"testing"

	"streamengine/internal/agg"
	"streamengine/internal/model"
	"streamengine/internal/watermark"
)

// TestTriggerWaitsForConfirmedWatermark pins the contract that MaybeTrigger
// closes windows against the confirmed watermark only. A window whose end the
// tentative frontier has already crossed must stay open until the source
// confirms the frontier, so an event arriving during that gap is aggregated
// rather than misclassified as late and dropped from the published result.
func TestTriggerWaitsForConfirmedWatermark(t *testing.T) {
	const size = int64(100)
	store := agg.NewStore()
	m := NewManager(size, store)
	tr := watermark.NewTracker(0)

	// Window [0,100): ingest one event, leave the window's final event
	// outstanding so the gap between advance and confirm is observable.
	if got := m.Ingest(model.NewData("k", 1, 10, 0)); got != OutcomeAccepted {
		t.Fatalf("first event not accepted: %v", got)
	}

	// Advance the tentative frontier past the window end WITHOUT confirming.
	// Before the fix, MaybeTrigger read Current() and would close the window
	// here, cutting off the in-flight final event.
	tr.Advance(150)
	results := m.MaybeTrigger(tr)
	if len(results) != 0 {
		t.Fatalf("window fired on tentative watermark before confirm: %v", results)
	}
	if m.OpenCount() != 1 {
		t.Fatalf("window closed before confirmed watermark: open=%d", m.OpenCount())
	}

	// The final event for the interval arrives while the advance is still
	// unconfirmed. It must be aggregated, not reported as late.
	if got := m.Ingest(model.NewData("k", 2, 90, 1)); got != OutcomeAccepted {
		t.Fatalf("in-flight event reported as late before confirm: %v", got)
	}

	// Source finishes draining and confirms. Now the window may fire.
	tr.Confirm()
	results = m.MaybeTrigger(tr)
	if len(results) != 1 {
		t.Fatalf("window did not fire after confirm: got %d results", len(results))
	}
	r := results[0]
	if r.Count != 2 || r.Sum != 3 {
		t.Fatalf("published aggregate missed the in-flight event: %+v", r)
	}

	// After the close, a genuinely late event must not mutate the published
	// result.
	if got := m.Ingest(model.NewData("k", 99, 90, 2)); got != OutcomeLate {
		t.Fatalf("post-close event not classified as late: %v", got)
	}
	late, ok := store.Result(model.WindowID{Start: 0, End: 100})
	if !ok {
		t.Fatalf("result missing after close")
	}
	if late.Count != 2 || late.Sum != 3 {
		t.Fatalf("published values changed after late update: %+v", late)
	}
	if late.LateCount != 1 || late.LateSum != 99 {
		t.Fatalf("late event not accumulated separately: %+v", late)
	}
}
