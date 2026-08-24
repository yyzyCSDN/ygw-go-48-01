package window

import (
	"testing"

	"streamengine/internal/model"
	"streamengine/internal/watermark"
)

// recordingSink records the calls a window manager makes into the result store.
// It is the test double for the agg.Store used to assert routing decisions.
type recordingSink struct {
	emitCalls  []emitCall
	updateVals []int64
	lateVals   []int64
}

type emitCall struct {
	w      model.WindowID
	values []int64
}

func (r *recordingSink) EmitResult(w model.WindowID, values []int64) (*model.WindowResult, bool) {
	r.emitCalls = append(r.emitCalls, emitCall{w: w, values: values})
	return &model.WindowResult{Window: w, Emitted: true}, true
}

func (r *recordingSink) Update(w model.WindowID, value int64) {
	r.updateVals = append(r.updateVals, value)
}

func (r *recordingSink) LateUpdate(w model.WindowID, value int64) {
	r.lateVals = append(r.lateVals, value)
}

// TestManagerLateEventRoutedToLateUpdate verifies that an event arriving after
// its window has already fired is routed onto the late-only path
// (LateUpdate), never the regular Update path that could overwrite the
// published aggregate.
func TestManagerLateEventRoutedToLateUpdate(t *testing.T) {
	sink := &recordingSink{}
	m := NewManager(100, sink)
	tracker := watermark.NewTracker(0)

	// Ingest one event and advance the watermark past the window end so the
	// window fires and closes.
	ev := model.NewData("k", 5, 10, 1)
	if got := m.Ingest(ev); got != OutcomeAccepted {
		t.Fatalf("first event should be accepted, got %v", got)
	}
	tracker.Advance(150)
	tracker.Confirm()
	m.MaybeTrigger(tracker)

	if len(sink.emitCalls) != 1 {
		t.Fatalf("window should have fired once, got %d", len(sink.emitCalls))
	}

	// A late event for the now-closed window arrives.
	late := model.NewData("k", 7, 10, 2)
	got := m.Ingest(late)
	if got != OutcomeLate {
		t.Fatalf("event for closed window should be OutcomeLate, got %v", got)
	}

	if len(sink.updateVals) != 0 {
		t.Fatalf("late event must not go through Update (would overwrite published result): got %v",
			sink.updateVals)
	}
	if len(sink.lateVals) != 1 || sink.lateVals[0] != 7 {
		t.Fatalf("late event should be routed to LateUpdate with the value 7, got %v", sink.lateVals)
	}
}
