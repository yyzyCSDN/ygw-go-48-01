package agg_test

import (
	"testing"

	"streamengine/internal/agg"
	"streamengine/internal/model"
	"streamengine/internal/watermark"
	"streamengine/internal/window"
)

func TestLateEventUpdatesWindowResult(t *testing.T) {
	store := agg.NewStore()
	wm := watermark.NewTracker(0)
	mgr := window.NewManager(100, store)

	w := model.WindowID{Start: 0, End: 100}
	mgr.Ingest(model.NewData("k", 5, 50, 1))
	mgr.Ingest(model.NewData("k", 5, 60, 2))
	wm.Advance(100)
	wm.Confirm()
	results := mgr.MaybeTrigger(wm)
	if len(results) != 1 {
		t.Fatalf("expected one emitted result, got %d", len(results))
	}
	if results[0].Sum != 10 {
		t.Fatalf("unexpected emitted sum: %d", results[0].Sum)
	}

	// A late event for the same window must not overwrite the emitted result.
	mgr.Ingest(model.NewData("k", 7, 90, 3))
	result, ok := store.Result(w)
	if !ok {
		t.Fatalf("window result missing after late event")
	}
	if result.Sum != 10 {
		t.Fatalf("late event overwrote the emitted sum: %d", result.Sum)
	}
	if result.LateSum != 7 {
		t.Fatalf("late event was not accumulated separately: %+v", result)
	}
}
