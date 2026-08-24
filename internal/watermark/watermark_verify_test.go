package watermark_test

import (
	"testing"

	"streamengine/internal/agg"
	"streamengine/internal/model"
	"streamengine/internal/watermark"
	"streamengine/internal/window"
)

func TestWatermarkNoRegressRepeatTrigger(t *testing.T) {
	store := agg.NewStore()
	wm := watermark.NewTracker(0)
	mgr := window.NewManager(100, store)

	w := model.WindowID{Start: 0, End: 100}
	mgr.Ingest(model.NewData("k", 3, 50, 1))
	wm.Advance(100)
	wm.Confirm()
	first := mgr.MaybeTrigger(wm)
	if len(first) != 1 {
		t.Fatalf("first trigger did not fire the window")
	}

	// A regressive advance must not allow the already-fired window to fire
	// again once the watermark climbs back above the boundary.
	wm.Advance(80)
	wm.Confirm()
	wm.Advance(120)
	wm.Confirm()
	second := mgr.MaybeTrigger(wm)
	if len(second) != 0 {
		t.Fatalf("already-fired window fired again after watermark regress: %d results", len(second))
	}
	result, ok := store.Result(w)
	if !ok {
		t.Fatalf("window result missing")
	}
	if result.Count != 1 {
		t.Fatalf("duplicate aggregation after regress: count=%d", result.Count)
	}
}
