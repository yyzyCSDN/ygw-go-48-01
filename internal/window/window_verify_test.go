package window_test

import (
	"sync"
	"testing"

	"streamengine/internal/agg"
	"streamengine/internal/model"
	"streamengine/internal/watermark"
	"streamengine/internal/window"
)

func TestWindowNoEarlyTrigger(t *testing.T) {
	store := agg.NewStore()
	wm := watermark.NewTracker(0)
	mgr := window.NewManager(100, store)

	ev1 := model.NewData("k", 5, 50, 1)
	ev2 := model.NewData("k", 7, 95, 2)
	if mgr.Ingest(ev1) != window.OutcomeAccepted {
		t.Fatalf("first event was not accepted")
	}

	// The watermark advances while the second event is still in flight. The
	// trigger must not close the window before that event is ingested.
	var wg sync.WaitGroup
	wg.Add(1)
	ingested := make(chan struct{})
	go func() {
		defer wg.Done()
		<-ingested
		mgr.Ingest(ev2)
	}()
	wm.Advance(100)
	mgr.MaybeTrigger(wm)
	close(ingested)
	wg.Wait()
	wm.Confirm()
	results := mgr.MaybeTrigger(wm)

	if len(results) != 1 {
		t.Fatalf("expected exactly one emitted result, got %d", len(results))
	}
	if results[0].Count != 2 || results[0].Sum != 12 {
		t.Fatalf("window lost an in-flight event: %+v", results[0])
	}
}
