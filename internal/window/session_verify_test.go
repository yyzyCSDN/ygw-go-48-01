package window_test

import (
	"sync"
	"testing"

	"streamengine/internal/agg"
	"streamengine/internal/model"
	"streamengine/internal/window"
)

func TestSessionWindowTimeoutNoSplit(t *testing.T) {
	store := agg.NewStore()
	sm := window.NewSessionManager(10, store)

	sm.Touch("u", 100, 1, true)
	sm.Touch("u", 105, 2, true)

	// Parallel churn on an unrelated key exercises the timeout scanner while
	// events keep arriving.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			sm.Touch("other", 50+int64(i%20), int64(i), true)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			sm.ScanTimeout(80)
		}
	}()
	wg.Wait()

	// The session received an event at 105 and must survive a scan at 111.
	sm.ScanTimeout(111)
	if !sm.IsActive("u") {
		t.Fatalf("session was split although an event arrived inside the live gap")
	}
	if result, ok := store.Result(model.WindowID{Start: 100, End: 110, Key: "u"}); ok {
		t.Fatalf("live session was closed and emitted: %+v", result)
	}
}
