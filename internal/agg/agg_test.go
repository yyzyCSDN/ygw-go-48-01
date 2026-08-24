package agg

import (
	"testing"

	"streamengine/internal/model"
)

// TestStoreEmittedResultImmutableAfterLateUpdate reproduces the reconciliation
// bug: after a window's result has been published by EmitResult, a late event
// routed through Update must not change the values downstream already saw.
// The published Sum/Count/Min/Max must stay frozen; the late value lands in the
// late-only accumulation instead.
func TestStoreEmittedResultImmutableAfterLateUpdate(t *testing.T) {
	store := NewStore()
	w := model.WindowID{Start: 0, End: 100}

	// Pre-emit accumulation.
	store.Update(w, 5)
	store.Update(w, 3)

	// Fire the window: this publishes the aggregate.
	result, ok := store.EmitResult(w, nil)
	if !ok {
		t.Fatalf("EmitResult should publish the first time")
	}
	if result.Sum != 8 || result.Count != 2 || result.Min != 3 || result.Max != 5 {
		t.Fatalf("published aggregate wrong before late event: %+v", result)
	}
	published := result

	// A late event for the already-fired window arrives through the regular
	// update path. It must not overwrite the published values.
	store.Update(w, 100)

	again, ok := store.Result(w)
	if !ok {
		t.Fatalf("Result should still be present")
	}
	if again.Sum != published.Sum || again.Count != published.Count ||
		again.Min != published.Min || again.Max != published.Max {
		t.Fatalf("published aggregate mutated by late event: got %+v want %+v",
			again, published)
	}
	if again.LateCount != 1 || again.LateSum != 100 {
		t.Fatalf("late value not folded into late accumulation: %+v", again)
	}
}

// TestStoreLateUpdateFreezesPublishedAggregate guards the dedicated late path:
// LateUpdate for an already-emitted window must keep the published values frozen.
func TestStoreLateUpdateFreezesPublishedAggregate(t *testing.T) {
	store := NewStore()
	w := model.WindowID{Start: 0, End: 100}

	store.Update(w, 10)
	result, _ := store.EmitResult(w, nil)
	published := result

	store.LateUpdate(w, 7)

	again, _ := store.Result(w)
	if again.Sum != published.Sum || again.Count != published.Count {
		t.Fatalf("LateUpdate mutated published aggregate: got %+v want %+v",
			again, published)
	}
	if again.LateCount != 1 || again.LateSum != 7 {
		t.Fatalf("late value lost: %+v", again)
	}
}

// TestStoreUpdateBeforeEmissionFoldsNormally ensures the immutability guard
// only kicks in after emission; pre-emit values fold into the live aggregate.
func TestStoreUpdateBeforeEmissionFoldsNormally(t *testing.T) {
	store := NewStore()
	w := model.WindowID{Start: 0, End: 100}

	store.Update(w, 4)
	store.Update(w, 6)

	r, _ := store.Result(w)
	if r.Sum != 10 || r.Count != 2 || r.Min != 4 || r.Max != 6 {
		t.Fatalf("pre-emit fold wrong: %+v", r)
	}
}
