package topology

import (
	"reflect"
	"testing"

	"streamengine/internal/model"
	"streamengine/internal/state"
)

// captureApply returns an ApplyFunc that records the offsets of every event it
// receives, in arrival order, so a test can assert exactly which records the
// downstream chain saw and in which order.
func captureApplied() (ApplyFunc, *[]int64) {
	var seen []int64
	return func(ev model.Event) { seen = append(seen, ev.Offset) }, &seen
}

func mkEvent(offset int64) model.Event {
	return model.NewData("k", int64(offset), int64(offset)*10, offset)
}

// TestResumeReplaysEveryBufferedRecordInOrder asserts that Resume replays the
// full buffer from the first entry. The original implementation started replay
// at index 1, silently dropping the first buffered record.
func TestResumeReplaysEveryBufferedRecordInOrder(t *testing.T) {
	apply, seen := captureApplied()
	m := NewManager(state.NewStore(), apply)
	m.AddOperator(model.OperatorSpec{Name: "op", Parallelism: 1, Stateful: true})

	m.Pause()
	for _, off := range []int64{1, 2, 3, 4} {
		m.Process(mkEvent(off)) // buffered while paused
	}
	replayed := m.Resume()

	if want := []int64{1, 2, 3, 4}; !reflect.DeepEqual(*seen, want) {
		t.Fatalf("downstream saw %v, want every record in buffer order %v", *seen, want)
	}
	gotOffsets := make([]int64, len(replayed))
	for i, ev := range replayed {
		gotOffsets[i] = ev.Offset
	}
	if want := []int64{1, 2, 3, 4}; !reflect.DeepEqual(gotOffsets, want) {
		t.Fatalf("Resume returned %v, want %v", gotOffsets, want)
	}
}

// TestResumeSkipsRecordAppliedDuringPause asserts that a record applied via
// ApplyNow while paused is not replayed again by Resume. The original
// implementation reconciled against a stale pause-time snapshot, so the same
// record was applied twice.
func TestResumeSkipsRecordAppliedDuringPause(t *testing.T) {
	apply, seen := captureApplied()
	m := NewManager(state.NewStore(), apply)
	m.AddOperator(model.OperatorSpec{Name: "op", Parallelism: 1, Stateful: true})

	m.Pause()
	// Record 2 also enters the buffer through the normal path.
	m.Process(mkEvent(1))
	m.Process(mkEvent(2))
	m.Process(mkEvent(3))
	// Record 2 was completed on another path during the pause.
	m.ApplyNow(mkEvent(2))

	replayed := m.Resume()

	// Each record must reach downstream exactly once. Offset 2 was applied
	// immediately by ApplyNow, so it appears once (early); Resume must not
	// re-apply it from the buffer.
	counts := map[int64]int{}
	for _, off := range *seen {
		counts[off]++
	}
	for _, want := range []int64{1, 2, 3} {
		if counts[want] != 1 {
			t.Fatalf("offset %d applied %d times, want exactly once (downstream saw %v)", want, counts[want], *seen)
		}
	}
	gotOffsets := make([]int64, 0, len(replayed))
	for _, ev := range replayed {
		gotOffsets = append(gotOffsets, ev.Offset)
	}
	if want := []int64{1, 3}; !reflect.DeepEqual(gotOffsets, want) {
		t.Fatalf("Resume returned offsets %v, want only the not-yet-applied records %v", gotOffsets, want)
	}
}

// TestResumePreservesBufferOrderNotOffsetOrder asserts that Resume keeps the
// order in which records entered the buffer rather than reordering by offset.
// The source delivers events in watermark-sorted (event-time) order, which is
// not necessarily ascending offset order; reordering by offset would corrupt
// that order.
func TestResumePreservesBufferOrderNotOffsetOrder(t *testing.T) {
	apply, seen := captureApplied()
	m := NewManager(state.NewStore(), apply)
	m.AddOperator(model.OperatorSpec{Name: "op", Parallelism: 1, Stateful: true})

	m.Pause()
	// Buffered in event-time/arrival order, which here is intentionally not
	// ascending offset order (offset 3 arrives before offset 1).
	for _, off := range []int64{3, 1, 2} {
		m.Process(mkEvent(off))
	}
	m.Resume()

	if want := []int64{3, 1, 2}; !reflect.DeepEqual(*seen, want) {
		t.Fatalf("downstream saw %v, want buffer arrival order preserved %v", *seen, want)
	}
}

// TestProcessNoLongerDoubleAppliesAfterResume asserts that once Resume has
// replayed the buffer, re-processing the same offset does not re-apply it (the
// applied set is the reconciliation truth across pause/resume cycles).
func TestProcessNoLongerDoubleAppliesAfterResume(t *testing.T) {
	apply, seen := captureApplied()
	m := NewManager(state.NewStore(), apply)
	m.AddOperator(model.OperatorSpec{Name: "op", Parallelism: 1, Stateful: true})

	m.Pause()
	m.Process(mkEvent(1))
	m.Resume()
	// The same offset re-entering the (now running) topology is a duplicate.
	m.Process(mkEvent(1))

	if want := []int64{1}; !reflect.DeepEqual(*seen, want) {
		t.Fatalf("downstream saw %v, want offset 1 applied exactly once", *seen)
	}
}
