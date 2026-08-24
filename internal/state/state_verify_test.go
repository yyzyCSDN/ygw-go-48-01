package state_test

import (
	"sync"
	"testing"

	"streamengine/internal/checkpoint"
	"streamengine/internal/sink"
	"streamengine/internal/source"
	"streamengine/internal/state"
)

func TestStateIncrementalMergeKeepsKeys(t *testing.T) {
	st := state.NewStore()
	sk := sink.New()
	off := source.NewOffsetTracker(0)
	cp := checkpoint.New(st, sk, off, nil)

	st.Apply("a", 1)
	st.Apply("a2", 2)
	snap := st.Snapshot()

	// The checkpoint snapshot also carries an increment journal captured while
	// operators kept merging keys.
	snapWithPending := state.SnapshotData{
		Data:    snap.Data,
		Pending: map[string]int64{"c": 3, "c2": 4},
		Seq:     snap.Seq,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		st.Apply("b", 5)
		st.Apply("b2", 6)
	}()
	cp.Restore(snapWithPending, 9)
	wg.Wait()

	for _, key := range []string{"a", "a2", "b", "b2", "c", "c2"} {
		if _, ok := st.Get(key); !ok {
			t.Fatalf("key %s lost after incremental merge restore", key)
		}
	}
	if value, _ := st.Get("b"); value != 5 {
		t.Fatalf("live increment value wrong after restore: %d", value)
	}
	if value, _ := st.Get("c"); value != 3 {
		t.Fatalf("snapshot journal value wrong after restore: %d", value)
	}
}
