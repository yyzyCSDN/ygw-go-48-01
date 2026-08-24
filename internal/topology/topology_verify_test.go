package topology_test

import (
	"sort"
	"sync"
	"testing"

	"streamengine/internal/model"
	"streamengine/internal/state"
	"streamengine/internal/topology"
)

func TestTopologyResumeReconcilesBuffer(t *testing.T) {
	st := state.NewStore()
	var mu sync.Mutex
	var applied []int64
	apply := func(ev model.Event) {
		mu.Lock()
		applied = append(applied, ev.Offset)
		mu.Unlock()
		st.Record(ev.Key, ev.Value)
	}
	tp := topology.NewManager(st, apply)

	ev1 := model.NewData("k", 1, 10, 1)
	ev2 := model.NewData("k", 2, 20, 2)
	ev3 := model.NewData("k", 3, 30, 3)

	tp.Pause()
	tp.Process(ev1)
	tp.Process(ev2)
	tp.Process(ev3)
	// This record completed on another path while the topology was paused.
	tp.ApplyNow(ev2)
	tp.Resume()

	mu.Lock()
	counts := make(map[int64]int)
	for _, offset := range applied {
		counts[offset]++
	}
	sorted := append([]int64(nil), applied...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	mu.Unlock()

	if len(applied) != 3 {
		t.Fatalf("expected three applications, got %v", sorted)
	}
	for _, offset := range []int64{1, 2, 3} {
		if counts[offset] != 1 {
			t.Fatalf("record %d applied %d times (all: %v)", offset, counts[offset], sorted)
		}
	}
}
