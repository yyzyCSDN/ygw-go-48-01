package state_test

import (
	"fmt"
	"sort"
	"sync"
	"testing"

	"streamengine/internal/checkpoint"
	"streamengine/internal/sink"
	"streamengine/internal/source"
	"streamengine/internal/state"
)

func TestCheckpointConsistentWithUpdates(t *testing.T) {
	st := state.NewStore()
	sk := sink.New()
	off := source.NewOffsetTracker(0)
	cp := checkpoint.New(st, sk, off, nil)

	deltas := make(map[string]int64, 120)
	keys := make([]string, 0, 120)
	for i := 0; i < 120; i++ {
		key := fmt.Sprintf("k%03d", i)
		deltas[key] = 1
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for round := 0; round < 80; round++ {
			st.ApplyRound(deltas)
		}
	}()

	inconsistent := false
	checked := false
	for i := 0; i < 300 && !inconsistent; i++ {
		snap := cp.SnapshotState()
		if len(snap.Data) < len(keys) {
			// No complete ApplyRound has been captured yet.
			continue
		}
		checked = true
		first := int64(-1)
		for _, key := range keys {
			value, ok := snap.Data[key]
			if !ok {
				inconsistent = true
				break
			}
			if first == -1 {
				first = value
			} else if value != first {
				inconsistent = true
				break
			}
		}
	}
	wg.Wait()
	if inconsistent {
		t.Fatalf("checkpoint snapshot mixed values from different update rounds")
	}
	if !checked {
		t.Fatalf("no complete update round was ever observed")
	}
}
