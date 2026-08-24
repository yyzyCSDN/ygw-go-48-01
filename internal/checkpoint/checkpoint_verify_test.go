package checkpoint_test

import (
	"testing"

	"streamengine/internal/checkpoint"
	"streamengine/internal/sink"
	"streamengine/internal/source"
	"streamengine/internal/state"
)

func TestCheckpointRetryNoOverwrite(t *testing.T) {
	st := state.NewStore()
	sk := sink.New()
	off := source.NewOffsetTracker(0)
	cp := checkpoint.New(st, sk, off, nil)

	for i := int64(1); i <= 8; i++ {
		st.Record("k", i)
		sk.Write("k", i)
	}
	cp.Commit(8)

	var replayed []int64
	replay := func(from int64) error {
		for o := from + 1; o <= 10; o++ {
			replayed = append(replayed, o)
		}
		return nil
	}
	if err := cp.Retry(6, replay); err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	for _, offset := range []int64{7, 8} {
		for _, replayedOffset := range replayed {
			if replayedOffset == offset {
				t.Fatalf("retry replayed already-committed offset %d (replayed=%v)", offset, replayed)
			}
		}
	}
}
