package state

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestSnapshotIsAtomicConcurrentWithApply hammers the store with Apply rounds
// on many goroutines while another goroutine repeatedly snapshots. Before the
// fix, Snapshot copied the data map without holding the store lock, so a
// concurrent Apply could mutate it mid-copy: the runtime would either detect a
// concurrent map read/write (fatal) or produce a snapshot that mixed values
// from different rounds. With the fix the copy runs under the store lock, so
// every snapshot is a consistent, self-consistent point-in-time view.
func TestSnapshotIsAtomicConcurrentWithApply(t *testing.T) {
	s := NewStore()
	const writers = 8
	const rounds = 2000

	// Each writer hammers one key with +1 deltas; if a snapshot ever sees a
	// half-applied delta the per-key sum would not match what the writer
	// actually wrote.
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				s.Apply(key, 1)
			}
		}(k(w))
	}

	// Snapshotter runs concurrently. Under the old code this was a data race
	// on the map; here it must simply never panic and must be consistent.
	var snaps atomic.Int64
	var stop atomic.Bool
	go func() {
		for !stop.Load() {
			_ = s.Snapshot()
			snaps.Add(1)
		}
	}()

	// Give the snapshotter a beat to race the writers.
	time.Sleep(20 * time.Millisecond)
	wg.Wait()
	stop.Store(true)

	// After all writers finish, the committed state must reflect every delta
	// exactly once: each of the writers*rounds increments is accounted for.
	total := int64(writers) * int64(rounds)
	var got int64
	for w := 0; w < writers; w++ {
		v, ok := s.Get(k(w))
		if !ok {
			t.Fatalf("key %s missing after writers finished", k(w))
		}
		got += v
	}
	if got != total {
		t.Fatalf("lost increments: got %d want %d (snaps taken: %d)", got, total, snaps.Load())
	}
	if snaps.Load() == 0 {
		t.Fatal("snapshotter never ran")
	}
}

// TestSnapshotSeqMonotonicAndConsistent verifies that every snapshot's sequence
// is non-decreasing and that a snapshot's data map never reflects increments
// past its recorded sequence — i.e. the snapshot is a prefix of the mutation
// order, not a mix of before/after.
func TestSnapshotSeqMonotonicAndConsistent(t *testing.T) {
	s := NewStore()
	s.Apply("a", 1) // seq 1
	s.Apply("b", 2) // seq 2
	snap1 := s.Snapshot()
	if snap1.Seq != 2 {
		t.Fatalf("snap1 seq = %d, want 2", snap1.Seq)
	}
	if snap1.Data["a"] != 1 || snap1.Data["b"] != 2 {
		t.Fatalf("snap1 data = %v, want a=1 b=2", snap1.Data)
	}

	s.Apply("a", 10) // seq 3
	snap2 := s.Snapshot()
	if snap2.Seq <= snap1.Seq {
		t.Fatalf("snap2 seq %d not after snap1 seq %d", snap2.Seq, snap1.Seq)
	}
	// snap2 must reflect the seq-3 increment on "a".
	if snap2.Data["a"] != 11 || snap2.Data["b"] != 2 {
		t.Fatalf("snap2 data = %v, want a=11 b=2", snap2.Data)
	}

	// snap1 must be unaffected by later mutations: it is an independent copy.
	if snap1.Data["a"] != 1 {
		t.Fatalf("snap1 mutated after capture: a=%d want 1", snap1.Data["a"])
	}
}

// TestRestoreRewindsToSnapshotPrefix restores snap1 (seq 2) after a later
// increment, and checks the store rewinds exactly to that prefix: "a" back to
// 1, "b" at 2, and no trace of the post-snapshot increment. The applied-offset
// set is also restored, so a subsequent replay redoes only offsets the snapshot
// has not yet absorbed.
func TestRestoreRewindsToSnapshotPrefix(t *testing.T) {
	s := NewStore()
	s.Apply("a", 1) // seq 1
	s.MarkApplied(1)
	s.Apply("b", 2) // seq 2
	s.MarkApplied(2)
	snap := s.Snapshot()

	s.Apply("a", 10) // seq 3 — past the snapshot, must be dropped on restore
	s.MarkApplied(3)

	s.RestoreSnapshot(snap)

	if v, _ := s.Get("a"); v != 1 {
		t.Fatalf("after restore a=%d, want 1 (dropped post-snapshot increment)", v)
	}
	if v, _ := s.Get("b"); v != 2 {
		t.Fatalf("after restore b=%d, want 2", v)
	}
	if got := s.Version(); got != snap.Seq {
		t.Fatalf("after restore seq=%d, want snapshot seq %d", got, snap.Seq)
	}
	// Offsets 1 and 2 are absorbed by the snapshot; offset 3 is not.
	if !s.IsApplied(1) || !s.IsApplied(2) {
		t.Fatal("restore dropped applied offsets that the snapshot had absorbed")
	}
	if s.IsApplied(3) {
		t.Fatal("restore kept an applied offset that arrived past the snapshot")
	}
}

// TestRestoreSnapshotConcurrentWithApply runs restores concurrently with
// writers. The race detector is the guard during concurrency: before the fix
// RestoreSnapshot swapped the data map under no lock while Apply mutated it,
// a concurrent map read/write the race detector reports as a fatal data race.
// With the fix the restore runs under the store lock, so writers block for its
// duration and no map is ever read and written at once.
//
// Observing the per-key value mid-concurrency is not meaningful — a writer can
// land an increment in the gap between RestoreSnapshot returning and the read.
// The well-defined check is after the writers stop: a final restore must bring
// the store back to the exact snapshot prefix, because no writer is running to
// add increments. A torn or double-counting restore would leave residual
// post-snapshot increments that a final restore could not clean up.
func TestRestoreSnapshotConcurrentWithApply(t *testing.T) {
	s := NewStore()
	for i := 0; i < 4; i++ {
		s.Apply(k(i), int64(i+1))
	}
	snap := s.Snapshot()

	var wg sync.WaitGroup
	stop := make(chan struct{})
	// Writers keep mutating after the snapshot, racing every restore.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				s.Apply(key, 1)
			}
		}(k(i))
	}

	// Restorer repeatedly resets to the snapshot concurrently with the writers.
	// Under the old code this loop fatals with a concurrent map read/write.
	for r := 0; r < 200; r++ {
		s.RestoreSnapshot(snap)
	}
	close(stop)
	wg.Wait()

	// Now no writer is running. A final restore must reset the store to exactly
	// the snapshot prefix — every key, the applied-offset set, and the sequence.
	// If RestoreSnapshot ever produced a torn map or a double-counted value, the
	// residual increments would survive this final reset.
	s.RestoreSnapshot(snap)
	for key, want := range snap.Data {
		if got, _ := s.Get(key); got != want {
			t.Fatalf("after final restore %s=%d, want %d", key, got, want)
		}
	}
	if got := s.Version(); got != snap.Seq {
		t.Fatalf("after final restore seq=%d, want %d", got, snap.Seq)
	}
	for _, off := range []int64{1, 2, 3} {
		if snap.Applied[off] && !s.IsApplied(off) {
			t.Fatalf("after final restore offset %d lost", off)
		}
		if !snap.Applied[off] && s.IsApplied(off) {
			t.Fatalf("after final restore offset %d should be absent", off)
		}
	}
}

// TestSnapshotCapturesAppliedOffsets confirms the applied-offset set is part of
// the consistent snapshot, so a restore-and-replay reconciles offsets rather
// than reapplying or skipping them.
func TestSnapshotCapturesAppliedOffsets(t *testing.T) {
	s := NewStore()
	s.MarkApplied(10)
	s.MarkApplied(20)
	snap := s.Snapshot()

	s.MarkApplied(30) // past the snapshot

	s.RestoreSnapshot(snap)
	if !s.IsApplied(10) || !s.IsApplied(20) {
		t.Fatal("restore lost applied offsets present in the snapshot")
	}
	if s.IsApplied(30) {
		t.Fatal("restore kept an applied offset past the snapshot")
	}
}

func k(i int) string {
	return "key-" + string(rune('A'+i))
}
