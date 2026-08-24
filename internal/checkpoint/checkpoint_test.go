package checkpoint

import (
	"testing"

	"streamengine/internal/model"
	"streamengine/internal/sink"
	"streamengine/internal/source"
	"streamengine/internal/state"
)

// newCoordinator builds a coordinator over a fresh state store, sink and
// offset tracker, with metrics attached so the counters are exercised.
func newCoordinator() (*Coordinator, *state.Store) {
	st := state.NewStore()
	sk := sink.New()
	off := source.NewOffsetTracker(0)
	return New(st, sk, off, model.NewMetrics()), st
}

// TestCommitManifestMatchesSnapshot proves the durable checkpoint tracks the
// exact state that was captured. Before the fix the coordinator built the
// manifest from a fresh store.Version() read taken at commit time — which, if
// an operator applied increments between SnapshotState and Commit, described a
// later sequence than the snapshot and so pointed recovery at state the
// snapshot never contained. With the fix the manifest Seq is the snapshot's own
// Seq, passed in by the commit, so the durable checkpoint and the captured
// state are the same prefix of the mutation order.
func TestCommitManifestMatchesSnapshot(t *testing.T) {
	c, st := newCoordinator()

	st.Apply("k", 5) // seq 1
	st.Apply("k", 5) // seq 2
	snap := c.SnapshotState()
	// Simulate an increment that races in after the snapshot but before the
	// commit. The manifest must still describe the snapshot, not seq 3.
	st.Apply("k", 100) // seq 3, past the snapshot

	c.Commit(42, snap)

	m := c.LastManifest()
	if m.Seq != snap.Seq {
		t.Fatalf("manifest seq = %d, want snapshot seq %d (must describe the captured state)", m.Seq, snap.Seq)
	}
	if m.Offset != 42 {
		t.Fatalf("manifest offset = %d, want 42", m.Offset)
	}
	if got := c.LastCommitted(); got != 42 {
		t.Fatalf("last committed = %d, want 42", got)
	}

	entries := c.RecentLog()
	if len(entries) != 1 || entries[0].Seq != snap.Seq || entries[0].Offset != 42 {
		t.Fatalf("log entry = %+v, want seq %d offset 42", entries, snap.Seq)
	}
}

// TestRestoreRealignsStateToCheckpoint restores a snapshot and checks the store
// rewinds to the snapshot prefix and the manifest tracks the restored state.
// This is the "recovered state aligns with operator state" guarantee.
func TestRestoreRealignsStateToCheckpoint(t *testing.T) {
	c, st := newCoordinator()

	st.Apply("a", 3) // seq 1
	st.Apply("b", 7) // seq 2
	snap := c.SnapshotState()
	c.Commit(10, snap)

	// Operator state drifts past the committed checkpoint.
	st.Apply("a", 100) // seq 3
	st.Apply("c", 1)   // seq 4

	// Recover: load the checkpoint snapshot back.
	c.Restore(snap, 10)

	if v, _ := st.Get("a"); v != 3 {
		t.Fatalf("after restore a=%d, want 3 (checkpoint value)", v)
	}
	if v, _ := st.Get("b"); v != 7 {
		t.Fatalf("after restore b=%d, want 7", v)
	}
	if _, ok := st.Get("c"); ok {
		t.Fatal("after restore key c should be absent (arrived past checkpoint)")
	}
	if got := st.Version(); got != snap.Seq {
		t.Fatalf("after restore seq=%d, want %d", got, snap.Seq)
	}
	m := c.LastManifest()
	if m.Seq != snap.Seq || m.Offset != 10 {
		t.Fatalf("after restore manifest = %+v, want seq %d offset 10", m, snap.Seq)
	}
}

// TestSnapshotAndCommitAreOneBarrier runs a snapshot, then mutates the store
// after the snapshot, and commits: the committed state must be the snapshot
// prefix, and the store's live state keeps the post-snapshot increments. This
// shows the snapshot is a barrier — increments after it are not folded into
// the durable checkpoint, and are kept in the live store for a later snapshot.
func TestSnapshotAndCommitAreOneBarrier(t *testing.T) {
	c, st := newCoordinator()

	st.Apply("x", 2) // seq 1
	snap := c.SnapshotState()
	c.Commit(1, snap)

	st.Apply("x", 2) // seq 2, after the committed checkpoint

	// Durable checkpoint reflects only the snapshot prefix.
	m := c.LastManifest()
	if m.Seq != 1 {
		t.Fatalf("checkpoint seq = %d, want 1 (snapshot prefix)", m.Seq)
	}
	// Live store keeps the post-snapshot increment — not lost, just not yet
	// durably checkpointed.
	if v, _ := st.Get("x"); v != 4 {
		t.Fatalf("live state x=%d, want 4 (post-snapshot increment retained)", v)
	}
}

// TestCommitThenSnapshotAdvancesSeq verifies that a checkpoint does not freeze
// the store: a snapshot after a commit and a later increment records the new
// sequence, so successive checkpoints advance over the mutation order.
func TestCommitThenSnapshotAdvancesSeq(t *testing.T) {
	c, st := newCoordinator()

	st.Apply("a", 1) // seq 1
	s1 := c.SnapshotState()
	c.Commit(1, s1)

	st.Apply("a", 1) // seq 2
	s2 := c.SnapshotState()
	c.Commit(2, s2)

	if s2.Seq != 2 {
		t.Fatalf("second snapshot seq = %d, want 2", s2.Seq)
	}
	if c.LastManifest().Seq != 2 {
		t.Fatalf("second commit manifest seq = %d, want 2", c.LastManifest().Seq)
	}
}
