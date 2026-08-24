package checkpoint

import (
	"sync"

	"streamengine/internal/model"
	"streamengine/internal/sink"
	"streamengine/internal/source"
	"streamengine/internal/state"
)

// ReplayFunc re-reads source events starting after the given offset and feeds
// them through the pipeline. The checkpoint layer chooses the starting point.
type ReplayFunc func(from int64) error

// Coordinator aligns checkpoints with the state backend and the sink. A
// checkpoint is a single barrier: the state snapshot and the sink flush are
// committed together against one snapshot sequence, and the durable offset is
// advanced only afterwards, so the restored state and the emitted records can
// never disagree about which prefix of the input has been applied.
type Coordinator struct {
	mu            sync.Mutex
	store         *state.Store
	sink          *sink.Sink
	offsets       *source.OffsetTracker
	metrics       *model.Metrics
	log           *Log
	lastCommitted int64
	attempts      int
	lastManifest  Manifest
}

// New creates a checkpoint coordinator.
func New(store *state.Store, sk *sink.Sink, offsets *source.OffsetTracker, metrics *model.Metrics) *Coordinator {
	return &Coordinator{
		store:   store,
		sink:    sk,
		offsets: offsets,
		metrics: metrics,
		log:     NewLog(32),
	}
}

// SnapshotState captures an atomic, self-consistent view of the state backend.
// The store holds its lock for the whole copy, fencing out concurrent Apply
// rounds for the duration, so the returned snapshot is a faithful prefix of
// the mutation sequence rather than a mix of values from different rounds.
func (c *Coordinator) SnapshotState() state.SnapshotData {
	return c.store.Snapshot()
}

// Commit confirms a checkpoint at the given source offset against a specific
// snapshot. Sink records up to the offset are flushed before the offset becomes
// durable, and the source position is advanced only afterwards. The manifest
// is built from the snapshot's sequence, not from a fresh read of the store, so
// the durable checkpoint describes exactly the state that was captured — never a
// later sequence that already absorbed increments beyond the snapshot.
func (c *Coordinator) Commit(offset int64, snap state.SnapshotData) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sink.CommitUpTo(offset)
	c.lastCommitted = offset
	c.lastManifest = NewManifest(snap.Seq, offset)
	c.offsets.Commit(offset)
	c.log.Append(LogEntry{Offset: offset, Seq: snap.Seq, Kind: "commit"})
	if c.metrics != nil {
		c.metrics.AddCheckpoint()
	}
}

// Retry resumes after a failed checkpoint. The replay always starts from the
// last committed offset so already-committed work is never replayed over newer
// results.
func (c *Coordinator) Retry(failedAttemptOffset int64, replay ReplayFunc) error {
	c.mu.Lock()
	from := c.lastCommitted
	c.attempts++
	c.mu.Unlock()
	return replay(from)
}

// Restore applies a checkpoint snapshot to the state backend, then advances
// the source position to the snapshot's offset. The store lock fences out
// concurrent Apply rounds during the replace, and the manifest is built from
// the snapshot sequence so the durable checkpoint tracks the restored state.
func (c *Coordinator) Restore(snap state.SnapshotData, offset int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.store.RestoreSnapshot(snap)
	c.lastCommitted = offset
	manifest := NewManifest(snap.Seq, offset)
	if manifest.Valid() {
		c.lastManifest = manifest
	}
	c.offsets.Commit(offset)
	c.log.Append(LogEntry{Offset: offset, Seq: snap.Seq, Kind: "restore"})
	if c.metrics != nil {
		c.metrics.AddRestore()
	}
}

// LastCommitted returns the last confirmed checkpoint offset.
func (c *Coordinator) LastCommitted() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastCommitted
}

// Attempts returns how many retries have been issued.
func (c *Coordinator) Attempts() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.attempts
}

// LastManifest returns the most recently committed or restored checkpoint.
func (c *Coordinator) LastManifest() Manifest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastManifest
}

// RecentLog returns the retained checkpoint history.
func (c *Coordinator) RecentLog() []LogEntry {
	return c.log.Recent()
}
