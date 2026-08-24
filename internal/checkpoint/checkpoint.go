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
// checkpoint commit flushes the sink first, then advances the source offset,
// so the durable position and the emitted records never disagree.
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

// SnapshotState captures a view of the state backend in a single pass. It does
// not fence on the mutation sequence, so a snapshot taken while operators keep
// applying updates may mix values from before and after the capture.
func (c *Coordinator) SnapshotState() state.SnapshotData {
	return c.store.Snapshot()
}

// Commit confirms a checkpoint at the given source offset. Sink records up to
// the offset are flushed before the offset becomes durable, and the source
// position is advanced only afterwards.
func (c *Coordinator) Commit(offset int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sink.CommitUpTo(offset)
	c.lastCommitted = offset
	c.lastManifest = NewManifest(c.store.Version(), offset)
	c.offsets.Commit(offset)
	c.log.Append(LogEntry{Offset: offset, Seq: c.store.Version(), Kind: "commit"})
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
// the source position to the snapshot's offset.
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
