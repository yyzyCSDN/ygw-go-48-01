package source

import (
	"sync"

	"streamengine/internal/model"
	"streamengine/internal/watermark"
)

// IngestFunc receives a fully ordered event from the source buffer and feeds it
// to the downstream pipeline.
type IngestFunc func(ev model.Event)

// Source owns the input buffer and the watermark handshake. Events arrive out
// of order, are reordered by event time, and are only handed downstream once
// the watermark has advanced past their timestamp. The pending counter tracks
// events that have been emitted to the pipeline but not yet acknowledged, so
// the watermark confirm never races an in-flight handoff.
type Source struct {
	mu      sync.Mutex
	cond    *sync.Cond
	buffer  *Buffer
	ingest  IngestFunc
	tracker *watermark.Tracker
	pending int
	lastOff int64
	stopped bool
	drained int64
}

// New creates a source over the given reorder buffer and pipeline callback.
func New(buffer *Buffer, ingest IngestFunc, tracker *watermark.Tracker) *Source {
	s := &Source{
		buffer:  buffer,
		ingest:  ingest,
		tracker: tracker,
		lastOff: -1,
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// Emit accepts one raw event into the out-of-order buffer.
func (s *Source) Emit(ev model.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return
	}
	s.buffer.Add(ev)
	s.lastOff = ev.Offset
}

// AdvanceTo drains every buffered event with a timestamp not greater than ts,
// waits for in-flight handoffs to finish, and then advances and confirms the
// watermark. Draining before confirmation is what keeps window triggers from
// closing while the last events of an interval are still in transit.
func (s *Source) AdvanceTo(ts int64) {
	s.mu.Lock()
	for !s.buffer.Empty() && s.buffer.MinTimestamp() <= ts {
		ev := s.buffer.PopMin()
		s.pending++
		s.mu.Unlock()
		s.ingest(ev)
		s.mu.Lock()
		s.pending--
		s.cond.Broadcast()
	}
	for s.pending > 0 {
		s.cond.Wait()
	}
	s.mu.Unlock()
	s.tracker.Advance(ts)
	s.tracker.Confirm()
	s.drained++
}

// LastOffset returns the highest offset ever accepted by the source.
func (s *Source) LastOffset() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastOff
}

// DrainedCount returns how many watermark advances completed.
func (s *Source) DrainedCount() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.drained
}

// Stop disables further ingestion.
func (s *Source) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopped = true
	s.cond.Broadcast()
}
