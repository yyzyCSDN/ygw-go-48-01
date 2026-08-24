package model

import "sync/atomic"

// Metrics holds engine-wide counters exposed through the HTTP probe.
type Metrics struct {
	Ingested    atomic.Int64
	Aggregated  atomic.Int64
	Emitted     atomic.Int64
	LateDropped atomic.Int64
	Checkpoints atomic.Int64
	Restores    atomic.Int64
}

// NewMetrics creates an empty counter set.
func NewMetrics() *Metrics {
	return &Metrics{}
}

// AddIngested counts one accepted source event.
func (m *Metrics) AddIngested() {
	m.Ingested.Add(1)
}

// AddAggregated counts one event folded into a window accumulator.
func (m *Metrics) AddAggregated() {
	m.Aggregated.Add(1)
}

// AddEmitted counts one published window result.
func (m *Metrics) AddEmitted() {
	m.Emitted.Add(1)
}

// AddLateDropped counts one late event discarded by a closed window.
func (m *Metrics) AddLateDropped() {
	m.LateDropped.Add(1)
}

// AddCheckpoint counts one committed checkpoint.
func (m *Metrics) AddCheckpoint() {
	m.Checkpoints.Add(1)
}

// AddRestore counts one restore from a checkpoint snapshot.
func (m *Metrics) AddRestore() {
	m.Restores.Add(1)
}

// Snapshot returns a stable view of all counters.
func (m *Metrics) Snapshot() map[string]int64 {
	return map[string]int64{
		"ingested":     m.Ingested.Load(),
		"aggregated":   m.Aggregated.Load(),
		"emitted":      m.Emitted.Load(),
		"late_dropped": m.LateDropped.Load(),
		"checkpoints":  m.Checkpoints.Load(),
		"restores":     m.Restores.Load(),
	}
}
