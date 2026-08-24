package topology

import (
	"sort"
	"sync"

	"streamengine/internal/agg"
	"streamengine/internal/checkpoint"
	"streamengine/internal/model"
	"streamengine/internal/sink"
	"streamengine/internal/source"
	"streamengine/internal/state"
	"streamengine/internal/watermark"
	"streamengine/internal/window"
)

// Pipeline wires the source, watermark, windows, aggregation, state backend,
// checkpoint coordinator and sink into one runnable chain. It implements the
// watermark observer so every confirmed advance drives window triggering.
type Pipeline struct {
	Source        *source.Source
	Tracker       *watermark.Tracker
	Fanout        *watermark.Fanout
	Idle          *watermark.IdleDetector
	Windows       *window.Manager
	Sessions      *window.SessionManager
	Agg           *agg.Store
	Combiner      *agg.Combiner
	State         *state.Store
	Sink          *sink.Sink
	Checkpoint    *checkpoint.Coordinator
	Offsets       *source.OffsetTracker
	Router        *Router
	PartStore     *agg.PartitionedStore
	Metrics       *model.Metrics
	Topology      *Manager
	deltaMu       sync.Mutex
	pendingDeltas map[string]int64
	flushEvery    int
}

// NewPipeline constructs the full engine chain from an engine configuration.
func NewPipeline(cfg model.EngineConfig, metrics *model.Metrics) *Pipeline {
	aggStore := agg.NewStore()
	windows := window.NewManager(cfg.WindowSizeMS, aggStore)
	sessions := window.NewSessionManager(cfg.SessionTimeout, aggStore)
	st := state.NewStore()
	sk := sink.New()
	offsets := source.NewOffsetTracker(0)
	cp := checkpoint.New(st, sk, offsets, metrics)
	partStore := agg.NewPartitionedStore(cfg.Partitions)
	router := NewRouter(cfg.Partitions, partStore)
	tracker := watermark.NewTracker(0)
	fanout := watermark.NewFanout()
	p := &Pipeline{
		Tracker:       tracker,
		Fanout:        fanout,
		Idle:          watermark.NewIdleDetector(cfg.IdleTimeoutMS),
		Windows:       windows,
		Sessions:      sessions,
		Agg:           aggStore,
		Combiner:      agg.NewCombiner(),
		State:         st,
		Sink:          sk,
		Checkpoint:    cp,
		Offsets:       offsets,
		Router:        router,
		PartStore:     partStore,
		Metrics:       metrics,
		pendingDeltas: make(map[string]int64),
		flushEvery:    8,
	}
	p.Topology = NewManager(st, p.processRecord)
	p.Topology.AddOperator(model.OperatorSpec{Name: "window-aggregate", Parallelism: cfg.Partitions, Stateful: true})
	p.Topology.AddOperator(model.OperatorSpec{Name: "session-track", Parallelism: cfg.Partitions, Stateful: true})
	p.Topology.AddOperator(model.OperatorSpec{Name: "sink-commit", Parallelism: 1, Stateful: false})
	_ = p.Topology.Connect("window-aggregate", "sink-commit")
	_ = p.Topology.Connect("session-track", "sink-commit")
	src := source.New(source.NewBuffer(cfg.BufferSize, nil), p.Topology.Process, tracker)
	p.Source = src
	p.Fanout.Add(p)
	return p
}

// Process accepts one ordered event from the source.
func (p *Pipeline) Process(ev model.Event) {
	if p.Metrics != nil {
		p.Metrics.AddIngested()
	}
	p.Idle.NoteEvent(ev.Timestamp)
	if ev.Kind != model.KindData {
		p.Sessions.Touch(ev.Key, ev.Timestamp, ev.Value, false)
		return
	}
	part := p.Router.Route(ev.Key)
	p.PartStore.Add(part, ev.Key, ev.Value)
	p.State.Record(ev.Key, ev.Value)
	outcome := p.Windows.Ingest(ev)
	if p.Metrics != nil && outcome == window.OutcomeAccepted {
		p.Metrics.AddAggregated()
	} else if p.Metrics != nil {
		p.Metrics.AddLateDropped()
	}
	p.Sessions.Touch(ev.Key, ev.Timestamp, ev.Value, true)
	p.Sink.Write(ev.Key, ev.Value)
	p.maybeFlushDeltas(ev)
}

func (p *Pipeline) maybeFlushDeltas(ev model.Event) {
	p.deltaMu.Lock()
	defer p.deltaMu.Unlock()
	p.pendingDeltas[ev.Key] += ev.Value
	if len(p.pendingDeltas) < p.flushEvery {
		return
	}
	batch := make(map[string]int64, len(p.pendingDeltas))
	for key, delta := range p.pendingDeltas {
		batch[key] = delta
	}
	p.State.ApplyRound(batch)
	p.pendingDeltas = make(map[string]int64)
}

func (p *Pipeline) processRecord(ev model.Event) {
	p.Process(ev)
}

// TriggerOnWatermark fires windows whose interval ended at or before the
// confirmed watermark and closes expired sessions.
func (p *Pipeline) TriggerOnWatermark(confirmed int64) {
	results := p.Windows.MaybeTrigger(p.Tracker)
	p.Sessions.ScanTimeout(confirmed)
	for range results {
		if p.Metrics != nil {
			p.Metrics.AddEmitted()
		}
	}
}

// OnWatermarkConfirmed implements watermark.Observer.
func (p *Pipeline) OnWatermarkConfirmed(confirmed int64) {
	p.TriggerOnWatermark(confirmed)
}

// Advance advances the source watermark and notifies observers.
func (p *Pipeline) Advance(ts int64) {
	p.Source.AdvanceTo(ts)
	p.Fanout.Notify(p.Tracker.Confirmed())
}

// RunCheckpoint snapshots the state backend and commits the sink barrier.
func (p *Pipeline) RunCheckpoint(offset int64) {
	snap := p.Checkpoint.SnapshotState()
	p.Checkpoint.Commit(offset)
	_ = snap
}

// RetryCheckpoint re-runs the checkpoint retry path with a caller-supplied
// replay callback; the retry always starts from the last committed offset.
func (p *Pipeline) RetryCheckpoint(attemptOffset int64, replay checkpoint.ReplayFunc) error {
	return p.Checkpoint.Retry(attemptOffset, replay)
}

// EmittedResults returns every emitted window result merged by window and
// sorted by window identity.
func (p *Pipeline) EmittedResults() []model.WindowResult {
	out := p.Combiner.Combine(p.Agg.Summary())
	sort.Slice(out, func(i, j int) bool { return out[i].Window.String() < out[j].Window.String() })
	return out
}

// Status assembles a monitoring snapshot for the HTTP page.
func (p *Pipeline) Status() map[string]any {
	maxKeyPartitions := 0
	for _, key := range p.PartStore.Keys() {
		if count := p.PartStore.PartitionCount(key); count > maxKeyPartitions {
			maxKeyPartitions = count
		}
	}
	return map[string]any{
		"watermark":           p.Tracker.Snapshot(),
		"watermark_current":   p.Tracker.Current(),
		"windows":             p.Windows.Snapshot(),
		"window_kind":         p.Windows.Kind().String(),
		"sessions":            p.Sessions.Snapshot(),
		"sink":                p.Sink.Snapshot(),
		"sink_pending":        p.Sink.PendingCount(),
		"sink_committed":      p.Sink.CommittedCount(),
		"sink_records":        len(p.Sink.CommittedRecords()),
		"sink_last_seq":       p.Sink.LastCommittedSeq(),
		"state_keys":          p.State.LatestCount(),
		"state_applied":       p.State.AppliedCount(),
		"partitions":          p.Router.RoutingState(),
		"max_key_partitions":  maxKeyPartitions,
		"key_total":           p.PartStore.Total("checkout"),
		"operators":           p.Topology.InstanceStates(),
		"node_window":         p.Topology.NodeInfo("window-aggregate"),
		"dag":                 p.Topology.DAGDescribe(),
		"buffered":            p.Topology.BufferedCount(),
		"paused":              p.Topology.Paused(),
		"open_windows":        p.Windows.OpenCount(),
		"session_count":       p.Sessions.SessionCount(),
		"session_active":      p.Sessions.IsActive("checkout"),
		"result_count":        p.Agg.ResultCount(),
		"emitted_results":     len(p.EmittedResults()),
		"drained":             p.Source.DrainedCount(),
		"source_last_offset":  p.Source.LastOffset(),
		"watermark_lag":       p.Tracker.Lag(),
		"metrics":             p.Metrics.Snapshot(),
		"last_offset":         p.Offsets.Committed(),
		"idle_fired":          p.Idle.Fired(),
		"checkpoint_log":      p.Checkpoint.RecentLog(),
		"checkpoint_attempts": p.Checkpoint.Attempts(),
		"last_manifest":       p.Checkpoint.LastManifest(),
		"window_catalog":      p.Windows.CatalogSummary(),
		"backpressure":        p.Sink.Backpressure(),
	}
}
