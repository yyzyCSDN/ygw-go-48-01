package window

import (
	"sort"
	"sync"

	"streamengine/internal/model"
	"streamengine/internal/watermark"
)

// ResultSink is implemented by the aggregation store. The window manager
// hands a closed window's values to the sink when the window fires, and the
// sink decides how late events are accumulated.
type ResultSink interface {
	EmitResult(w model.WindowID, values []int64) (*model.WindowResult, bool)
	LateUpdate(w model.WindowID, value int64)
	Update(w model.WindowID, value int64)
}

// IngestOutcome tells the caller what the manager did with an event.
type IngestOutcome int

const (
	OutcomeAccepted IngestOutcome = iota
	OutcomeLate
)

// managedWindow tracks one tumbling window's lifecycle.
type managedWindow struct {
	state  model.WindowState
	values []int64
}

// Manager owns tumbling windows and coordinates their trigger with the
// confirmed watermark. A window is only closed after the watermark confirms
// the interval boundary, so events that are still in flight when the boundary
// is reached are ingested before the close.
type Manager struct {
	mu      sync.Mutex
	size    int64
	sink    ResultSink
	windows map[model.WindowID]*managedWindow
	catalog *Catalog
	kind    model.WindowType
}

// NewManager creates a tumbling window manager with the given interval size.
func NewManager(size int64, sink ResultSink) *Manager {
	return &Manager{
		size:    size,
		sink:    sink,
		windows: make(map[model.WindowID]*managedWindow),
		catalog: NewCatalog(),
		kind:    model.WindowTumbling,
	}
}

// WindowFor computes the tumbling window that owns the given timestamp.
func (m *Manager) WindowFor(ts int64) model.WindowID {
	start := ts / m.size * m.size
	return model.WindowID{Start: start, End: start + m.size}
}

// Ingest folds one data event into its tumbling window. Events for already
// closed windows are still routed into the shared result through the regular
// update path, which can overwrite the published aggregate.
func (m *Manager) Ingest(ev model.Event) IngestOutcome {
	w := m.WindowFor(ev.Timestamp)
	m.mu.Lock()
	defer m.mu.Unlock()
	mw, ok := m.windows[w]
	if !ok {
		mw = &managedWindow{state: model.WindowOpen}
		m.windows[w] = mw
		m.catalog.Open(w, ev.Timestamp)
	}
	if mw.state == model.WindowClosed {
		m.sink.Update(w, ev.Value)
		m.catalog.RecordLate(w)
		return OutcomeLate
	}
	mw.values = append(mw.values, ev.Value)
	m.catalog.RecordEvent(w, ev.Timestamp)
	return OutcomeAccepted
}

// MaybeTrigger closes every open window whose end is not greater than the
// confirmed watermark. The confirmed value is used rather than the tentative
// frontier so an early advance cannot close a window while the final events of
// the interval are still being handed off.
func (m *Manager) MaybeTrigger(tracker *watermark.Tracker) []model.WindowResult {
	confirmed := tracker.Confirmed()
	m.mu.Lock()
	defer m.mu.Unlock()
	var emitted []model.WindowResult
	for w, mw := range m.windows {
		if w.End > confirmed || !mw.state.CanClose() {
			continue
		}
		mw.state = model.WindowTriggering
		result, ok := m.sink.EmitResult(w, mw.values)
		if ok {
			emitted = append(emitted, *result)
		}
		mw.state = model.WindowClosing
		mw.state = model.WindowClosed
		m.catalog.Close(w, confirmed)
	}
	sort.Slice(emitted, func(i, j int) bool { return emitted[i].Window.Start < emitted[j].Window.Start })
	return emitted
}

// OpenCount returns how many windows are not yet closed.
func (m *Manager) OpenCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for _, mw := range m.windows {
		if mw.state != model.WindowClosed {
			count++
		}
	}
	return count
}

// Snapshot returns a stable view of every known window and its state.
func (m *Manager) Snapshot() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]string, len(m.windows))
	for w, mw := range m.windows {
		out[w.String()] = mw.state.String()
	}
	return out
}

// CatalogSummary returns the monitored lifecycle of every window.
func (m *Manager) CatalogSummary() []CatalogEntry {
	return m.catalog.Summary()
}

// Kind returns the window family managed by this instance.
func (m *Manager) Kind() model.WindowType {
	return m.kind
}
