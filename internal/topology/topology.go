package topology

import (
	"fmt"
	"sort"
	"sync"

	"streamengine/internal/model"
	"streamengine/internal/state"
)

// ApplyFunc applies one ordered event to the downstream operator chain.
type ApplyFunc func(ev model.Event)

// Manager schedules operators and owns the pause/resume buffer. While paused,
// incoming events accumulate in the buffer; on resume the buffer is reconciled
// against the applied offsets tracked in the state backend so records are
// neither replayed twice nor skipped.
type Manager struct {
	mu        sync.Mutex
	state     *state.Store
	apply     ApplyFunc
	instances map[string]*model.OperatorInstance
	buffer    []model.Event
	paused    bool
	dag       *DAG
}

// NewManager creates an operator manager that applies records through the
// given callback and tracks applied offsets in the state backend.
func NewManager(st *state.Store, apply ApplyFunc) *Manager {
	return &Manager{
		state:     st,
		apply:     apply,
		instances: make(map[string]*model.OperatorInstance),
		dag:       NewDAG(),
	}
}

// AddOperator registers an operator instance in the topology.
func (m *Manager) AddOperator(spec model.OperatorSpec) *model.OperatorInstance {
	m.mu.Lock()
	defer m.mu.Unlock()
	instance := model.NewOperatorInstance(spec)
	m.instances[spec.Name] = instance
	return instance
}

// Connect adds a DAG edge between two registered operators.
func (m *Manager) Connect(from string, to string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.instances[from]; !ok {
		return fmt.Errorf("unknown operator %s", from)
	}
	if _, ok := m.instances[to]; !ok {
		return fmt.Errorf("unknown operator %s", to)
	}
	m.dag.Connect(from, to)
	return m.dag.Validate()
}

// Process routes one event through the topology. Paused operators buffer the
// event until resume.
func (m *Manager) Process(ev model.Event) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.paused {
		m.buffer = append(m.buffer, ev)
		return
	}
	m.applyLocked(ev)
}

// ApplyNow applies a record immediately even while paused. It is used for
// records that completed on another path before the resume reconciliation.
func (m *Manager) ApplyNow(ev model.Event) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.applyLocked(ev)
}

func (m *Manager) applyLocked(ev model.Event) {
	if m.state.IsApplied(ev.Offset) {
		return
	}
	m.state.MarkApplied(ev.Offset)
	m.apply(ev)
}

// Pause stops feeding new events to the operator chain.
func (m *Manager) Pause() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.paused = true
	for _, instance := range m.instances {
		instance.Pause()
	}
}

// Resume reconciles the buffered events against the current applied offsets:
// buffered records are replayed in offset order and records already applied are
// skipped, so the operator state and the buffer agree exactly once.
func (m *Manager) Resume() []model.Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.paused {
		return nil
	}
	if err := m.dag.Validate(); err != nil {
		m.buffer = nil
		m.paused = false
		return nil
	}
	sorted := append([]model.Event(nil), m.buffer...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Offset < sorted[j].Offset })
	var replayed []model.Event
	for _, ev := range sorted {
		if m.state.IsApplied(ev.Offset) {
			continue
		}
		m.state.MarkApplied(ev.Offset)
		m.apply(ev)
		replayed = append(replayed, ev)
	}
	m.buffer = nil
	m.paused = false
	for _, instance := range m.instances {
		instance.Resume()
	}
	return replayed
}

// BufferedCount returns how many events are waiting in the pause buffer.
func (m *Manager) BufferedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.buffer)
}

// Paused reports whether the topology is currently paused.
func (m *Manager) Paused() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.paused
}

// InstanceStates returns a stable view of every operator instance.
func (m *Manager) InstanceStates() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]string, len(m.instances))
	for name, instance := range m.instances {
		out[name] = instance.State.String()
	}
	return out
}

// NodeInfo returns a display record for one operator.
func (m *Manager) NodeInfo(name string) map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	instance, ok := m.instances[name]
	if !ok {
		return map[string]string{"name": name, "state": "unknown"}
	}
	return TopologyNode(instance, m.dag)
}

// DAGDescribe returns the current operator graph as text.
func (m *Manager) DAGDescribe() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.dag.Describe()
}
