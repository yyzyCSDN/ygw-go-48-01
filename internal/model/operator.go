package model

// OperatorState is the scheduling state machine of a stream operator:
// running -> pausing -> resumed.
type OperatorState int

const (
	OperatorRunning OperatorState = iota
	OperatorPausing
	OperatorResumed
)

func (s OperatorState) String() string {
	switch s {
	case OperatorRunning:
		return "running"
	case OperatorPausing:
		return "pausing"
	case OperatorResumed:
		return "resumed"
	default:
		return "unknown"
	}
}

// OperatorSpec describes a logical operator placed in a topology. Stateful
// operators keep per-key state in the state backend; stateless ones only pass
// records through.
type OperatorSpec struct {
	Name        string
	Parallelism int
	Stateful    bool
}

// OperatorInstance is a concrete execution unit of an operator. It tracks the
// current scheduling state and the number of records buffered while paused.
type OperatorInstance struct {
	Spec     OperatorSpec
	State    OperatorState
	Buffered int
}

// NewOperatorInstance creates a running operator instance from a spec.
func NewOperatorInstance(spec OperatorSpec) *OperatorInstance {
	return &OperatorInstance{Spec: spec, State: OperatorRunning}
}

// Pause transitions a running operator into the pausing state.
func (o *OperatorInstance) Pause() {
	if o.State == OperatorRunning {
		o.State = OperatorPausing
	}
}

// Resume transitions a pausing operator into the resumed state.
func (o *OperatorInstance) Resume() {
	if o.State == OperatorPausing {
		o.State = OperatorResumed
	}
}
