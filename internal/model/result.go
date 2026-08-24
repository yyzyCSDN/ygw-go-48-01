package model

// WindowResult is the aggregated output of a window. Count, Sum, Min and Max
// describe the data events accepted by the window. Emitted marks the result as
// published to the sink; late events afterwards accumulate in the Late fields
// and never overwrite the published values.
type WindowResult struct {
	Window    WindowID
	Count     int64
	Sum       int64
	Min       int64
	Max       int64
	Emitted   bool
	LateCount int64
	LateSum   int64
}

// Apply folds a data value into the pre-emission accumulation.
func (r *WindowResult) Apply(value int64) {
	r.Count++
	r.Sum += value
	if r.Count == 1 || value < r.Min {
		r.Min = value
	}
	if value > r.Max {
		r.Max = value
	}
}

// ApplyLate folds a late value into the late-only accumulation.
func (r *WindowResult) ApplyLate(value int64) {
	r.LateCount++
	r.LateSum += value
}

// Snapshot returns a defensive copy of the result.
func (r *WindowResult) Snapshot() WindowResult {
	return *r
}
