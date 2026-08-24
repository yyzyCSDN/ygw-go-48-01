package watermark

// Observer receives watermark transitions. The topology installs observers to
// drive window triggering after a watermark confirm.
type Observer interface {
	OnWatermarkConfirmed(confirmed int64)
}

// Fanout broadcasts confirmed watermark transitions to a fixed set of
// observers. Observers are invoked synchronously in registration order.
type Fanout struct {
	observers []Observer
}

// NewFanout builds an empty observer fanout.
func NewFanout() *Fanout {
	return &Fanout{}
}

// Add registers one observer.
func (f *Fanout) Add(o Observer) {
	f.observers = append(f.observers, o)
}

// Notify calls OnWatermarkConfirmed on every registered observer.
func (f *Fanout) Notify(confirmed int64) {
	for _, observer := range f.observers {
		observer.OnWatermarkConfirmed(confirmed)
	}
}

// Count returns the number of registered observers.
func (f *Fanout) Count() int {
	return len(f.observers)
}
